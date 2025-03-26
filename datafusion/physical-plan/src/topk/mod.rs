// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! TopK: Combination of Sort / LIMIT

use std::mem::size_of;
use std::sync::{Arc, RwLock};
use std::{cmp::Ordering, collections::BinaryHeap};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::{
    compute::interleave,
    row::{RowConverter, Rows, SortField},
};
use arrow_schema::SortOptions;
use datafusion_common::Result;
use datafusion_common::{internal_err, DataFusionError, HashMap};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    runtime_env::RuntimeEnv,
};
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{is_not_null, is_null, lit, BinaryExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use super::metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder};
use crate::dynamic_filters::DynamicFilterSource;
use crate::spill::get_record_batch_memory_size;
use crate::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};

/// Global TopK
///
/// # Background
///
/// "Top K" is a common query optimization used for queries such as
/// "find the top 3 customers by revenue". The (simplified) SQL for
/// such a query might be:
///
/// ```sql
/// SELECT customer_id, revenue FROM 'sales.csv' ORDER BY revenue DESC limit 3;
/// ```
///
/// The simple plan would be:
///
/// ```sql
/// > explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
/// +--------------+----------------------------------------+
/// | plan_type    | plan                                   |
/// +--------------+----------------------------------------+
/// | logical_plan | Limit: 3                               |
/// |              |   Sort: revenue DESC NULLS FIRST       |
/// |              |     Projection: customer_id, revenue   |
/// |              |       TableScan: sales                 |
/// +--------------+----------------------------------------+
/// ```
///
/// While this plan produces the correct answer, it will fully sorts the
/// input before discarding everything other than the top 3 elements.
///
/// The same answer can be produced by simply keeping track of the top
/// K=3 elements, reducing the total amount of required buffer memory.
///
/// # Structure
///
/// This operator tracks the top K items using a `TopKHeap`.
pub struct TopK {
    /// schema of the output (and the input)
    schema: SchemaRef,
    /// Runtime metrics
    metrics: TopKMetrics,
    /// Reservation
    reservation: MemoryReservation,
    /// The target number of rows for output batches
    batch_size: usize,
    /// sort expressions
    expr: Arc<[PhysicalSortExpr]>,
    /// row converter, for sort keys
    row_converter: RowConverter,
    /// scratch space for converting rows
    scratch_rows: Rows,
    /// stores the top k values and their sort key values, in order
    heap: Arc<RwLock<TopKHeap>>,
}

impl std::fmt::Debug for TopK {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopK")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("expr", &self.expr)
            .finish()
    }
}

impl TopK {
    /// Create a new [`TopK`] that stores the top `k` values, as
    /// defined by the sort expressions in `expr`.
    // TODO: make a builder or some other nicer API
    pub fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let reservation = MemoryConsumer::new(format!("TopK[{partition_id}]"))
            .register(&runtime.memory_pool);

        let expr: Arc<[PhysicalSortExpr]> = expr.into();

        let sort_fields: Vec<_> = expr
            .iter()
            .map(|e| {
                Ok(SortField::new_with_options(
                    e.expr.data_type(&schema)?,
                    e.options,
                ))
            })
            .collect::<Result<_>>()?;

        // TODO there is potential to add special cases for single column sort fields
        // to improve performance
        let row_converter = RowConverter::new(sort_fields)?;
        let scratch_rows = row_converter.empty_rows(
            batch_size,
            20 * batch_size, // guesstimate 20 bytes per row
        );

        Ok(Self {
            schema: Arc::clone(&schema),
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            batch_size,
            expr,
            row_converter,
            scratch_rows,
            heap: Arc::new(RwLock::new(TopKHeap::new(k, batch_size, schema))),
        })
    }

    pub fn dynamic_filter_source(&self) -> Arc<dyn DynamicFilterSource> {
        Arc::new(TopKDynamicFilterSource {
            heap: Arc::clone(&self.heap),
            expr: Arc::clone(&self.expr),
        })
    }

    /// Insert `batch`, remembering if any of its values are among
    /// the top k seen so far.
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Updates on drop
        let _timer = self.metrics.baseline.elapsed_compute().timer();

        let sort_keys: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|expr| {
                let value = expr.expr.evaluate(&batch)?;
                value.into_array(batch.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;

        // reuse existing `Rows` to avoid reallocations
        let rows = &mut self.scratch_rows;
        rows.clear();
        self.row_converter.append(rows, &sort_keys)?;

        // TODO make this algorithmically better?:
        // Idea: filter out rows >= self.heap.max() early (before passing to `RowConverter`)
        //       this avoids some work and also might be better vectorizable.
        let mut heap = self.heap.try_write().map_err(|_| {
            DataFusionError::Internal(
                "Failed to acquire write lock on TopK heap".to_string(),
            )
        })?;
        let mut batch_entry = heap.register_batch(batch);
        for (index, row) in rows.iter().enumerate() {
            match heap.max() {
                // heap has k items, and the new row is greater than the
                // current max in the heap ==> it is not a new topk
                Some(max_row) if row.as_ref() >= max_row.row() => {}
                // don't yet have k items or new item is lower than the currently k low values
                None | Some(_) => {
                    heap.add(&mut batch_entry, row, index);
                    self.metrics.row_replacements.add(1);
                }
            }
        }
        heap.insert_batch_entry(batch_entry);

        // conserve memory
        heap.maybe_compact()?;

        // update memory reservation
        let heap_size = heap.size();
        self.reservation.try_resize(self.size(heap_size))?;
        Ok(())
    }

    /// Returns the top k results broken into `batch_size` [`RecordBatch`]es, consuming the heap
    pub fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            batch_size,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            heap,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer(); // time updated on drop

        // break into record batches as needed
        let mut batches = vec![];
        let mut heap = heap.write().map_err(|_| {
            DataFusionError::Internal(
                "Failed to acquire write lock on TopK heap".to_string(),
            )
        })?;
        if let Some(mut batch) = heap.emit()? {
            metrics.baseline.output_rows().add(batch.num_rows());

            loop {
                if batch.num_rows() <= batch_size {
                    batches.push(Ok(batch));
                    break;
                } else {
                    batches.push(Ok(batch.slice(0, batch_size)));
                    let remaining_length = batch.num_rows() - batch_size;
                    batch = batch.slice(batch_size, remaining_length);
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        )))
    }

    /// return the size of memory used by this operator, in bytes
    fn size(&self, heap_size: usize) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.scratch_rows.size()
            + heap_size
    }
}

struct TopKMetrics {
    /// metrics
    pub baseline: BaselineMetrics,

    /// count of how many rows were replaced in the heap
    pub row_replacements: Count,
}

impl TopKMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            row_replacements: MetricBuilder::new(metrics)
                .counter("row_replacements", partition),
        }
    }
}

/// This structure keeps at most the *smallest* k items, using the
/// [arrow::row] format for sort keys. While it is called "topK" for
/// values like `1, 2, 3, 4, 5` the "top 3" really means the
/// *smallest* 3 , `1, 2, 3`, not the *largest* 3 `3, 4, 5`.
///
/// Using the `Row` format handles things such as ascending vs
/// descending and nulls first vs nulls last.
struct TopKHeap {
    /// The maximum number of elements to store in this heap.
    k: usize,
    /// The target number of rows for output batches
    batch_size: usize,
    /// Storage for up at most `k` items using a BinaryHeap. Reversed
    /// so that the smallest k so far is on the top
    inner: BinaryHeap<TopKRow>,
    /// Storage the original row values (TopKRow only has the sort key)
    store: RecordBatchStore,
    /// The size of all owned data held by this heap
    owned_bytes: usize,
}

/// Holds threshold value and sort order information for a column
struct ColumnThreshold {
    /// The column expression
    pub expr: Arc<dyn PhysicalExpr>,
    /// The threshold value
    pub value: datafusion_common::ScalarValue,
    /// Sort options
    pub sort_options: SortOptions,
}

impl TopKHeap {
    pub fn new(k: usize, batch_size: usize, schema: SchemaRef) -> Self {
        assert!(k > 0);
        Self {
            k,
            batch_size,
            inner: BinaryHeap::new(),
            store: RecordBatchStore::new(schema),
            owned_bytes: 0,
        }
    }

    /// Get threshold values for all columns in the given sort expressions.
    /// If the heap does not yet have k items, returns None.
    /// Otherwise, returns the threshold values from the max row in the heap.
    pub fn get_threshold_values(
        &self,
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Option<Vec<ColumnThreshold>>> {
        // If the heap doesn't have k elements yet, we can't create thresholds
        let max_row = match self.max() {
            Some(row) => row,
            None => return Ok(None),
        };

        // Get the batch that contains the max row
        let batch_entry = match self.store.get(max_row.batch_id) {
            Some(entry) => entry,
            None => return internal_err!("Invalid batch ID in TopKRow"),
        };

        // Extract threshold values for each sort expression
        let mut thresholds = Vec::with_capacity(sort_exprs.len());
        for sort_expr in sort_exprs {
            // Extract the value for this column from the max row
            let expr = Arc::clone(&sort_expr.expr);
            let value = expr.evaluate(&batch_entry.batch.slice(max_row.index, 1))?;

            // Convert to scalar value - should be a single value since we're evaluating on a single row batch
            let scalar = match value {
                ColumnarValue::Scalar(scalar) => scalar,
                ColumnarValue::Array(array) if array.len() == 1 => {
                    // Extract the first (and only) value from the array
                    datafusion_common::ScalarValue::try_from_array(&array, 0)?
                }
                array => {
                    return internal_err!("Expected a scalar value, got {:?}", array)
                }
            };

            thresholds.push(ColumnThreshold {
                expr,
                value: scalar,
                sort_options: sort_expr.options,
            });
        }

        Ok(Some(thresholds))
    }

    /// Register a [`RecordBatch`] with the heap, returning the
    /// appropriate entry
    pub fn register_batch(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        self.store.register(batch)
    }

    /// Insert a [`RecordBatchEntry`] created by a previous call to
    /// [`Self::register_batch`] into storage.
    pub fn insert_batch_entry(&mut self, entry: RecordBatchEntry) {
        self.store.insert(entry)
    }

    /// Returns the largest value stored by the heap if there are k
    /// items, otherwise returns None. Remember this structure is
    /// keeping the "smallest" k values
    pub fn max(&self) -> Option<&TopKRow> {
        if self.inner.len() < self.k {
            None
        } else {
            self.inner.peek()
        }
    }

    /// Adds `row` to this heap. If inserting this new item would
    /// increase the size past `k`, removes the previously smallest
    /// item.
    fn add(
        &mut self,
        batch_entry: &mut RecordBatchEntry,
        row: impl AsRef<[u8]>,
        index: usize,
    ) {
        let batch_id = batch_entry.id;
        batch_entry.uses += 1;

        assert!(self.inner.len() <= self.k);
        let row = row.as_ref();

        // Reuse storage for evicted item if possible
        let new_top_k = if self.inner.len() == self.k {
            let prev_min = self.inner.pop().unwrap();

            // Update batch use
            if prev_min.batch_id == batch_entry.id {
                batch_entry.uses -= 1;
            } else {
                self.store.unuse(prev_min.batch_id);
            }

            // update memory accounting
            self.owned_bytes -= prev_min.owned_size();
            prev_min.with_new_row(row, batch_id, index)
        } else {
            TopKRow::new(row, batch_id, index)
        };

        self.owned_bytes += new_top_k.owned_size();

        // put the new row into the heap
        self.inner.push(new_top_k)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], resetting the inner heap
    pub fn emit(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.emit_with_state()?.0)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], and a sorted vec of the
    /// current heap's contents
    pub fn emit_with_state(&mut self) -> Result<(Option<RecordBatch>, Vec<TopKRow>)> {
        let schema = Arc::clone(self.store.schema());

        // generate sorted rows
        let topk_rows = std::mem::take(&mut self.inner).into_sorted_vec();

        if self.store.is_empty() {
            return Ok((None, topk_rows));
        }

        // Indices for each row within its respective RecordBatch
        let indices: Vec<_> = topk_rows
            .iter()
            .enumerate()
            .map(|(i, k)| (i, k.index))
            .collect();

        let num_columns = schema.fields().len();

        // build the output columns one at time, using the
        // `interleave` kernel to pick rows from different arrays
        let output_columns: Vec<_> = (0..num_columns)
            .map(|col| {
                let input_arrays: Vec<_> = topk_rows
                    .iter()
                    .map(|k| {
                        let entry =
                            self.store.get(k.batch_id).expect("invalid stored batch id");
                        entry.batch.column(col) as &dyn Array
                    })
                    .collect();

                // at this point `indices` contains indexes within the
                // rows and `input_arrays` contains a reference to the
                // relevant Array for that index. `interleave` pulls
                // them together into a single new array
                Ok(interleave(&input_arrays, &indices)?)
            })
            .collect::<Result<_>>()?;

        let new_batch = RecordBatch::try_new(schema, output_columns)?;
        Ok((Some(new_batch), topk_rows))
    }

    /// Compact this heap, rewriting all stored batches into a single
    /// input batch
    pub fn maybe_compact(&mut self) -> Result<()> {
        // we compact if the number of "unused" rows in the store is
        // past some pre-defined threshold. Target holding up to
        // around 20 batches, but handle cases of large k where some
        // batches might be partially full
        let max_unused_rows = (20 * self.batch_size) + self.k;
        let unused_rows = self.store.unused_rows();

        // don't compact if the store has one extra batch or
        // unused rows is under the threshold
        if self.store.len() <= 2 || unused_rows < max_unused_rows {
            return Ok(());
        }
        // at first, compact the entire thing always into a new batch
        // (maybe we can get fancier in the future about ignoring
        // batches that have a high usage ratio already

        // Note: new batch is in the same order as inner
        let num_rows = self.inner.len();
        let (new_batch, mut topk_rows) = self.emit_with_state()?;
        let Some(new_batch) = new_batch else {
            return Ok(());
        };

        // clear all old entries in store (this invalidates all
        // store_ids in `inner`)
        self.store.clear();

        let mut batch_entry = self.register_batch(new_batch);
        batch_entry.uses = num_rows;

        // rewrite all existing entries to use the new batch, and
        // remove old entries. The sortedness and their relative
        // position do not change
        for (i, topk_row) in topk_rows.iter_mut().enumerate() {
            topk_row.batch_id = batch_entry.id;
            topk_row.index = i;
        }
        self.insert_batch_entry(batch_entry);
        // restore the heap
        self.inner = BinaryHeap::from(topk_rows);

        Ok(())
    }

    /// return the size of memory used by this heap, in bytes
    fn size(&self) -> usize {
        size_of::<Self>()
            + (self.inner.capacity() * size_of::<TopKRow>())
            + self.store.size()
            + self.owned_bytes
    }
}

/// Represents one of the top K rows held in this heap. Orders
/// according to memcmp of row (e.g. the arrow Row format, but could
/// also be primitive values)
///
/// Reuses allocations to minimize runtime overhead of creating new Vecs
#[derive(Debug, PartialEq)]
struct TopKRow {
    /// the value of the sort key for this row. This contains the
    /// bytes that could be stored in `OwnedRow` but uses `Vec<u8>` to
    /// reuse allocations.
    row: Vec<u8>,
    /// the RecordBatch this row came from: an id into a [`RecordBatchStore`]
    batch_id: u32,
    /// the index in this record batch the row came from
    index: usize,
}

impl TopKRow {
    /// Create a new TopKRow with new allocation
    fn new(row: impl AsRef<[u8]>, batch_id: u32, index: usize) -> Self {
        Self {
            row: row.as_ref().to_vec(),
            batch_id,
            index,
        }
    }

    /// Create a new  TopKRow reusing the existing allocation
    fn with_new_row(
        self,
        new_row: impl AsRef<[u8]>,
        batch_id: u32,
        index: usize,
    ) -> Self {
        let Self {
            mut row,
            batch_id: _,
            index: _,
        } = self;
        row.clear();
        row.extend_from_slice(new_row.as_ref());

        Self {
            row,
            batch_id,
            index,
        }
    }

    /// Returns the number of bytes owned by this row in the heap (not
    /// including itself)
    fn owned_size(&self) -> usize {
        self.row.capacity()
    }

    /// Returns a slice to the owned row value
    pub fn row(&self) -> &[u8] {
        self.row.as_slice()
    }
}

impl Eq for TopKRow {}

impl PartialOrd for TopKRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

#[derive(Debug)]
pub struct RecordBatchEntry {
    id: u32,
    batch: RecordBatch,
    // for this batch, how many times has it been used
    uses: usize,
}

/// This structure tracks [`RecordBatch`] by an id so that:
///
/// 1. The baches can be tracked via an id that can be copied cheaply
/// 2. The total memory held by all batches is tracked
#[derive(Debug)]
struct RecordBatchStore {
    /// id generator
    next_id: u32,
    /// storage
    batches: HashMap<u32, RecordBatchEntry>,
    /// total size of all record batches tracked by this store
    batches_size: usize,
    /// schema of the batches
    schema: SchemaRef,
}

impl RecordBatchStore {
    fn new(schema: SchemaRef) -> Self {
        Self {
            next_id: 0,
            batches: HashMap::new(),
            batches_size: 0,
            schema,
        }
    }

    /// Register this batch with the store and assign an ID. No
    /// attempt is made to compare this batch to other batches
    pub fn register(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        let id = self.next_id;
        self.next_id += 1;
        RecordBatchEntry { id, batch, uses: 0 }
    }

    /// Insert a record batch entry into this store, tracking its
    /// memory use, if it has any uses
    pub fn insert(&mut self, entry: RecordBatchEntry) {
        // uses of 0 means that none of the rows in the batch were stored in the topk
        if entry.uses > 0 {
            self.batches_size += get_record_batch_memory_size(&entry.batch);
            self.batches.insert(entry.id, entry);
        }
    }

    /// Clear all values in this store, invalidating all previous batch ids
    fn clear(&mut self) {
        self.batches.clear();
        self.batches_size = 0;
    }

    fn get(&self, id: u32) -> Option<&RecordBatchEntry> {
        self.batches.get(&id)
    }

    /// returns the total number of batches stored in this store
    fn len(&self) -> usize {
        self.batches.len()
    }

    /// Returns the total number of rows in batches minus the number
    /// which are in use
    fn unused_rows(&self) -> usize {
        self.batches
            .values()
            .map(|batch_entry| batch_entry.batch.num_rows() - batch_entry.uses)
            .sum()
    }

    /// returns true if the store has nothing stored
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// return the schema of batches stored
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// remove a use from the specified batch id. If the use count
    /// reaches zero the batch entry is removed from the store
    ///
    /// panics if there were no remaining uses of id
    pub fn unuse(&mut self, id: u32) {
        let remove = if let Some(batch_entry) = self.batches.get_mut(&id) {
            batch_entry.uses = batch_entry.uses.checked_sub(1).expect("underflow");
            batch_entry.uses == 0
        } else {
            panic!("No entry for id {id}");
        };

        if remove {
            let old_entry = self.batches.remove(&id).unwrap();
            self.batches_size = self
                .batches_size
                .checked_sub(get_record_batch_memory_size(&old_entry.batch))
                .unwrap();
        }
    }

    /// returns the size of memory used by this store, including all
    /// referenced `RecordBatch`es, in bytes
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + self.batches.capacity() * (size_of::<u32>() + size_of::<RecordBatchEntry>())
            + self.batches_size
    }
}

/// Pushdown of dynamic fitlers from TopK operators is used to speed up queries
/// such as `SELECT * FROM table ORDER BY col DESC LIMIT 10` by pushing down the
/// threshold values for the sort columns to the data source.
/// That is, the TopK operator will keep track of the top 10 values for the sort
/// and before a new file is opened it's statitics will be checked against the
/// threshold values to determine if the file can be skipped and predicate pushdown
/// will use these to skip rows during the scan.
///
/// For example, imagine this data gets created if multiple sources with clock skews,
/// network delays, etc. are writing data and you don't do anything fancy to guarantee
/// perfect sorting by `timestamp` (i.e. you naively write out the data to Parquet, maybe do some compaction, etc.).
/// The point is that 99% of yesterday's files have a `timestamp` smaller than 99% of today's files
/// but there may be a couple seconds of overlap between files.
/// To be concrete, let's say this is our data:
//
// | file | min | max |
// |------|-----|-----|
// | 1    | 1   | 10  |
// | 2    | 9   | 19  |
// | 3    | 20  | 31  |
// | 4    | 30  | 35  |
//
// Ideally a [`TableProvider`] is able to use file level stats or other methods to roughly order the files
// within each partition / file group such that we start with the newest / largest `timestamp`s.
// If this is not possible the optimization still works but is less efficient and harder to visualize,
// so for this example let's assume that we process 1 file at a time and we started with file 4.
// After processing file 4 let's say we have 10 values in our TopK heap, the smallest of which is 30.
// The TopK operator will then push down the filter `timestamp < 30` down the tree of [`ExecutionPlan`]s
// and if the data source supports dynamic filter pushdown it will accept a reference to this [`DynamicFilterSource`]
// and when it goes to open file 3 it will ask the [`DynamicFilterSource`] for the current filters.
// Since file 3 may contain values larger than 30 we cannot skip it entirely,
// but scanning it may still be more efficient due to page pruning and other optimizations.
// Once we get to file 2 however we can skip it entirely because we know that all values in file 2 are smaller than 30.
// The same goes for file 1.
// So this optimization just saved us 50% of the work of scanning the data.
struct TopKDynamicFilterSource {
    /// The TopK heap that provides the current filters
    heap: Arc<RwLock<TopKHeap>>,
    /// The sort expressions used to create the TopK
    expr: Arc<[PhysicalSortExpr]>,
}

impl std::fmt::Debug for TopKDynamicFilterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopKDynamicFilterSource")
            .field("expr", &self.expr)
            .finish()
    }
}

impl DynamicFilterSource for TopKDynamicFilterSource {
    fn current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let heap_guard = self.heap.read().map_err(|_| {
            DataFusionError::Internal(
                "Failed to acquire read lock on TopK heap".to_string(),
            )
        })?;

        // Get the threshold values for all sort expressions
        let Some(thresholds) = heap_guard.get_threshold_values(&self.expr)? else {
            return Ok(vec![]); // No thresholds available yet
        };

        // Create filter expressions for each threshold
        let mut filters: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(thresholds.len());

        let mut prev_sort_expr: Option<Arc<dyn PhysicalExpr>> = None;
        for threshold in thresholds {
            // Create the appropriate operator based on sort order
            let op = if threshold.sort_options.descending {
                // For descending sort, we want col > threshold (exclude smaller values)
                Operator::Gt
            } else {
                // For ascending sort, we want col < threshold (exclude larger values)
                Operator::Lt
            };

            let value_null = threshold.value.is_null();

            let comparison = Arc::new(BinaryExpr::new(
                Arc::clone(&threshold.expr),
                op,
                lit(threshold.value.clone()),
            ));

            let comparison_with_null =
                match (threshold.sort_options.nulls_first, value_null) {
                    // For nulls first, transform to (threshold.value is not null) and (threshold.expr is null or comparison)
                    (true, true) => lit(false),
                    (true, false) => Arc::new(BinaryExpr::new(
                        is_null(Arc::clone(&threshold.expr))?,
                        Operator::Or,
                        comparison,
                    )),
                    // For nulls last, transform to (threshold.value is null and threshold.expr is not null)
                    // or (threshold.value is not null and comparison)
                    (false, true) => is_not_null(Arc::clone(&threshold.expr))?,
                    (false, false) => comparison,
                };

            let mut eq_expr = Arc::new(BinaryExpr::new(
                Arc::clone(&threshold.expr),
                Operator::Eq,
                lit(threshold.value.clone()),
            ));

            if value_null {
                eq_expr = Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&threshold.expr))?,
                    Operator::Or,
                    eq_expr,
                ));
            }

            // For a query like order by a, b, the filter for column `b` is only applied if
            // the condition a = threshold.value (considering null equality) is met.
            // Therefore, we add equality predicates for all preceding fields to the filter logic of the current field,
            // and include the current field's equality predicate in `prev_sort_expr` for use with subsequent fields.
            match prev_sort_expr.take() {
                None => {
                    prev_sort_expr = Some(eq_expr);
                    filters.push(comparison_with_null);
                }
                Some(p) => {
                    filters.push(Arc::new(BinaryExpr::new(
                        Arc::clone(&p),
                        Operator::And,
                        comparison_with_null,
                    )));

                    prev_sort_expr =
                        Some(Arc::new(BinaryExpr::new(p, Operator::And, eq_expr)));
                }
            }
        }

        let dynamic_predicate = filters
            .into_iter()
            .reduce(|a, b| Arc::new(BinaryExpr::new(a, Operator::Or, b)));

        match dynamic_predicate {
            None => Ok(vec![]),
            Some(p) => Ok(vec![p]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, RecordBatch};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};

    /// This test ensures the size calculation is correct for RecordBatches with multiple columns.
    #[test]
    fn test_record_batch_store_size() {
        // given
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));
        let mut record_batch_store = RecordBatchStore::new(Arc::clone(&schema));
        let int_array =
            Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]); // 5 * 4 = 20
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]); // 5 * 8 = 40

        let record_batch_entry = RecordBatchEntry {
            id: 0,
            batch: RecordBatch::try_new(
                schema,
                vec![Arc::new(int_array), Arc::new(float64_array)],
            )
            .unwrap(),
            uses: 1,
        };

        // when insert record batch entry
        record_batch_store.insert(record_batch_entry);
        assert_eq!(record_batch_store.batches_size, 60);

        // when unuse record batch entry
        record_batch_store.unuse(0);
        assert_eq!(record_batch_store.batches_size, 0);
    }

    #[test]
    fn test_topk_as_dynamic_filter_source() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, true),
            Field::new("col2", DataType::Float64, false),
        ]));

        let runtime = Arc::new(RuntimeEnv::default());
        let metrics = ExecutionPlanMetricsSet::new();

        // Create a TopK with descending sort on col2
        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(datafusion_physical_expr::expressions::Column::new(
                "col2", 1,
            )),
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];

        let mut topk = TopK::try_new(
            0,
            Arc::clone(&schema),
            sort_expr.into(),
            5,   // k=5
            100, // batch_size
            runtime,
            &metrics,
        )
        .unwrap();

        // Initially there should be no filters (empty heap)
        let filters = topk.dynamic_filter_source().current_filters().unwrap();
        assert_eq!(filters.len(), 0);

        // Insert some data to fill the heap
        let col1 = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let col2 =
            Float64Array::from(vec![10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(col1), Arc::new(col2)],
        )
        .unwrap();

        // Insert the data into TopK
        topk.insert_batch(batch).unwrap();

        // Now there should be a filter
        let filters = topk.dynamic_filter_source().current_filters().unwrap();

        // We expect a filter for col2 > 6.0 (since we're doing descending sort and have 5 values)
        assert_eq!(filters.len(), 1);
    }
}
