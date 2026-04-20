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

use crate::sort::reverse_row_selection;
use datafusion_common::{Result, assert_eq_or_internal_err};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

/// A selection of rows and row groups within a ParquetFile to decode.
///
/// A `ParquetAccessPlan` is used to limit the row groups and data pages a `DataSourceExec`
/// will read and decode to improve performance.
///
/// Note that page level pruning based on ArrowPredicate is applied after all of
/// these selections
///
/// # Example
///
/// For example, given a Parquet file with 4 row groups, a `ParquetAccessPlan`
/// can be used to specify skipping row group 0 and 2, scanning a range of rows
/// in row group 1, and scanning all rows in row group 3 as follows:
///
/// ```rust
/// # use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
/// # use datafusion_datasource_parquet::ParquetAccessPlan;
/// // Default to scan all row groups
/// let mut access_plan = ParquetAccessPlan::new_all(4);
/// access_plan.skip(0); // skip row group
/// // Use parquet reader RowSelector to specify scanning rows 100-200 and 350-400
/// // in a row group that has 1000 rows
/// let row_selection = RowSelection::from(vec![
///    RowSelector::skip(100),
///    RowSelector::select(100),
///    RowSelector::skip(150),
///    RowSelector::select(50),
///    RowSelector::skip(600),  // skip last 600 rows
/// ]);
/// access_plan.scan_selection(1, row_selection);
/// access_plan.skip(2); // skip row group 2
/// // row group 3 is scanned by default
/// ```
///
/// The resulting plan would look like:
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///
/// │                   │  SKIP
///
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///  Row Group 0
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///  ┌────────────────┐    SCAN ONLY ROWS
/// │└────────────────┘ │  100-200
///  ┌────────────────┐    350-400
/// │└────────────────┘ │
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///  Row Group 1
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                        SKIP
/// │                   │
///
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///  Row Group 2
/// ┌───────────────────┐
/// │                   │  SCAN ALL ROWS
/// │                   │
/// │                   │
/// └───────────────────┘
///  Row Group 3
/// ```
///
/// For more background, please also see the [Embedding User-Defined Indexes in Apache Parquet Files blog]
///
/// [Embedding User-Defined Indexes in Apache Parquet Files blog]: https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetAccessPlan {
    /// How to access the i-th row group
    row_groups: Vec<RowGroupAccess>,
}

/// Describes how the parquet reader will access a row group
#[derive(Debug, Clone, PartialEq)]
pub enum RowGroupAccess {
    /// Do not read the row group at all
    Skip,
    /// Read all rows from the row group
    Scan,
    /// Scan only the specified rows within the row group
    Selection(RowSelection),
}

impl RowGroupAccess {
    /// Return true if this row group should be scanned
    pub fn should_scan(&self) -> bool {
        match self {
            RowGroupAccess::Skip => false,
            RowGroupAccess::Scan | RowGroupAccess::Selection(_) => true,
        }
    }
}

impl ParquetAccessPlan {
    /// Create a new `ParquetAccessPlan` that scans all row groups
    pub fn new_all(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Scan; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` that scans no row groups
    pub fn new_none(row_group_count: usize) -> Self {
        Self {
            row_groups: vec![RowGroupAccess::Skip; row_group_count],
        }
    }

    /// Create a new `ParquetAccessPlan` from the specified [`RowGroupAccess`]es
    pub fn new(row_groups: Vec<RowGroupAccess>) -> Self {
        Self { row_groups }
    }

    /// Set the i-th row group to the specified [`RowGroupAccess`]
    pub fn set(&mut self, idx: usize, access: RowGroupAccess) {
        self.row_groups[idx] = access;
    }

    /// skips the i-th row group (should not be scanned)
    pub fn skip(&mut self, idx: usize) {
        self.set(idx, RowGroupAccess::Skip);
    }

    /// scan the i-th row group
    pub fn scan(&mut self, idx: usize) {
        self.set(idx, RowGroupAccess::Scan);
    }

    /// Return true if the i-th row group should be scanned
    pub fn should_scan(&self, idx: usize) -> bool {
        self.row_groups[idx].should_scan()
    }

    /// Set to scan only the [`RowSelection`] in the specified row group.
    ///
    /// Behavior is different depending on the existing access
    /// * [`RowGroupAccess::Skip`]: does nothing
    /// * [`RowGroupAccess::Scan`]: Updates to scan only the rows in the `RowSelection`
    /// * [`RowGroupAccess::Selection`]: Updates to scan only the intersection of the existing selection and the new selection
    pub fn scan_selection(&mut self, idx: usize, selection: RowSelection) {
        self.row_groups[idx] = match &self.row_groups[idx] {
            // already skipping the entire row group
            RowGroupAccess::Skip => RowGroupAccess::Skip,
            RowGroupAccess::Scan => RowGroupAccess::Selection(selection),
            RowGroupAccess::Selection(existing_selection) => {
                RowGroupAccess::Selection(existing_selection.intersection(&selection))
            }
        }
    }

    /// Return an overall `RowSelection`, if needed
    ///
    /// This is used to compute the row selection for the parquet reader. See
    /// [`ArrowReaderBuilder::with_row_selection`] for more details.
    ///
    /// Returns
    /// * `None` if there are no  [`RowGroupAccess::Selection`]
    /// * `Some(selection)` if there are [`RowGroupAccess::Selection`]s
    ///
    /// The returned selection represents which rows to scan across any row
    /// row groups which are not skipped.
    ///
    /// # Notes
    ///
    /// If there are no [`RowGroupAccess::Selection`]s, the overall row
    /// selection is `None` because each row group is either entirely skipped or
    /// scanned, which is covered by [`Self::row_group_indexes`].
    ///
    /// If there are any [`RowGroupAccess::Selection`], an overall row selection
    /// is returned for *all* the rows in the row groups that are not skipped.
    /// Thus it includes a `Select` selection for any [`RowGroupAccess::Scan`].
    ///
    /// # Errors
    ///
    /// Returns an error if any specified row selection does not specify
    /// the same number of rows as in it's corresponding `row_group_metadata`.
    ///
    /// # Example: No Selections
    ///
    /// Given an access plan like this
    ///
    /// ```text
    ///   RowGroupAccess::Scan (scan all row group 0)
    ///   RowGroupAccess::Skip (skip row group 1)
    ///   RowGroupAccess::Scan (scan all row group 2)
    ///   RowGroupAccess::Scan (scan all row group 3)
    /// ```
    ///
    /// The overall row selection would be `None` because there are no
    /// [`RowGroupAccess::Selection`]s. The row group indexes
    /// returned by [`Self::row_group_indexes`] would be `0, 2, 3` .
    ///
    /// # Example: With Selections
    ///
    /// Given an access plan like this:
    ///
    /// ```text
    ///   RowGroupAccess::Scan (scan all row group 0)
    ///   RowGroupAccess::Skip (skip row group 1)
    ///   RowGroupAccess::Select (skip 50, scan 50, skip 900) (scan rows 50-100 in row group 2)
    ///   RowGroupAccess::Scan (scan all row group 3)
    /// ```
    ///
    /// Assuming each row group has 1000 rows, the resulting row selection would
    /// be the rows to scan in row group 0, 2 and 4:
    ///
    /// ```text
    ///  RowSelection::Select(1000) (scan all rows in row group 0)
    ///  RowSelection::Skip(50)     (skip first 50 rows in row group 2)
    ///  RowSelection::Select(50)   (scan rows 50-100 in row group 2)
    ///  RowSelection::Skip(900)    (skip last 900 rows in row group 2)
    ///  RowSelection::Select(1000) (scan all rows in row group 3)
    /// ```
    ///
    /// Note there is no entry for the (entirely) skipped row group 1.
    ///
    /// The row group indexes returned by [`Self::row_group_indexes`] would
    /// still be `0, 2, 3` .
    ///
    /// [`ArrowReaderBuilder::with_row_selection`]: parquet::arrow::arrow_reader::ArrowReaderBuilder::with_row_selection
    pub fn into_overall_row_selection(
        self,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<Option<RowSelection>> {
        assert_eq!(row_group_meta_data.len(), self.row_groups.len());
        // Intuition: entire row groups are filtered out using
        // `row_group_indexes` which come from Skip and Scan. An overall
        // RowSelection is only useful if there is any parts *within* a row group
        // which can be filtered out, that is a `Selection`.
        if !self
            .row_groups
            .iter()
            .any(|rg| matches!(rg, RowGroupAccess::Selection(_)))
        {
            return Ok(None);
        }

        // validate all Selections
        for (idx, (rg, rg_meta)) in self
            .row_groups
            .iter()
            .zip(row_group_meta_data.iter())
            .enumerate()
        {
            let RowGroupAccess::Selection(selection) = rg else {
                continue;
            };
            let rows_in_selection = selection
                .iter()
                .map(|selection| selection.row_count)
                .sum::<usize>();

            let row_group_row_count = rg_meta.num_rows();
            assert_eq_or_internal_err!(
                rows_in_selection as i64,
                row_group_row_count,
                "Invalid ParquetAccessPlan Selection. Row group {idx} has {row_group_row_count} rows \
                    but selection only specifies {rows_in_selection} rows. \
                    Selection: {selection:?}"
            );
        }

        let total_selection: RowSelection = self
            .row_groups
            .into_iter()
            .zip(row_group_meta_data.iter())
            .flat_map(|(rg, rg_meta)| {
                match rg {
                    RowGroupAccess::Skip => vec![],
                    RowGroupAccess::Scan => {
                        // need a row group access to scan the entire row group (need row group counts)
                        vec![RowSelector::select(rg_meta.num_rows() as usize)]
                    }
                    RowGroupAccess::Selection(selection) => {
                        let selection: Vec<RowSelector> = selection.into();
                        selection
                    }
                }
            })
            .collect();

        Ok(Some(total_selection))
    }

    /// Return an iterator over the row group indexes that should be scanned
    pub fn row_group_index_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.row_groups
            .iter()
            .enumerate()
            .filter_map(|(idx, b)| if b.should_scan() { Some(idx) } else { None })
    }

    /// Return a vec of all row group indexes to scan
    pub fn row_group_indexes(&self) -> Vec<usize> {
        self.row_group_index_iter().collect()
    }

    /// Return the total number of row groups (not the total number or groups to
    /// scan)
    pub fn len(&self) -> usize {
        self.row_groups.len()
    }

    /// Return true if there are no row groups
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    /// Get a reference to the inner accesses
    pub fn inner(&self) -> &[RowGroupAccess] {
        &self.row_groups
    }

    /// Covert into the inner row group accesses
    pub fn into_inner(self) -> Vec<RowGroupAccess> {
        self.row_groups
    }

    /// Prepare this plan and resolve to the final `PreparedAccessPlan`
    pub(crate) fn prepare(
        self,
        row_group_meta_data: &[RowGroupMetaData],
    ) -> Result<PreparedAccessPlan> {
        let row_group_indexes = self.row_group_indexes();
        let row_selection = self.into_overall_row_selection(row_group_meta_data)?;

        PreparedAccessPlan::new(row_group_indexes, row_selection)
    }

    /// Split this plan into an ordered list of sub-plans ("chunks"), each of
    /// which represents a contiguous prefix of work packed together.
    ///
    /// Each returned plan has the same `len()` as `self`. Row groups outside
    /// the chunk are set to [`RowGroupAccess::Skip`]; row groups inside the
    /// chunk keep their original [`RowGroupAccess`].
    ///
    /// Chunks are formed by walking `self.row_groups` in order and grouping
    /// consecutive entries with `should_scan() == true`. A new chunk is started
    /// whenever adding the next scannable row group would push the accumulated
    /// row count past `max_rows` or compressed byte size past `max_bytes`. A
    /// single row group that already exceeds either limit becomes its own
    /// chunk (no sub-row-group split is performed).
    ///
    /// [`RowGroupAccess::Skip`] entries are carried silently in whichever chunk
    /// is active at that point; they contribute no rows or bytes.
    ///
    /// If there are no scannable row groups, the result is empty.
    pub(crate) fn split_into_chunks(
        self,
        row_group_meta_data: &[RowGroupMetaData],
        max_rows: u64,
        max_bytes: u64,
    ) -> Vec<ParquetAccessPlan> {
        assert_eq!(self.row_groups.len(), row_group_meta_data.len());

        let len = self.row_groups.len();
        let mut chunks: Vec<ParquetAccessPlan> = Vec::new();
        let mut current: Option<(ParquetAccessPlan, u64, u64)> = None;

        for (idx, access) in self.row_groups.into_iter().enumerate() {
            if !access.should_scan() {
                // Skip entries are attached to the currently open chunk (if
                // any) so they do not force a chunk boundary. They contribute
                // zero rows/bytes.
                if let Some((plan, _, _)) = current.as_mut() {
                    plan.row_groups[idx] = access;
                }
                continue;
            }

            let rg_meta = &row_group_meta_data[idx];
            let rg_rows = rg_meta.num_rows().max(0) as u64;
            let rg_bytes = rg_meta.compressed_size().max(0) as u64;

            if let Some((plan, acc_rows, acc_bytes)) = current.as_mut() {
                let exceeds = acc_rows.saturating_add(rg_rows) > max_rows
                    || acc_bytes.saturating_add(rg_bytes) > max_bytes;
                if exceeds {
                    chunks.push(current.take().unwrap().0);
                } else {
                    plan.row_groups[idx] = access;
                    *acc_rows += rg_rows;
                    *acc_bytes += rg_bytes;
                    continue;
                }
            }

            // Start a new chunk with this row group.
            let mut plan = ParquetAccessPlan::new_none(len);
            plan.row_groups[idx] = access;
            current = Some((plan, rg_rows, rg_bytes));
        }

        if let Some((plan, _, _)) = current {
            chunks.push(plan);
        }

        chunks
    }
}

/// Represents a prepared, fully resolved [`ParquetAccessPlan`]
///
/// The [`RowSelection`] represents the result of applying all pruning such as
/// user provided scans, Row Group statistics, DataPage statistics, and Bloom
/// Filters.
///
/// This plan is what is passed to the parquet reader
pub(crate) struct PreparedAccessPlan {
    /// Row group indexes to read
    pub(crate) row_group_indexes: Vec<usize>,
    /// Optional row selection for filtering within row groups
    pub(crate) row_selection: Option<RowSelection>,
}

impl PreparedAccessPlan {
    /// Create a new prepared access plan
    fn new(
        row_group_indexes: Vec<usize>,
        row_selection: Option<RowSelection>,
    ) -> Result<Self> {
        Ok(Self {
            row_group_indexes,
            row_selection,
        })
    }

    /// Reverse the access plan for reverse scanning
    pub(crate) fn reverse(mut self, file_metadata: &ParquetMetaData) -> Result<Self> {
        // Get the row group indexes before reversing
        let row_groups_to_scan = self.row_group_indexes.clone();

        // Reverse the row group indexes
        self.row_group_indexes = self.row_group_indexes.into_iter().rev().collect();

        // If we have a row selection, reverse it to match the new row group order
        if let Some(row_selection) = self.row_selection {
            self.row_selection = Some(reverse_row_selection(
                &row_selection,
                file_metadata,
                &row_groups_to_scan, // Pass the original (non-reversed) row group indexes
            )?);
        }

        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_common::assert_contains;
    use parquet::basic::LogicalType;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use std::sync::{Arc, LazyLock};

    #[test]
    fn test_only_scans() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        // scan all row groups, no selection
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_eq!(row_selection, None);
    }

    #[test]
    fn test_only_skips() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        // skip all row groups, no selection
        assert_eq!(row_group_indexes, vec![] as Vec<usize>);
        assert_eq!(row_selection, None);
    }
    #[test]
    fn test_mixed_1() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(
                // specifies all 20 rows in row group 1
                vec![
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(8),
                ]
                .into(),
            ),
            RowGroupAccess::Skip,
            RowGroupAccess::Skip,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        assert_eq!(row_group_indexes, vec![0, 1]);
        assert_eq!(
            row_selection,
            Some(
                vec![
                    // select the entire first row group
                    RowSelector::select(10),
                    // selectors from the second row group
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(8)
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_mixed_2() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(
                // specify all 30 rows in row group 1
                vec![
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(18),
                ]
                .into(),
            ),
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap();

        assert_eq!(row_group_indexes, vec![1, 2, 3]);
        assert_eq!(
            row_selection,
            Some(
                vec![
                    // select the entire second row group
                    RowSelector::select(20),
                    // selectors from the third row group
                    RowSelector::select(5),
                    RowSelector::skip(7),
                    RowSelector::select(18),
                    // select the entire fourth row group
                    RowSelector::select(40),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_invalid_too_few() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            // specify only 12 rows in selection, but row group 1 has 20
            RowGroupAccess::Selection(
                vec![RowSelector::select(5), RowSelector::skip(7)].into(),
            ),
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let err = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap_err()
            .to_string();
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_contains!(
            err,
            "Row group 1 has 20 rows but selection only specifies 12 rows"
        );
    }

    #[test]
    fn test_invalid_too_many() {
        let access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            // specify 22 rows in selection, but row group 1 has only 20
            RowGroupAccess::Selection(
                vec![
                    RowSelector::select(10),
                    RowSelector::skip(2),
                    RowSelector::select(10),
                ]
                .into(),
            ),
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);

        let row_group_indexes = access_plan.row_group_indexes();
        let err = access_plan
            .into_overall_row_selection(&ROW_GROUP_METADATA)
            .unwrap_err()
            .to_string();
        assert_eq!(row_group_indexes, vec![0, 1, 2, 3]);
        assert_contains!(
            err,
            "Invalid ParquetAccessPlan Selection. Row group 1 has 20 rows but selection only specifies 22 rows"
        );
    }

    /// [`RowGroupMetaData`] that returns 4 row groups with 10, 20, 30, 40 rows
    /// respectively
    static ROW_GROUP_METADATA: LazyLock<Vec<RowGroupMetaData>> = LazyLock::new(|| {
        let schema_descr = get_test_schema_descr();
        let row_counts = [10, 20, 30, 40];

        row_counts
            .into_iter()
            .map(|num_rows| {
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_num_values(num_rows)
                    .build()
                    .unwrap();

                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(num_rows)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect()
    });

    /// Build metadata for row groups with the given `(num_rows, compressed_bytes)`
    /// pairs. Returned metadata has one `BYTE_ARRAY` column per row group.
    fn row_groups_with_bytes(specs: &[(i64, i64)]) -> Vec<RowGroupMetaData> {
        let schema_descr = get_test_schema_descr();
        specs
            .iter()
            .map(|(num_rows, compressed)| {
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_num_values(*num_rows)
                    .set_total_compressed_size(*compressed)
                    .build()
                    .unwrap();

                RowGroupMetaData::builder(schema_descr.clone())
                    .set_num_rows(*num_rows)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect()
    }

    fn access_kinds(plan: &ParquetAccessPlan) -> Vec<&'static str> {
        plan.inner()
            .iter()
            .map(|rg| match rg {
                RowGroupAccess::Skip => "skip",
                RowGroupAccess::Scan => "scan",
                RowGroupAccess::Selection(_) => "sel",
            })
            .collect()
    }

    #[test]
    fn test_split_into_chunks_empty() {
        let plan = ParquetAccessPlan::new(vec![]);
        let chunks = plan.split_into_chunks(&[], 1000, 1000);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_into_chunks_all_skip() {
        let meta = row_groups_with_bytes(&[(100, 1_000), (100, 1_000)]);
        let plan = ParquetAccessPlan::new_none(2);
        let chunks = plan.split_into_chunks(&meta, 1000, 10_000);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_into_chunks_one_per_row_group() {
        // Each row group is already at the per-morsel limit, so each becomes
        // its own chunk.
        let meta = row_groups_with_bytes(&[(100, 1_000), (100, 1_000), (100, 1_000)]);
        let plan = ParquetAccessPlan::new_all(3);
        let chunks = plan.split_into_chunks(&meta, 100, 1_000);
        assert_eq!(chunks.len(), 3);
        assert_eq!(access_kinds(&chunks[0]), vec!["scan", "skip", "skip"]);
        assert_eq!(access_kinds(&chunks[1]), vec!["skip", "scan", "skip"]);
        assert_eq!(access_kinds(&chunks[2]), vec!["skip", "skip", "scan"]);
    }

    #[test]
    fn test_split_into_chunks_packs_small() {
        // Three small row groups fit within one chunk by rows AND bytes.
        let meta = row_groups_with_bytes(&[(30, 100), (30, 100), (30, 100)]);
        let plan = ParquetAccessPlan::new_all(3);
        let chunks = plan.split_into_chunks(&meta, 100, 1_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(access_kinds(&chunks[0]), vec!["scan", "scan", "scan"]);
    }

    #[test]
    fn test_split_into_chunks_oversized_single() {
        // First row group alone exceeds max_rows; still becomes its own chunk
        // (no sub-row-group split).
        let meta = row_groups_with_bytes(&[(1_000, 100), (10, 100), (10, 100)]);
        let plan = ParquetAccessPlan::new_all(3);
        let chunks = plan.split_into_chunks(&meta, 100, 10_000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(access_kinds(&chunks[0]), vec!["scan", "skip", "skip"]);
        assert_eq!(access_kinds(&chunks[1]), vec!["skip", "scan", "scan"]);
    }

    #[test]
    fn test_split_into_chunks_respects_bytes() {
        // All row groups are small in rows but the second one is big enough
        // that it must start a new chunk on byte budget alone.
        let meta = row_groups_with_bytes(&[(10, 500), (10, 600), (10, 100), (10, 100)]);
        let plan = ParquetAccessPlan::new_all(4);
        let chunks = plan.split_into_chunks(&meta, 1_000_000, 1_000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            access_kinds(&chunks[0]),
            vec!["scan", "skip", "skip", "skip"]
        );
        assert_eq!(
            access_kinds(&chunks[1]),
            vec!["skip", "scan", "scan", "scan"]
        );
    }

    #[test]
    fn test_split_into_chunks_with_skip_preserved() {
        // Skip entries are carried by whichever chunk is currently being
        // grown and never contribute to the row/byte budget, so here all
        // three scan row groups fit together despite the wide skip in the
        // middle.
        let meta =
            row_groups_with_bytes(&[(30, 100), (1_000, 500), (30, 100), (30, 100)]);
        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
        ]);
        let chunks = plan.split_into_chunks(&meta, 100, 1_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            access_kinds(&chunks[0]),
            vec!["scan", "skip", "scan", "scan"]
        );
    }

    #[test]
    fn test_split_into_chunks_skip_between_chunks() {
        // When a chunk closes on budget, a following Skip is picked up by the
        // next chunk rather than creating an empty one.
        let meta = row_groups_with_bytes(&[(50, 100), (50, 100), (50, 100), (50, 100)]);
        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Scan,
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
        ]);
        let chunks = plan.split_into_chunks(&meta, 100, 10_000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            access_kinds(&chunks[0]),
            vec!["scan", "scan", "skip", "skip"]
        );
        // rg2's Skip still lives in chunk 0 because chunk 0 was still open
        // when we hit rg2; chunk 1 only covers rg3.
        assert_eq!(
            access_kinds(&chunks[1]),
            vec!["skip", "skip", "skip", "scan"]
        );
    }

    #[test]
    fn test_split_into_chunks_preserves_selection() {
        let meta = row_groups_with_bytes(&[(10, 100), (20, 100), (30, 100)]);
        let selection: RowSelection =
            vec![RowSelector::select(5), RowSelector::skip(15)].into();
        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan,
            RowGroupAccess::Selection(selection),
            RowGroupAccess::Scan,
        ]);
        // Budget forces each row group into its own chunk.
        let chunks = plan.split_into_chunks(&meta, 15, 10_000);
        assert_eq!(chunks.len(), 3);
        assert_eq!(access_kinds(&chunks[0]), vec!["scan", "skip", "skip"]);
        assert_eq!(access_kinds(&chunks[1]), vec!["skip", "sel", "skip"]);
        assert_eq!(access_kinds(&chunks[2]), vec!["skip", "skip", "scan"]);
        // The Selection must be preserved verbatim in its chunk.
        let RowGroupAccess::Selection(sel) = &chunks[1].inner()[1] else {
            panic!("expected Selection preserved in chunk");
        };
        let selectors: Vec<_> = sel.clone().into();
        assert_eq!(selectors.len(), 2);
        assert_eq!((selectors[0].skip, selectors[0].row_count), (false, 5));
        assert_eq!((selectors[1].skip, selectors[1].row_count), (true, 15));
    }

    /// Single column schema with a single column named "a" of type `BYTE_ARRAY`/`String`
    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::Type as SchemaType;
        let field = SchemaType::primitive_type_builder("a", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
