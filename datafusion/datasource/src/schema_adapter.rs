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

//! [`SchemaAdapter`] and [`SchemaAdapterFactory`] to adapt file-level record batches to a table schema.
//!
//! Adapter provides a method of translating the RecordBatches that come out of the
//! physical format into how they should be used by DataFusion.  For instance, a schema
//! can be stored external to a parquet file that maps parquet logical types to arrow types.

use arrow::array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::plan_err;
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Factory for creating [`SchemaAdapter`]
///
/// This interface provides a way to implement custom schema adaptation logic
/// for DataSourceExec (for example, to fill missing columns with default value
/// other than null).
///
/// Most users should use [`DefaultSchemaAdapterFactory`]. See that struct for
/// more details and examples.
pub trait SchemaAdapterFactory: Debug + Send + Sync + 'static {
    /// Create a [`SchemaAdapter`]
    ///
    /// Arguments:
    ///
    /// * `projected_table_schema`: The schema for the table, projected to
    ///    include only the fields being output (projected) by the this mapping.
    ///
    /// * `table_schema`: The entire table schema for the table
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter>;
}

/// Creates [`SchemaMapper`]s to map file-level [`RecordBatch`]es to a table
/// schema, which may have a schema obtained from merging multiple file-level
/// schemas.
///
/// This is useful for implementing schema evolution in partitioned datasets.
///
/// See [`DefaultSchemaAdapterFactory`] for more details and examples.
pub trait SchemaAdapter: Send + Sync {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// This is used while reading a file to push down projections by mapping
    /// projected column indexes from the table schema to the file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize>;

    /// Creates a mapping for casting columns from the file schema to the table
    /// schema.
    ///
    /// This is used after reading a record batch. The returned [`SchemaMapper`]:
    ///
    /// 1. Maps columns to the expected columns indexes
    /// 2. Handles missing values (e.g. fills nulls or a default value) for
    ///    columns in the in the table schema not in the file schema
    /// 2. Handles different types: if the column in the file schema has a
    ///    different type than `table_schema`, the mapper will resolve this
    ///    difference (e.g. by casting to the appropriate type)
    ///
    /// Returns:
    /// * a [`SchemaMapper`]
    /// * an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)>;
}

/// Maps, columns from a specific file schema to the table schema.
///
/// See [`DefaultSchemaAdapterFactory`] for more details and examples.
pub trait SchemaMapper: Debug + Send + Sync {
    /// Adapts a `RecordBatch` to match the `table_schema`
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch>;

    /// Adapts a [`RecordBatch`] that does not have all the columns from the
    /// file schema.
    ///
    /// This method is used, for example,  when applying a filter to a subset of
    /// the columns as part of `DataFusionArrowPredicate` when `filter_pushdown`
    /// is enabled.
    ///
    /// This method is slower than `map_batch` as it looks up columns by name.
    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch>;
}

pub trait MissingColumnGeneratorFactory: Debug + Send + Sync {
    /// Create a [`MissingColumnGenerator`] for the given `field` and `file_schema`.
    /// Returns None if the column cannot be generated by this generator.
    /// Otherwise, returns a [`MissingColumnGenerator`] that can generate the missing column.
    fn create(
        &self,
        field: &Field,
        file_schema: &Schema,
    ) -> Option<Arc<dyn MissingColumnGenerator + Send + Sync>>;
}

pub trait MissingColumnGenerator: Debug + Send + Sync {
    /// Generate a missing column for the given `field` from the provided `batch`.
    /// When this method is called `batch` will contain all of the columns declared as dependencies in `dependencies`.
    /// If the column cannot be generated, this method should return an error.
    /// Otherwise, it should return the generated column as an `ArrayRef`.
    /// No casting or post processing is done by this method, so the generated column should match the data type
    /// of the `field` it is being generated for.
    /// The order of
    fn generate(&self, batch: RecordBatch) -> datafusion_common::Result<ArrayRef>;

    /// Returns a list of column names that this generator depends on to generate the missing column.
    /// This is used when creating the `RecordBatch` to ensure that all dependencies are present before calling `generate`.
    /// The dependencies do not need to be declared in any particular order.
    fn dependencies(&self) -> Vec<String>;
}

pub type ColumnGeneratorFactories =
    Vec<Arc<dyn MissingColumnGeneratorFactory + Send + Sync>>;

#[derive(Debug)]
enum FieldSource {
    /// The field is present in the (projected) file schema at the given index
    Table(usize),
    /// The field is generated by the given generator
    Generated(Arc<dyn MissingColumnGenerator + Send + Sync>),
    /// The field will be populated with null
    Null,
}

/// Default  [`SchemaAdapterFactory`] for mapping schemas.
///
/// This can be used to adapt file-level record batches to a table schema and
/// implement schema evolution.
///
/// Given an input file schema and a table schema, this factory returns
/// [`SchemaAdapter`] that return [`SchemaMapper`]s that:
///
/// 1. Reorder columns
/// 2. Cast columns to the correct type
/// 3. Fill missing columns with nulls
///
/// # Errors:
///
/// * If a column in the table schema is non-nullable but is not present in the
///   file schema (i.e. it is missing), the returned mapper tries to fill it with
///   nulls resulting in a schema error.
///
/// # Illustration of Schema Mapping
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///  ┌───────┐   ┌───────┐ │                  ┌───────┐   ┌───────┐   ┌───────┐ │
/// ││  1.0  │   │ "foo" │                   ││ NULL  │   │ "foo" │   │ "1.0" │
///  ├───────┤   ├───────┤ │ Schema mapping   ├───────┤   ├───────┤   ├───────┤ │
/// ││  2.0  │   │ "bar" │                   ││  NULL │   │ "bar" │   │ "2.0" │
///  └───────┘   └───────┘ │────────────────▶ └───────┘   └───────┘   └───────┘ │
/// │                                        │
///  column "c"  column "b"│                  column "a"  column "b"  column "c"│
/// │ Float64       Utf8                     │  Int32        Utf8        Utf8
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘                  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     Input Record Batch                         Output Record Batch
///
///     Schema {                                   Schema {
///      "c": Float64,                              "a": Int32,
///      "b": Utf8,                                 "b": Utf8,
///     }                                           "c": Utf8,
///                                                }
/// ```
///
/// # Example of using the `DefaultSchemaAdapterFactory` to map [`RecordBatch`]s
///
/// Note `SchemaMapping` also supports mapping partial batches, which is used as
/// part of predicate pushdown.
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory};
/// # use datafusion_common::record_batch;
/// // Table has fields "a",  "b" and "c"
/// let table_schema = Schema::new(vec![
///     Field::new("a", DataType::Int32, true),
///     Field::new("b", DataType::Utf8, true),
///     Field::new("c", DataType::Utf8, true),
/// ]);
///
/// // create an adapter to map the table schema to the file schema
/// let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
///
/// // The file schema has fields "c" and "b" but "b" is stored as an 'Float64'
/// // instead of 'Utf8'
/// let file_schema = Schema::new(vec![
///    Field::new("c", DataType::Utf8, true),
///    Field::new("b", DataType::Float64, true),
/// ]);
///
/// // Get a mapping from the file schema to the table schema
/// let (mapper, _indices) = adapter.map_schema(&file_schema).unwrap();
///
/// let file_batch = record_batch!(
///     ("c", Utf8, vec!["foo", "bar"]),
///     ("b", Float64, vec![1.0, 2.0])
/// ).unwrap();
///
/// let mapped_batch = mapper.map_batch(file_batch).unwrap();
///
/// // the mapped batch has the correct schema and the "b" column has been cast to Utf8
/// let expected_batch = record_batch!(
///    ("a", Int32, vec![None, None]),  // missing column filled with nulls
///    ("b", Utf8, vec!["1.0", "2.0"]), // b was cast to string and order was changed
///    ("c", Utf8, vec!["foo", "bar"])
/// ).unwrap();
/// assert_eq!(mapped_batch, expected_batch);
/// ```
#[derive(Clone, Debug, Default)]
pub struct DefaultSchemaAdapterFactory {
    /// Optional generator for missing columns
    ///
    /// This is used to fill in missing columns with a default value other than null.
    /// If this is `None`, then missing columns will be filled with nulls.
    column_generators: ColumnGeneratorFactories,
}

impl DefaultSchemaAdapterFactory {
    /// Create a new factory for mapping batches from a file schema to a table
    /// schema.
    ///
    /// This is a convenience for [`DefaultSchemaAdapterFactory::create`] with
    /// the same schema for both the projected table schema and the table
    /// schema.
    pub fn from_schema(table_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Self {
            column_generators: vec![],
        }
        .create(Arc::clone(&table_schema), table_schema)
    }

    pub fn with_column_generator(
        self,
        generator: Arc<dyn MissingColumnGeneratorFactory + Send + Sync>,
    ) -> Self {
        let mut generators = self.column_generators;
        generators.push(generator);
        Self {
            column_generators: generators,
        }
    }

    pub fn with_column_generators(self, generators: ColumnGeneratorFactories) -> Self {
        Self {
            column_generators: generators,
        }
    }
}

impl SchemaAdapterFactory for DefaultSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DefaultSchemaAdapter {
            projected_table_schema,
            table_schema,
            column_generators: self.column_generators.clone(),
        })
    }
}

/// This SchemaAdapter requires both the table schema and the projected table
/// schema. See  [`SchemaMapping`] for more details
#[derive(Clone, Debug)]
pub(crate) struct DefaultSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetSource
    projected_table_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    table_schema: SchemaRef,
    /// The column generators to use when a column is missing
    column_generators: ColumnGeneratorFactories,
}

impl SchemaAdapter for DefaultSchemaAdapter {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.projected_table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    /// Creates a `SchemaMapping` for casting or mapping the columns from the
    /// file schema to the table schema.
    ///
    /// If the provided `file_schema` contains columns of a different type to
    /// the expected `table_schema`, the method will attempt to cast the array
    /// data from the file schema to the table schema where possible.
    ///
    /// Returns a [`SchemaMapping`] that can be applied to the output batch
    /// along with an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        // Projection is the indexes into the file schema that we need to read
        // Note that readers will NOT respect the order of the columns in projection
        let mut projection = Vec::with_capacity(file_schema.fields().len());

        // Additions to the projection which will be needed to generate missing columns
        // TODO can we refactor this away?
        let mut dependency_projection = Vec::with_capacity(file_schema.fields().len());

        let mut field_sources =
            Vec::with_capacity(self.projected_table_schema.fields().len());

        for field in self.projected_table_schema.fields() {
            if let Some((file_idx, file_field)) = file_schema.fields.find(field.name()) {
                // If the field exists in the file schema, check if we can cast it to the table schema
                match can_cast_types(file_field.data_type(), field.data_type()) {
                    true => {
                        projection.push(file_idx);
                        field_sources.push(FieldSource::Table(projection.len() - 1));
                    }
                    false => {
                        return plan_err!(
                            "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                            file_field.name(),
                            file_field.data_type(),
                            field.data_type()
                        )
                    }
                }
            } else if let Some(generator) = self
                .column_generators
                .iter()
                .find_map(|factory| factory.create(field, file_schema))
            {
                let dependencies = generator.dependencies();
                let mut dependency_indices = Vec::with_capacity(dependencies.len());
                for dep in &dependencies {
                    if let Ok(dep_idx) = file_schema.index_of(dep) {
                        dependency_indices.push(dep_idx);
                    } else {
                        return plan_err!(
                            "Generated column {} depends on column {} but column {} is not present in the file schema, columns present: {:?}",
                            field.name(),
                            dep,
                            dep,
                            file_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                        );
                    }
                }
                dependency_projection.extend(dependency_indices);
                field_sources.push(FieldSource::Generated(generator));
            } else if field.is_nullable() {
                field_sources.push(FieldSource::Null);
            } else {
                return plan_err!(
                    "Column {} is missing from the file schema, cannot be generated, and is non-nullable",
                    field.name()
                );
            }
        }

        for dep in dependency_projection {
            if !projection.contains(&dep) {
                projection.push(dep);
            }
        }

        let (sorted_projection, field_sources) =
            sort_projections_and_sources(&projection, field_sources);

        debug_assert!(sorted_projection.is_sorted());
        debug_assert!(sorted_projection.iter().all_unique());

        Ok((
            Arc::new(SchemaMapping {
                projected_table_schema: self.projected_table_schema.clone(),
                field_sources,
                table_schema: self.table_schema.clone(),
            }),
            sorted_projection,
        ))
    }
}

/// The parquet reader needs projections to be sorted (it does not respect the order of the columns in the projection, only the values)
/// This function adjusts the projections and mappings so that they all reference sorted projections
fn sort_projections_and_sources(
    projection: &[usize],
    mut field_sources: Vec<FieldSource>,
) -> (Vec<usize>, Vec<FieldSource>) {
    // Sort projection and create a mapping from old to new positions
    let mut sorted_projection = projection.to_vec();
    sorted_projection.sort_unstable();

    // Create a mapping from old projection values to their new positions
    let mut new_position_map = HashMap::new();
    for (new_pos, &proj_val) in sorted_projection.iter().enumerate() {
        new_position_map.insert(proj_val, new_pos);
    }

    // Create a mapping from old positions to new positions in the projected schema
    let mut position_mapping = vec![0; projection.len()];
    for (old_pos, &proj_val) in projection.iter().enumerate() {
        position_mapping[old_pos] = *new_position_map
            .get(&proj_val)
            .expect("should always be present");
    }

    // Update field_sources to reflect the new positions
    for source in &mut field_sources {
        if let FieldSource::Table(pos) = source {
            *pos = position_mapping[*pos];
        }
    }

    (sorted_projection, field_sources)
}

/// The SchemaMapping struct holds a mapping from the file schema to the table
/// schema and any necessary type conversions.
///
/// Note, because `map_batch` and `map_partial_batch` functions have different
/// needs, this struct holds two schemas:
///
/// 1. The projected **table** schema
/// 2. The full table schema
///
/// [`map_batch`] is used by the ParquetOpener to produce a RecordBatch which
/// has the projected schema, since that's the schema which is supposed to come
/// out of the execution of this query. Thus `map_batch` uses
/// `projected_table_schema` as it can only operate on the projected fields.
///
/// [`map_partial_batch`]  is used to create a RecordBatch with a schema that
/// can be used for Parquet predicate pushdown, meaning that it may contain
/// fields which are not in the projected schema (as the fields that parquet
/// pushdown filters operate can be completely distinct from the fields that are
/// projected (output) out of the ParquetSource). `map_partial_batch` thus uses
/// `table_schema` to create the resulting RecordBatch (as it could be operating
/// on any fields in the schema).
///
/// [`map_batch`]: Self::map_batch
/// [`map_partial_batch`]: Self::map_partial_batch
#[derive(Debug)]
pub struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion
    /// and it should match the schema of the query result.
    projected_table_schema: SchemaRef,
    /// A mapping from the fields in `projected_table_schema`` to the way to materialize
    /// them from the projected file schema.
    field_sources: Vec<FieldSource>,
    /// The entire table schema, as opposed to the projected_table_schema (which
    /// only contains the columns that we are projecting out of this query).
    /// This contains all fields in the table, regardless of if they will be
    /// projected out or not.
    table_schema: SchemaRef,
}

impl SchemaMapper for SchemaMapping {
    /// Adapts a `RecordBatch` to match the `projected_table_schema` using the stored mapping and
    /// conversions. The produced RecordBatch has a schema that contains only the projected
    /// columns, so if one needs a RecordBatch with a schema that references columns which are not
    /// in the projected, it would be better to use `map_partial_batch`
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        debug_assert_eq!(
            self.projected_table_schema.fields().len(),
            self.field_sources.len()
        );

        let cols = self
            .projected_table_schema
            // go through each field in the projected schema
            .fields()
            .iter()
            // and zip it with the index that maps fields from the projected table schema to the
            // projected file schema in `batch`
            .zip(&self.field_sources)
            // and for each one...
            .map(|(field, source)| -> datafusion_common::Result<_> {
                let column = match source {
                    FieldSource::Table(file_idx) => batch_cols[*file_idx].clone(),
                    FieldSource::Generated(generator) => {
                        generator.generate(batch.clone())?
                    }
                    FieldSource::Null => {
                        debug_assert!(field.is_nullable());
                        new_null_array(field.data_type(), batch_rows)
                    }
                };
                Ok(cast(&column, field.data_type())?)
            })
            .collect::<datafusion_common::Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = self.projected_table_schema.clone();
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    /// Adapts a [`RecordBatch`]'s schema into one that has all the correct output types and only
    /// contains the fields that exist in both the file schema and table schema.
    ///
    /// Unlike `map_batch` this method also preserves the columns that
    /// may not appear in the final output (`projected_table_schema`) but may
    /// appear in push down predicates
    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch> {
        let batch_cols = batch.columns().to_vec();
        let schema = batch.schema();

        // for each field in the batch's schema (which is based on a file, not a table)...
        let (cols, fields) = schema
            .fields()
            .iter()
            .zip(batch_cols.iter())
            .flat_map(|(field, batch_col)| {
                self.table_schema
                    // try to get the same field from the table schema that we have stored in self
                    .field_with_name(field.name())
                    // and if we don't have it, that's fine, ignore it. This may occur when we've
                    // created an external table whose fields are a subset of the fields in this
                    // file, then tried to read data from the file into this table. If that is the
                    // case here, it's fine to ignore because we don't care about this field
                    // anyways
                    .ok()
                    // but if we do have it,
                    .map(|table_field| {
                        // try to cast it into the correct output type. we don't want to ignore this
                        // error, though, so it's propagated.
                        cast(batch_col, table_field.data_type())
                            // and if that works, return the field and column.
                            .map(|new_col| (new_col, table_field.clone()))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema =
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}
