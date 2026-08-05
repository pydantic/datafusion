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

//! Schema-driven nested projection pruning.
//!
//! When a scan's projection consumes a nested column only through a cast to a
//! *narrower* nested type, for example the file contains
//! `events: List<Struct<x, y, z, ...>>` but the expression is
//! `CAST(events AS List<Struct<x, y>>)`, the Parquet reader does not need to
//! fetch or decode the leaves the cast target never names. This module
//! computes which Parquet leaves survive such a cast, and the Arrow type the
//! reader will emit for them, by walking the physical and target type trees
//! in parallel and matching struct fields by name (the equivalent of Spark's
//! `ParquetReadSupport.clipParquetSchema`).
//!
//! This situation arises whenever a table's logical schema declares a nested
//! column narrower than the physical Parquet file: the physical expression
//! adapter rewrites the projected column into exactly such a whole-column
//! cast (see `datafusion_physical_expr_adapter`). Engines like Spark
//! communicate nested projection pruning to the scan this way, as a clipped
//! read *schema* rather than as `get_field` expressions.
//!
//! # Safety of clipping
//!
//! The runtime cast for nested types
//! ([`datafusion_common::nested_struct::cast_column`]) consumes source struct
//! children exclusively by looking up the *target* field names, recursively
//! through list wrappers. Physical subtrees not named by the target are
//! provably dead: removing them from the read cannot change the cast's
//! output. That holds for *any* [`CastExpr`] over a nested type, not just the
//! ones the schema adapter inserts:
//! [`datafusion_expr_common::columnar_value::ColumnarValue::cast_to`] routes
//! every cast for which
//! [`requires_nested_struct_cast`](datafusion_common::nested_struct::requires_nested_struct_cast)
//! holds — the same predicate the projection analysis gates on — through
//! `cast_column`.
//!
//! Struct-level nullability is preserved because the Parquet reader
//! reconstructs ancestor validity from the definition levels of any surviving
//! leaf, so every struct level that is clipped must keep at least one leaf.
//! A struct cast with zero field-name overlap at *any* nesting depth would
//! break that: the reader drops a field whose leaves are all masked out, so
//! the emitted type would not match the one predicted here. Such a cast is
//! rejected during physical planning
//! (`datafusion_common::nested_struct::validate_struct_compatibility`, called
//! recursively from `DefaultPhysicalExprAdapter::rewrite`) and by the logical
//! planner's own castability check, so it should never reach this module; if
//! one does anyway (a custom `PhysicalExprAdapter` could build one),
//! [`clip_for_cast`] detects the empty level and declines to clip.
//!
//! The clip is *total*: any type shape it does not understand (maps,
//! dictionaries, wrapper-kind mismatches, ...) keeps all of its leaves, so
//! the worst case is today's behavior of reading the full column. Map values
//! are deliberately not clipped: the runtime cast routes maps through Arrow's
//! positional struct cast, which requires all children to be present. Nor are
//! `ListView`/`LargeListView`/`Dictionary` wrappers clipped here, even though
//! `cast_column` does recurse through them by name. That is a conservative
//! choice (safe, since the worst case is still just a full read) left as a
//! candidate follow-up rather than something this module currently handles.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

/// The single child type one level of container nesting wraps, or `None` for
/// a type this module does not descend through (leaves, `Struct`, `Map`, and
/// wrapper kinds this module intentionally does not clip, see the module
/// doc). Shared by [`count_leaves`] and [`contains_struct`], which otherwise
/// need to agree on the exact same set of container variants.
fn nested_child(dt: &DataType) -> Option<&DataType> {
    match dt {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => Some(f.data_type()),
        DataType::Dictionary(_, value) => Some(value),
        DataType::RunEndEncoded(_, value) => Some(value.data_type()),
        _ => None,
    }
}

/// Clip `physical` against `cast_target`, returning the Parquet leaves the
/// cast actually consumes (as offsets relative to the root column's first
/// leaf, sorted ascending and non-empty) together with the Arrow type the
/// reader will emit for exactly those leaves.
///
/// Returns `None` when nothing can be pruned (every leaf is consumed, or the
/// shapes do not allow safe clipping), in which case the caller should read
/// the whole column as before. This function never fails: unknown shapes
/// degrade to keeping all leaves.
pub(crate) fn clip_for_cast(
    physical: &DataType,
    cast_target: &DataType,
) -> Option<(Vec<usize>, DataType)> {
    let total = count_leaves(physical);
    let mut kept = Vec::new();
    let mut next_leaf = 0;
    let mut unclippable = false;
    let pruned_type = clip_type(
        physical,
        cast_target,
        &mut next_leaf,
        &mut kept,
        &mut unclippable,
    );
    debug_assert_eq!(next_leaf, total, "leaf accounting must cover the type");
    if unclippable || kept.is_empty() || kept.len() >= total {
        return None;
    }
    Some((kept, pruned_type))
}

/// Number of Parquet leaf columns a (Parquet-derived) Arrow type occupies.
pub(crate) fn count_leaves(dt: &DataType) -> usize {
    match dt {
        DataType::Struct(fields) => {
            fields.iter().map(|f| count_leaves(f.data_type())).sum()
        }
        _ => nested_child(dt).map_or(1, count_leaves),
    }
}

/// Does this type contain a struct at any nesting depth? Used as a fast-path
/// gate: a root with no struct anywhere in its type has no leaves this
/// module could ever clip.
pub(crate) fn contains_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(_)) || nested_child(dt).is_some_and(contains_struct)
}

/// Above this many target fields, matching physical children against them one
/// by one turns into a quadratic string comparison; build a name lookup
/// instead. Below it the map's allocation costs more than the linear scan it
/// saves (Spark's `ParquetReadSupport.clipParquetGroupFields` builds the map
/// unconditionally; struct widths in practice are small enough that the
/// threshold is worth the branch).
const LINEAR_FIELD_SCAN_MAX: usize = 8;

/// Find `name` among `fields`, using `by_name` when it was worth building.
/// Duplicate names resolve to the first occurrence either way.
fn lookup_field<'a>(
    fields: &'a Fields,
    by_name: &Option<HashMap<&'a str, &'a FieldRef>>,
    name: &str,
) -> Option<&'a FieldRef> {
    match by_name {
        Some(map) => map.get(name).copied(),
        None => fields.iter().find(|f| f.name() == name),
    }
}

/// Recursive walker: advances `next_leaf` across every leaf of `physical`,
/// pushing the offsets the cast target consumes into `kept`, and returns the
/// Arrow type the reader emits for those kept leaves.
///
/// `unclippable` is set when a shape is encountered whose emitted type this
/// module cannot predict; the caller must then read the whole column. The walk
/// still runs to completion so `next_leaf` stays a valid leaf count.
fn clip_type(
    physical: &DataType,
    target: &DataType,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
    unclippable: &mut bool,
) -> DataType {
    match (physical, target) {
        (DataType::Struct(p_children), DataType::Struct(t_children)) => {
            let t_by_name = (t_children.len() > LINEAR_FIELD_SCAN_MAX).then(|| {
                let mut map = HashMap::with_capacity(t_children.len());
                for tc in t_children.iter() {
                    map.entry(tc.name().as_str()).or_insert(tc);
                }
                map
            });
            let kept_children: Fields = p_children
                .iter()
                .filter_map(|pc| {
                    let Some(tc) = lookup_field(t_children, &t_by_name, pc.name()) else {
                        skip_leaves(pc.data_type(), next_leaf);
                        return None;
                    };
                    let before = kept.len();
                    let pruned = clip_type(
                        pc.data_type(),
                        tc.data_type(),
                        next_leaf,
                        kept,
                        unclippable,
                    );
                    if kept.len() == before {
                        // This child matched by name but kept no leaves at
                        // all, which only happens when a nested struct level
                        // below it shares no field name with its target. The
                        // reader drops a field whose leaves are all masked
                        // out, so the emitted type could not be predicted;
                        // give up on clipping this column entirely rather
                        // than promise a type the decoder will not produce.
                        // (`DefaultPhysicalExprAdapter` never builds such a
                        // cast — `validate_struct_compatibility` rejects a
                        // zero-overlap struct level at planning time — but a
                        // custom `PhysicalExprAdapter` could.)
                        *unclippable = true;
                    }
                    Some(field_with_type(pc, pruned))
                })
                .collect();
            DataType::Struct(kept_children)
        }
        (DataType::List(p_item), DataType::List(t_item)) => {
            let pruned = clip_type(
                p_item.data_type(),
                t_item.data_type(),
                next_leaf,
                kept,
                unclippable,
            );
            DataType::List(field_with_type(p_item, pruned))
        }
        (DataType::LargeList(p_item), DataType::LargeList(t_item)) => {
            let pruned = clip_type(
                p_item.data_type(),
                t_item.data_type(),
                next_leaf,
                kept,
                unclippable,
            );
            DataType::LargeList(field_with_type(p_item, pruned))
        }
        // Anything else, leaf pairs, wrapper-kind mismatches, maps,
        // dictionaries, fixed-size lists, views, is kept wholesale.
        _ => keep_all_leaves(physical, next_leaf, kept),
    }
}

/// Keep every leaf of `dt` (no pruning below this point); returns `dt`
/// unchanged since nothing was clipped.
fn keep_all_leaves(
    dt: &DataType,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
) -> DataType {
    let n = count_leaves(dt);
    kept.extend(*next_leaf..*next_leaf + n);
    *next_leaf += n;
    dt.clone()
}

fn skip_leaves(dt: &DataType, next_leaf: &mut usize) {
    *next_leaf += count_leaves(dt);
}

/// A projected root column that is consumed through a cast to a narrower
/// nested type (`CAST(col AS target_type)`), recorded during projection
/// analysis.
#[derive(Debug, Clone)]
pub(crate) struct CastColumnAccess {
    /// Arrow root column index of the column in the file schema.
    pub(crate) root_index: usize,
    /// The cast's target type.
    pub(crate) target_type: DataType,
}

/// Rebuild `field` with a new data type, preserving name, nullability and
/// metadata.
pub(crate) fn field_with_type(field: &Field, data_type: DataType) -> FieldRef {
    Arc::new(field.clone().with_data_type(data_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8(name: &str) -> Field {
        Field::new(name, DataType::Utf8, true)
    }

    fn int64(name: &str) -> Field {
        Field::new(name, DataType::Int64, true)
    }

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn count_leaves_shapes() {
        assert_eq!(count_leaves(&DataType::Int32), 1);
        assert_eq!(count_leaves(&struct_of(vec![utf8("a"), int64("b")])), 2);
        assert_eq!(
            count_leaves(&list_of(struct_of(vec![
                utf8("a"),
                struct_of(vec![int64("x"), int64("y")]).into_field("s")
            ]))),
            3
        );
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                struct_of(vec![utf8("key"), int64("value")]),
                false,
            )),
            false,
        );
        assert_eq!(count_leaves(&map), 2);
        let dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(count_leaves(&dict), 1);
    }

    /// `{a, b, c} CAST TO {b}` keeps only b's leaf.
    #[test]
    fn clip_struct_subset() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![int64("b")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![1]);
        assert_eq!(emitted, struct_of(vec![int64("b")]));
    }

    /// Target field order does not matter: emitted type is in physical order.
    #[test]
    fn clip_struct_reordered_target() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![utf8("c"), utf8("a")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 2]);
        assert_eq!(emitted, struct_of(vec![utf8("a"), utf8("c")]));
    }

    /// Target fields missing from the physical type are ignored (the runtime
    /// cast null-fills them).
    #[test]
    fn clip_struct_target_field_missing_from_physical() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("a"), int64("z")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted, struct_of(vec![utf8("a")]));
    }

    /// Leaf-level type mismatch (promotion) still clips: the emitted type
    /// keeps the physical leaf type; the cast performs the promotion.
    #[test]
    fn clip_keeps_physical_leaf_types() {
        let physical =
            struct_of(vec![Field::new("x", DataType::Int32, true), utf8("pad")]);
        let target = struct_of(vec![int64("x")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted,
            struct_of(vec![Field::new("x", DataType::Int32, true)])
        );
    }

    /// Nested struct-in-struct clips at both levels.
    #[test]
    fn clip_nested_struct() {
        let inner_physical = struct_of(vec![int64("x"), utf8("pad_inner")]);
        let physical = struct_of(vec![
            inner_physical.clone().into_field("inner"),
            utf8("pad_outer"),
        ]);
        let target = struct_of(vec![struct_of(vec![int64("x")]).into_field("inner")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted,
            struct_of(vec![struct_of(vec![int64("x")]).into_field("inner")])
        );
    }

    /// List<Struct>, the headline case.
    #[test]
    fn clip_list_of_struct() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("y"), utf8("pad")]));
        let target = list_of(struct_of(vec![int64("x"), utf8("y")]));
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);
        assert_eq!(emitted, list_of(struct_of(vec![int64("x"), utf8("y")])));
    }

    /// Two levels of `list<struct>` nesting, the inner one also narrowed,
    /// the `events: array<struct<..., items: array<struct<...>>>>` shape
    /// reported in `datafusion-comet#4859`, where a sibling struct field at
    /// the outer level (`aux`, standing in for that report's
    /// `latency_parts`) is dropped entirely rather than clipped.
    #[test]
    fn clip_two_level_nested_list_of_struct() {
        let physical = list_of(struct_of(vec![
            int64("a"),
            utf8("pad"),
            struct_of(vec![int64("x"), utf8("y")]).into_field("aux"),
            list_of(struct_of(vec![int64("g"), utf8("pad2")])).into_field("items"),
        ]));
        let target = list_of(struct_of(vec![
            int64("a"),
            list_of(struct_of(vec![int64("g")])).into_field("items"),
        ]));

        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        // a=0, pad=1, aux.x=2, aux.y=3, items.g=4, items.pad2=5: only a and
        // items.g survive; pad, all of aux, and items.pad2 are dropped.
        assert_eq!(kept, vec![0, 4]);
        assert_eq!(
            emitted,
            list_of(struct_of(vec![
                int64("a"),
                list_of(struct_of(vec![int64("g")])).into_field("items"),
            ]))
        );
    }

    #[test]
    fn clip_large_list_of_struct() {
        let item = |fields| Arc::new(Field::new("item", struct_of(fields), true));
        let physical = DataType::LargeList(item(vec![int64("x"), utf8("pad")]));
        let target = DataType::LargeList(item(vec![int64("x")]));
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted, DataType::LargeList(item(vec![int64("x")])));
    }

    /// Wrapper-kind mismatch cannot be clipped.
    #[test]
    fn no_clip_on_wrapper_mismatch() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("pad")]));
        let target = DataType::LargeList(Arc::new(Field::new(
            "item",
            struct_of(vec![int64("x")]),
            true,
        )));
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Maps are opaque: never clipped.
    #[test]
    fn no_clip_on_map() {
        let entries = |fields| Arc::new(Field::new("entries", struct_of(fields), false));
        let physical =
            DataType::Map(entries(vec![utf8("key"), int64("a"), int64("b")]), false);
        let target = DataType::Map(entries(vec![utf8("key"), int64("a")]), false);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Identical types: nothing to prune.
    #[test]
    fn no_clip_when_identical() {
        let t = struct_of(vec![utf8("a"), int64("b")]);
        assert!(clip_for_cast(&t, &t).is_none());
    }

    /// Non-nested types: nothing to prune.
    #[test]
    fn no_clip_on_primitives() {
        assert!(clip_for_cast(&DataType::Int32, &DataType::Int64).is_none());
    }

    /// A struct level with zero field-name overlap can't actually reach this
    /// code: `validate_struct_compatibility` rejects it during physical
    /// planning (see the module doc), so `clip_for_cast` is only ever called
    /// with targets that overlap at every nesting level. If it were reached
    /// anyway, the generic catch-all keeps every leaf, still safe, just
    /// unpruned.
    #[test]
    fn no_clip_on_zero_overlap() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("z")]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// A *nested* struct level with zero field-name overlap must not be
    /// clipped, even when a sibling keeps leaves. The reader drops a field
    /// whose leaves are all masked out (pinned by
    /// [`reader_drops_struct_child_with_no_selected_leaves`]), so predicting
    /// `{inner: Struct[], c}` here would be a schema the decoder never
    /// produces. Read the whole column instead.
    #[test]
    fn no_clip_when_nested_struct_level_has_no_overlap() {
        let physical = struct_of(vec![
            struct_of(vec![int64("a"), int64("b")]).into_field("inner"),
            int64("c"),
        ]);
        let target = struct_of(vec![
            struct_of(vec![int64("z")]).into_field("inner"),
            int64("c"),
        ]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Same, one level deeper and behind a list wrapper.
    #[test]
    fn no_clip_when_nested_list_struct_level_has_no_overlap() {
        let physical = struct_of(vec![
            list_of(struct_of(vec![int64("a"), int64("b")])).into_field("items"),
            int64("c"),
        ]);
        let target = struct_of(vec![
            list_of(struct_of(vec![int64("z")])).into_field("items"),
            int64("c"),
        ]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Wide structs take the name-map matching path rather than the linear
    /// scan; both must produce the same clip.
    #[test]
    fn clip_wide_struct_matches_by_name() {
        let width = LINEAR_FIELD_SCAN_MAX * 4;
        let physical = struct_of((0..width).map(|i| int64(&format!("f{i}"))).collect());
        // Even fields only, declared in reverse order: the emitted type is
        // still in physical order.
        let target = struct_of(
            (0..width)
                .rev()
                .filter(|i| i % 2 == 0)
                .map(|i| int64(&format!("f{i}")))
                .collect(),
        );
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, (0..width).filter(|i| i % 2 == 0).collect::<Vec<_>>());
        assert_eq!(
            emitted,
            struct_of(
                (0..width)
                    .filter(|i| i % 2 == 0)
                    .map(|i| int64(&format!("f{i}")))
                    .collect()
            )
        );
    }

    /// Duplicate physical field names both match the single target field and
    /// are both kept, which is what the reader emits for that mask.
    #[test]
    fn clip_keeps_duplicate_physical_field_names() {
        let physical = struct_of(vec![int64("a"), utf8("pad"), int64("a")]);
        let target = struct_of(vec![int64("a")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 2]);
        assert_eq!(emitted, struct_of(vec![int64("a"), int64("a")]));
    }

    /// Pins the arrow-rs behavior the empty-level guard above depends on: a
    /// struct child none of whose leaves are selected disappears from the
    /// type the reader emits, rather than surviving as an empty struct.
    #[test]
    fn reader_drops_struct_child_with_no_selected_leaves() {
        use arrow::array::{ArrayRef, Int64Array, StructArray};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::{ArrowWriter, ProjectionMask};

        let inner_fields = Fields::from(vec![int64("a"), int64("b")]);
        let outer_fields = Fields::from(vec![
            Field::new("inner", DataType::Struct(inner_fields.clone()), true),
            int64("c"),
        ]);
        let inner: ArrayRef = Arc::new(StructArray::new(
            inner_fields,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            ],
            None,
        ));
        let outer = StructArray::new(
            outer_fields.clone(),
            vec![inner, Arc::new(Int64Array::from(vec![5, 6])) as ArrayRef],
            None,
        );
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "s",
            DataType::Struct(outer_fields),
            true,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer)]).unwrap();

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        assert_eq!(builder.parquet_schema().num_columns(), 3);
        // Keep only s.c (leaf 2): every leaf of s.inner is masked out.
        let mask = ProjectionMask::leaves(builder.parquet_schema(), [2usize]);
        let reader = builder.with_projection(mask).build().unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &struct_of(vec![int64("c")]),
            "the fully masked `inner` child is dropped, not emitted as an empty struct"
        );
    }

    /// Pins the arrow-rs behavior this module relies on: selecting a subset
    /// of leaves under a `List<Struct>` column with `ProjectionMask::leaves`
    /// makes the reader emit exactly the type predicted by [`clip_for_cast`],
    /// and null list rows / null struct elements survive (their validity is
    /// reconstructed from the surviving leaves' definition levels).
    #[test]
    fn arrow_reader_emits_clipped_type_for_masked_list_struct() {
        use arrow::array::{
            Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::{ArrowWriter, ProjectionMask};

        let item_fields = Fields::from(vec![int64("x"), utf8("y"), utf8("pad")]);
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(item_fields.clone()),
            true,
        ));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::clone(&item_field)),
            true,
        )]));

        // 3 elements; element 1 is a NULL struct. Rows: [e0, e1], NULL, [e2].
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            Arc::new(StringArray::from(vec![Some("p0"), None, Some("p2")])),
        ];
        let struct_validity = NullBuffer::from(vec![true, false, true]);
        let values = StructArray::new(item_fields, columns, Some(struct_validity));
        let list_validity = NullBuffer::from(vec![true, false, true]);
        let events = ListArray::new(
            item_field,
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(values),
            Some(list_validity),
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(events)]).unwrap();

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Clip to the narrow target {x, y}.
        let physical = batch.schema().field(0).data_type().clone();
        let target = list_of(struct_of(vec![int64("x"), utf8("y")]));
        let (kept, predicted_type) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), kept.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(out.len(), 1);
        let out = &out[0];

        // Emitted type matches the prediction.
        assert_eq!(out.schema().field(0).data_type(), &predicted_type);

        // Null semantics survive the clip.
        let events = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(events.is_valid(0));
        assert!(events.is_null(1));
        assert!(events.is_valid(2));
        let structs = events
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(structs.len(), 3);
        assert!(structs.is_valid(0));
        assert!(structs.is_null(1));
        assert!(structs.is_valid(2));
        let x = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x.value(0), 1);
        assert_eq!(x.value(2), 3);
    }

    trait IntoField {
        fn into_field(self, name: &str) -> Field;
    }

    impl IntoField for DataType {
        fn into_field(self, name: &str) -> Field {
            Field::new(name, self, true)
        }
    }
}

/// Randomised differential harness for [`clip_for_cast`].
///
/// Each case builds a random nested Arrow type, writes a small batch of that
/// type to an in-memory Parquet file, derives a random narrowing cast target
/// from the type the *reader* reports for that file, and then checks the two
/// promises this module makes to
/// [`crate::projection_read_plan::build_read_plan_with_cast_clipping`]:
///
/// 1. the Arrow type predicted for the clipped leaf set is exactly the type
///    the arrow-rs decoder emits under `ProjectionMask::leaves(kept)`, and
/// 2. the values (and nulls, at every nesting level) of the clipped read are
///    exactly those of an unclipped read with the dropped subtrees removed.
///
/// Failures print the seed and case index; re-running with
/// `NESTED_PRUNING_FUZZ_SEED0`/`_SEEDS`/`_CASES` reproduces them exactly.
#[cfg(test)]
mod fuzz {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeListArray, Float64Array,
        Int32Array, Int64Array, LargeListArray, ListArray, MapArray, StringArray,
        StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Int32Type, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::{ArrowWriter, ProjectionMask};

    /// Maximum nesting depth of a generated physical type.
    const MAX_DEPTH: usize = 4;
    /// Maximum number of fields per generated struct level.
    const MAX_WIDTH: usize = 6;
    /// Rows per generated file.
    const ROWS: usize = 8;

    /// Field-name pool. Small enough that duplicate names inside one struct
    /// happen naturally, and includes non-ASCII names.
    const NAMES: [&str; 8] = ["a", "b", "c", "x", "id", "ключ", "名前", "f-1"];

    // ----------------------------------------------------------------- rng

    /// xorshift64: seeded, reproducible, no dependency.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }

        /// Inclusive range.
        fn range(&mut self, lo: usize, hi: usize) -> usize {
            lo + self.below(hi - lo + 1)
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.next_u64() % 100 < percent
        }
    }

    // ------------------------------------------------------ type generator

    fn gen_leaf(rng: &mut Rng) -> DataType {
        match rng.below(6) {
            0 => DataType::Int32,
            1 => DataType::Int64,
            2 => DataType::Utf8,
            3 => DataType::Boolean,
            4 => DataType::Float64,
            _ => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }

    fn gen_type(rng: &mut Rng, depth: usize) -> DataType {
        if depth == 0 {
            return gen_leaf(rng);
        }
        match rng.below(12) {
            0..=4 => DataType::Struct(gen_fields(rng, depth - 1)),
            5..=6 => DataType::List(Arc::new(gen_item_field(rng, depth - 1))),
            7 => DataType::LargeList(Arc::new(gen_item_field(rng, depth - 1))),
            8 => DataType::FixedSizeList(Arc::new(gen_item_field(rng, depth - 1)), 2),
            9 => gen_map(rng, depth - 1),
            _ => gen_leaf(rng),
        }
    }

    fn gen_fields(rng: &mut Rng, depth: usize) -> Fields {
        // Bias away from width-1 structs: a level with a single field can
        // only ever be kept whole or emptied, so it exercises little.
        let n = rng.range(1, MAX_WIDTH).max(rng.range(1, 2));
        Fields::from(
            (0..n)
                .map(|_| {
                    let name = NAMES[rng.below(NAMES.len())];
                    Field::new(name, gen_type(rng, depth), !rng.chance(20))
                })
                .collect::<Vec<_>>(),
        )
    }

    fn gen_item_field(rng: &mut Rng, depth: usize) -> Field {
        Field::new("item", gen_type(rng, depth), !rng.chance(20))
    }

    fn gen_map(rng: &mut Rng, depth: usize) -> DataType {
        let entries = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", gen_type(rng, depth), true),
        ]);
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            false,
        )
    }

    /// A root column type that contains a struct somewhere (otherwise there is
    /// nothing this module could ever clip).
    fn gen_root_field(rng: &mut Rng) -> Field {
        loop {
            let dt = gen_type(rng, MAX_DEPTH);
            if contains_struct(&dt) && count_leaves(&dt) <= 32 {
                return Field::new("col", dt, true);
            }
        }
    }

    // ---------------------------------------------------- target generator

    /// Derive a random cast target from a physical type: drop struct fields,
    /// reorder them, widen leaves, invent absent names, and occasionally swap
    /// a wrapper kind.
    fn gen_target(rng: &mut Rng, physical: &DataType) -> DataType {
        // Occasionally replace a subtree's target with an unrelated type, so
        // the walker's catch-all (keep every leaf below here) is exercised
        // with shapes no schema evolution would ever produce.
        if rng.chance(4) {
            return gen_type(rng, 1);
        }
        match physical {
            DataType::Struct(fields) => {
                let mut kept: Vec<Field> = fields
                    .iter()
                    .filter(|_| rng.chance(55))
                    .map(|f| f.as_ref().clone())
                    .collect();
                // Usually make sure at least one field survives, so the clip
                // has something to keep; the remaining 10% exercise the
                // zero-overlap bail-out.
                if kept.is_empty() && !rng.chance(10) {
                    kept.push(fields[rng.below(fields.len())].as_ref().clone());
                }
                // Recurse, then shuffle so target order differs from physical.
                let mut out: Vec<Field> = kept
                    .into_iter()
                    .map(|f| {
                        let dt = gen_target(rng, f.data_type());
                        f.with_data_type(dt)
                    })
                    .collect();
                if rng.chance(20) {
                    out.push(Field::new("__absent__", DataType::Int64, true));
                }
                for i in (1..out.len()).rev() {
                    out.swap(i, rng.below(i + 1));
                }
                DataType::Struct(Fields::from(out))
            }
            DataType::List(item) => {
                let inner = item
                    .as_ref()
                    .clone()
                    .with_data_type(gen_target(rng, item.data_type()));
                if rng.chance(10) {
                    DataType::LargeList(Arc::new(inner))
                } else {
                    DataType::List(Arc::new(inner))
                }
            }
            DataType::LargeList(item) => {
                let inner = item
                    .as_ref()
                    .clone()
                    .with_data_type(gen_target(rng, item.data_type()));
                if rng.chance(10) {
                    DataType::List(Arc::new(inner))
                } else {
                    DataType::LargeList(Arc::new(inner))
                }
            }
            DataType::FixedSizeList(item, n) => {
                let inner = item
                    .as_ref()
                    .clone()
                    .with_data_type(gen_target(rng, item.data_type()));
                let n = if rng.chance(10) { n + 1 } else { *n };
                DataType::FixedSizeList(Arc::new(inner), n)
            }
            DataType::Map(entries, sorted) => {
                let DataType::Struct(children) = entries.data_type() else {
                    return physical.clone();
                };
                let value = children[1].as_ref().clone();
                let value = value
                    .clone()
                    .with_data_type(gen_target(rng, value.data_type()));
                let new_entries = Fields::from(vec![children[0].as_ref().clone(), value]);
                DataType::Map(
                    Arc::new(
                        entries
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(new_entries)),
                    ),
                    *sorted,
                )
            }
            DataType::Dictionary(key, value) => {
                DataType::Dictionary(key.clone(), Box::new(gen_target(rng, value)))
            }
            DataType::Int32 if rng.chance(30) => DataType::Int64,
            other => other.clone(),
        }
    }

    // ---------------------------------------------------- data generator

    fn validity(rng: &mut Rng, nullable: bool, len: usize) -> Vec<bool> {
        if !nullable || len == 0 {
            return vec![true; len];
        }
        // 25% of arrays have no nulls at all, 10% are entirely null.
        if rng.chance(25) {
            return vec![true; len];
        }
        let all_null = rng.chance(10);
        (0..len)
            .map(|_| !all_null && !rng.chance(30))
            .collect::<Vec<_>>()
    }

    fn null_buffer(valid: &[bool]) -> Option<NullBuffer> {
        valid
            .iter()
            .any(|v| !v)
            .then(|| NullBuffer::from(valid.to_vec()))
    }

    fn gen_array(rng: &mut Rng, field: &Field, len: usize) -> ArrayRef {
        let nullable = field.is_nullable();
        match field.data_type() {
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .map(|f| gen_array(rng, f, len))
                    .collect::<Vec<_>>();
                let valid = validity(rng, nullable, len);
                Arc::new(StructArray::new(
                    fields.clone(),
                    children,
                    null_buffer(&valid),
                ))
            }
            DataType::List(item) => {
                let valid = validity(rng, nullable, len);
                let lengths = valid
                    .iter()
                    .map(|v| if *v { rng.below(3) } else { 0 })
                    .collect::<Vec<_>>();
                let total: usize = lengths.iter().sum();
                let values = gen_array(rng, item, total);
                Arc::new(ListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::<i32>::from_lengths(lengths),
                    values,
                    null_buffer(&valid),
                ))
            }
            DataType::LargeList(item) => {
                let valid = validity(rng, nullable, len);
                let lengths = valid
                    .iter()
                    .map(|v| if *v { rng.below(3) } else { 0 })
                    .collect::<Vec<_>>();
                let total: usize = lengths.iter().sum();
                let values = gen_array(rng, item, total);
                Arc::new(LargeListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::<i64>::from_lengths(lengths),
                    values,
                    null_buffer(&valid),
                ))
            }
            DataType::FixedSizeList(item, n) => {
                let valid = validity(rng, nullable, len);
                let values = gen_array(rng, item, len * *n as usize);
                Arc::new(FixedSizeListArray::new(
                    Arc::clone(item),
                    *n,
                    values,
                    null_buffer(&valid),
                ))
            }
            DataType::Map(entries_field, sorted) => {
                let DataType::Struct(children) = entries_field.data_type() else {
                    unreachable!("map entries are always a struct")
                };
                let valid = validity(rng, nullable, len);
                let lengths = valid
                    .iter()
                    .map(|v| if *v { rng.below(3) } else { 0 })
                    .collect::<Vec<_>>();
                let total: usize = lengths.iter().sum();
                let keys: ArrayRef = Arc::new(StringArray::from_iter_values(
                    (0..total).map(|i| format!("k{i}")),
                ));
                let values = gen_array(rng, &children[1], total);
                let entries =
                    StructArray::new(children.clone(), vec![keys, values], None);
                Arc::new(MapArray::new(
                    Arc::clone(entries_field),
                    OffsetBuffer::<i32>::from_lengths(lengths),
                    entries,
                    null_buffer(&valid),
                    *sorted,
                ))
            }
            DataType::Dictionary(_, _) => {
                let valid = validity(rng, nullable, len);
                let words = ["alpha", "beta", "gamma"];
                let it = valid
                    .iter()
                    .enumerate()
                    .map(|(i, v)| v.then_some(words[i % words.len()]));
                Arc::new(DictionaryArray::<Int32Type>::from_iter(it))
            }
            DataType::Int32 => {
                let valid = validity(rng, nullable, len);
                Arc::new(Int32Array::from(
                    valid
                        .iter()
                        .enumerate()
                        .map(|(i, v)| v.then_some(i as i32))
                        .collect::<Vec<_>>(),
                ))
            }
            DataType::Int64 => {
                let valid = validity(rng, nullable, len);
                Arc::new(Int64Array::from(
                    valid
                        .iter()
                        .enumerate()
                        .map(|(i, v)| v.then_some(i as i64 * 7))
                        .collect::<Vec<_>>(),
                ))
            }
            DataType::Boolean => {
                let valid = validity(rng, nullable, len);
                Arc::new(BooleanArray::from(
                    valid
                        .iter()
                        .enumerate()
                        .map(|(i, v)| v.then_some(i % 2 == 0))
                        .collect::<Vec<_>>(),
                ))
            }
            DataType::Float64 => {
                let valid = validity(rng, nullable, len);
                Arc::new(Float64Array::from(
                    valid
                        .iter()
                        .enumerate()
                        .map(|(i, v)| v.then_some(i as f64 + 0.5))
                        .collect::<Vec<_>>(),
                ))
            }
            DataType::Utf8 => {
                let valid = validity(rng, nullable, len);
                Arc::new(StringArray::from(
                    valid
                        .iter()
                        .enumerate()
                        .map(|(i, v)| v.then(|| format!("v{i}")))
                        .collect::<Vec<_>>(),
                ))
            }
            other => unreachable!("generator does not produce {other:?}"),
        }
    }

    // --------------------------------------------------------- parquet io

    /// Write `batch`, optionally *without* the embedded Arrow schema.
    ///
    /// Files written by other engines (Spark, parquet-mr) carry no Arrow
    /// metadata, so the reader derives the Arrow type from the Parquet
    /// schema alone: three-level list encodings, `element`/`key_value` field
    /// names, dictionaries decoded as their value type, `LargeList` and
    /// `FixedSizeList` coming back as `List`. That is exactly the shape the
    /// motivating workload (a Spark-written `List<Struct>`) has, and it
    /// produces physical types this generator could not otherwise reach.
    fn write_file(schema: SchemaRef, batch: &RecordBatch, embed_schema: bool) -> Bytes {
        let mut buf = Vec::new();
        let options = ArrowWriterOptions::new().with_skip_arrow_metadata(!embed_schema);
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buf, schema, options).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    /// Read the single column of `data`, optionally through a leaf mask.
    /// Returns the emitted type and the concatenated column.
    fn read_column(data: &Bytes, mask: Option<&[usize]>) -> (DataType, ArrayRef) {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
        if let Some(leaves) = mask {
            let mask =
                ProjectionMask::leaves(builder.parquet_schema(), leaves.iter().copied());
            builder = builder.with_projection(mask);
        }
        let reader = builder.with_batch_size(1024).build().unwrap();
        let batches = reader.map(|b| b.unwrap()).collect::<Vec<_>>();
        assert_eq!(batches.len(), 1, "fixture is one batch");
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 1);
        (
            batch.schema().field(0).data_type().clone(),
            Arc::clone(batch.column(0)),
        )
    }

    // ------------------------------------------------------- verification

    /// Do two (possibly absent) null buffers mark the same slots valid?
    fn same_validity(a: Option<&NullBuffer>, b: Option<&NullBuffer>, len: usize) -> bool {
        (0..len)
            .all(|i| a.is_none_or(|n| n.is_valid(i)) == b.is_none_or(|n| n.is_valid(i)))
    }

    /// Assert that the clipped read carries exactly the values and nulls of
    /// the unclipped read, with the subtrees `predicted` dropped removed.
    ///
    /// Walks the two trees in parallel rather than rebuilding a pruned array,
    /// so it compares what the decoder produced instead of what a validating
    /// Arrow constructor would accept. `predicted`'s struct children are
    /// always a *subsequence* (in physical order) of the physical children,
    /// which is what makes the greedy name matching below well defined even
    /// with duplicate field names.
    fn assert_same(
        full: &ArrayRef,
        clipped: &ArrayRef,
        predicted: &DataType,
        path: &str,
        ctx: &dyn Fn() -> String,
    ) {
        assert_eq!(
            full.len(),
            clipped.len(),
            "length differs at {path}\n{}",
            ctx()
        );
        assert!(
            same_validity(full.nulls(), clipped.nulls(), full.len()),
            "validity differs at {path}: {:?} vs {:?}\n{}",
            full.nulls(),
            clipped.nulls(),
            ctx()
        );
        match (full.data_type(), predicted) {
            (DataType::Struct(p_fields), DataType::Struct(t_fields)) => {
                assert!(
                    !t_fields.is_empty(),
                    "clip produced an empty struct level at {path}\n{}",
                    ctx()
                );
                let full_s = full.as_any().downcast_ref::<StructArray>().unwrap();
                let clipped_s = clipped.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(clipped_s.num_columns(), t_fields.len());
                let mut ti = 0;
                for (i, pf) in p_fields.iter().enumerate() {
                    if ti < t_fields.len() && pf.name() == t_fields[ti].name() {
                        assert_same(
                            full_s.column(i),
                            clipped_s.column(ti),
                            t_fields[ti].data_type(),
                            &format!("{path}.{}", pf.name()),
                            ctx,
                        );
                        ti += 1;
                    }
                }
                assert_eq!(
                    ti,
                    t_fields.len(),
                    "predicted struct at {path} is not a subsequence of the \
                     physical struct\n{}",
                    ctx()
                );
            }
            (DataType::List(_), DataType::List(t_item)) => {
                let full_l = full.as_any().downcast_ref::<ListArray>().unwrap();
                let clipped_l = clipped.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(
                    full_l.offsets(),
                    clipped_l.offsets(),
                    "list offsets differ at {path}\n{}",
                    ctx()
                );
                assert_same(
                    full_l.values(),
                    clipped_l.values(),
                    t_item.data_type(),
                    &format!("{path}[]"),
                    ctx,
                );
            }
            (DataType::LargeList(_), DataType::LargeList(t_item)) => {
                let full_l = full.as_any().downcast_ref::<LargeListArray>().unwrap();
                let clipped_l =
                    clipped.as_any().downcast_ref::<LargeListArray>().unwrap();
                assert_eq!(
                    full_l.offsets(),
                    clipped_l.offsets(),
                    "large list offsets differ at {path}\n{}",
                    ctx()
                );
                assert_same(
                    full_l.values(),
                    clipped_l.values(),
                    t_item.data_type(),
                    &format!("{path}[]"),
                    ctx,
                );
            }
            // A subtree the clip does not descend through: it must have been
            // kept whole, so it has to be identical, type and values.
            _ => {
                assert_eq!(
                    full.data_type(),
                    predicted,
                    "clip changed a type it does not descend through at \
                     {path}\n{}",
                    ctx()
                );
                assert_eq!(
                    full.to_data(),
                    clipped.to_data(),
                    "values differ at {path}\n{}",
                    ctx()
                );
            }
        }
    }

    /// Why a case did not clip, for the bail-out breakdown.
    #[derive(Debug, Default)]
    struct Stats {
        cases: usize,
        clipped: usize,
        bail_nothing_kept: usize,
        bail_kept_everything: usize,
        bail_unclippable: usize,
        leaf_count_mismatch: usize,
        /// Clipped cases whose physical type nests a struct under a list
        /// wrapper (the headline `List<Struct>` shape).
        clipped_under_list: usize,
        /// Clipped cases whose physical type has a struct level with
        /// duplicate field names.
        clipped_with_dup_names: usize,
        /// Clipped cases whose physical type is more than two levels deep.
        clipped_deep: usize,
        /// Total Parquet leaves the clips dropped.
        leaves_dropped: usize,
        /// Clipped cases over a file with no embedded Arrow schema, i.e. one
        /// whose Arrow type the reader derived from the Parquet schema.
        clipped_without_arrow_metadata: usize,
    }

    impl Stats {
        fn merge(&mut self, other: &Stats) {
            self.cases += other.cases;
            self.clipped += other.clipped;
            self.bail_nothing_kept += other.bail_nothing_kept;
            self.bail_kept_everything += other.bail_kept_everything;
            self.bail_unclippable += other.bail_unclippable;
            self.leaf_count_mismatch += other.leaf_count_mismatch;
            self.clipped_under_list += other.clipped_under_list;
            self.clipped_with_dup_names += other.clipped_with_dup_names;
            self.clipped_deep += other.clipped_deep;
            self.leaves_dropped += other.leaves_dropped;
            self.clipped_without_arrow_metadata += other.clipped_without_arrow_metadata;
        }
    }

    /// Does a struct live under a list wrapper anywhere in `dt`?
    fn struct_under_list(dt: &DataType) -> bool {
        match dt {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _) => contains_struct(f.data_type()),
            DataType::Struct(fields) => {
                fields.iter().any(|f| struct_under_list(f.data_type()))
            }
            _ => nested_child(dt).is_some_and(struct_under_list),
        }
    }

    /// Does any struct level in `dt` repeat a field name?
    fn has_duplicate_names(dt: &DataType) -> bool {
        match dt {
            DataType::Struct(fields) => {
                let mut names: Vec<&str> =
                    fields.iter().map(|f| f.name().as_str()).collect();
                names.sort_unstable();
                let dup = names.windows(2).any(|w| w[0] == w[1]);
                dup || fields.iter().any(|f| has_duplicate_names(f.data_type()))
            }
            _ => nested_child(dt).is_some_and(has_duplicate_names),
        }
    }

    fn type_depth(dt: &DataType) -> usize {
        match dt {
            DataType::Struct(fields) => {
                1 + fields
                    .iter()
                    .map(|f| type_depth(f.data_type()))
                    .max()
                    .unwrap_or(0)
            }
            _ => nested_child(dt).map_or(0, |c| 1 + type_depth(c)),
        }
    }

    /// Re-run the walk to classify *why* `clip_for_cast` returned `None`.
    fn classify_bail(physical: &DataType, target: &DataType, stats: &mut Stats) {
        let total = count_leaves(physical);
        let mut kept = Vec::new();
        let mut next_leaf = 0;
        let mut unclippable = false;
        clip_type(
            physical,
            target,
            &mut next_leaf,
            &mut kept,
            &mut unclippable,
        );
        if unclippable {
            stats.bail_unclippable += 1;
        } else if kept.is_empty() {
            stats.bail_nothing_kept += 1;
        } else if kept.len() >= total {
            stats.bail_kept_everything += 1;
        }
    }

    fn run_case(rng: &mut Rng, seed: u64, case: usize, stats: &mut Stats) {
        let write_field = gen_root_field(rng);
        let write_schema = Arc::new(Schema::new(vec![write_field.clone()]));
        let array = gen_array(rng, &write_field, ROWS);
        let batch = RecordBatch::try_new(Arc::clone(&write_schema), vec![array]).unwrap();
        // Half the files keep the embedded Arrow schema, half do not.
        let embed_schema = rng.chance(50);
        let data = write_file(Arc::clone(&write_schema), &batch, embed_schema);

        // The type the *reader* reports is what DataFusion uses as the file
        // schema, so that is the "physical" input to `clip_for_cast`.
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
        let physical = builder.schema().field(0).data_type().clone();
        let num_parquet_leaves = builder.parquet_schema().num_columns();
        drop(builder);

        if count_leaves(&physical) != num_parquet_leaves {
            // `build_read_plan_with_cast_clipping` refuses to clip in this
            // case; record it and move on.
            stats.leaf_count_mismatch += 1;
            return;
        }

        let target = gen_target(rng, &physical);
        stats.cases += 1;

        let ctx = || {
            format!(
                "seed={seed} case={case}\n  physical = {physical:#?}\n  target   = {target:#?}"
            )
        };

        let Some((kept, predicted)) = clip_for_cast(&physical, &target) else {
            classify_bail(&physical, &target, stats);
            return;
        };
        stats.clipped += 1;
        stats.leaves_dropped += num_parquet_leaves - kept.len();
        if !embed_schema {
            stats.clipped_without_arrow_metadata += 1;
        }
        if struct_under_list(&physical) {
            stats.clipped_under_list += 1;
        }
        if has_duplicate_names(&physical) {
            stats.clipped_with_dup_names += 1;
        }
        if type_depth(&physical) > 2 {
            stats.clipped_deep += 1;
        }

        assert!(
            !kept.is_empty() && kept.len() < num_parquet_leaves,
            "clip must keep a strict, non-empty subset: {kept:?}\n{}",
            ctx()
        );
        assert!(
            kept.windows(2).all(|w| w[0] < w[1]),
            "kept leaves must be sorted and unique: {kept:?}\n{}",
            ctx()
        );

        let (full_type, full_array) = read_column(&data, None);
        assert_eq!(full_type, physical, "unclipped read type\n{}", ctx());

        let (clipped_type, clipped_array) = read_column(&data, Some(&kept));

        // (1) the schema the decoder actually emits must be the predicted one
        assert_eq!(
            clipped_type,
            predicted,
            "predicted type does not match the type the reader emits for \
             leaves {kept:?}\n{}",
            ctx()
        );

        // (2) every surviving leaf must carry the same values, and every
        // nesting level the same nulls, as the unclipped read
        assert_same(&full_array, &clipped_array, &predicted, "col", &ctx);
    }

    fn run_seed(seed: u64, cases: usize) -> Stats {
        let mut rng = Rng::new(seed);
        let mut stats = Stats::default();
        for case in 0..cases {
            run_case(&mut rng, seed, case, &mut stats);
        }
        stats
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// The harness itself. Runs many independent seeds; every seed is a
    /// reproducible stream, and any failure prints the seed plus the exact
    /// physical/target pair that broke.
    #[test]
    fn clip_for_cast_differential() {
        let seed0 = env_usize("NESTED_PRUNING_FUZZ_SEED0", 1) as u64;
        let seeds = env_usize("NESTED_PRUNING_FUZZ_SEEDS", 24);
        let cases = env_usize("NESTED_PRUNING_FUZZ_CASES", 60);

        let mut total = Stats::default();
        for i in 0..seeds {
            total.merge(&run_seed(seed0 + i as u64, cases));
        }

        println!("nested pruning fuzz: {total:?}");
        assert!(total.cases > 0);
        // A harness that always bails proves nothing: require that a healthy
        // share of the generated cases actually took the clip path.
        let clipped_ratio = total.clipped as f64 / total.cases as f64;
        assert!(
            clipped_ratio > 0.25,
            "only {:.1}% of {} cases exercised the clip path: {total:?}",
            clipped_ratio * 100.0,
            total.cases
        );
    }
}
