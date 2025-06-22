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

//! Physical expression schema rewriting utilities

use std::sync::Arc;
use std::cmp::Ordering;

use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DataType, FieldRef, Schema, TimeUnit, MAX_DECIMAL128_FOR_EACH_PRECISION,
    MIN_DECIMAL128_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::{
    exec_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_expr::Operator;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expressions::{self, BinaryExpr, CastExpr, Column, Literal};

/// Builder for rewriting physical expressions to match different schemas.
///
/// # Example
///
/// ```rust
/// use datafusion_physical_expr::schema_rewriter::PhysicalExprSchemaRewriter;
/// use arrow::datatypes::Schema;
///
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let rewriter = PhysicalExprSchemaRewriter::new(physical_file_schema, logical_file_schema);
/// let adapted_predicate = rewriter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
pub struct PhysicalExprSchemaRewriter<'a> {
    physical_file_schema: &'a Schema,
    logical_file_schema: &'a Schema,
    partition_fields: Vec<FieldRef>,
    partition_values: Vec<ScalarValue>,
}

impl<'a> PhysicalExprSchemaRewriter<'a> {
    /// Create a new schema rewriter with the given schemas
    pub fn new(
        physical_file_schema: &'a Schema,
        logical_file_schema: &'a Schema,
    ) -> Self {
        Self {
            physical_file_schema,
            logical_file_schema,
            partition_fields: Vec::new(),
            partition_values: Vec::new(),
        }
    }

    /// Add partition columns and their corresponding values
    ///
    /// When a column reference matches a partition field, it will be replaced
    /// with the corresponding literal value from partition_values.
    pub fn with_partition_columns(
        mut self,
        partition_fields: Vec<FieldRef>,
        partition_values: Vec<ScalarValue>,
    ) -> Self {
        self.partition_fields = partition_fields;
        self.partition_values = partition_values;
        self
    }

    /// Rewrite the given physical expression to match the target schema
    ///
    /// This method applies the following transformations:
    /// 1. Replaces partition column references with literal values
    /// 2. Handles missing columns by inserting null literals
    /// 3. Casts columns when logical and physical schemas have different types
    /// 4. Optimizes cast expressions in binary comparisons by unwrapping casts
    pub fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| self.rewrite_expr(expr)).data()
    }

    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check for binary expressions that can be optimized by unwrapping casts FIRST
        // before we rewrite the children, since child rewriting might add casts
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            if let Some(optimized) = self.try_unwrap_cast_in_comparison(binary_expr)? {
                // Don't recursively transform the optimized expression here since it might
                // cause double-casting. Instead just return it and let the parent transform handle it.
                return Ok(Transformed::yes(optimized));
            }
        }

        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        Ok(Transformed::no(expr))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Get the logical field for this column
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                // If the column is a partition field, we can use the partition value
                if let Some(partition_value) = self.get_partition_value(column.name()) {
                    return Ok(Transformed::yes(expressions::lit(partition_value)));
                }
                // If the column is not found in the logical schema and is not a partition value, return an error
                // This should probably never be hit unless something upstream broke, but nontheless it's better
                // for us to return a handleable error than to panic / do something unexpected.
                return Err(e.into());
            }
        };

        // Check if the column exists in the physical schema
        let physical_column_index =
            match self.physical_file_schema.index_of(column.name()) {
                Ok(index) => index,
                Err(_) => {
                    if !logical_field.is_nullable() {
                        return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                    }
                    // If the column is missing from the physical schema fill it in with nulls as `SchemaAdapter` would do.
                    // TODO: do we need to sync this with what the `SchemaAdapter` actually does?
                    // While the default implementation fills in nulls in theory a custom `SchemaAdapter` could do something else!
                    let null_value =
                        ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(expressions::lit(null_value)));
                }
            };
        let physical_field = self.physical_file_schema.field(physical_column_index);

        let column = match (
            column.index() == physical_column_index,
            logical_field.data_type() == physical_field.data_type(),
        ) {
            // If the column index matches and the data types match, we can use the column as is
            (true, true) => return Ok(Transformed::no(expr)),
            // If the indexes or data types do not match, we need to create a new column expression
            (true, _) => column.clone(),
            (false, _) => {
                Column::new_with_schema(logical_field.name(), self.physical_file_schema)?
            }
        };

        if logical_field.data_type() == physical_field.data_type() {
            // If the data types match, we can use the column as is
            return Ok(Transformed::yes(Arc::new(column)));
        }

        // We need to cast the column to the logical data type
        // Note: Binary expressions with casts are optimized separately in try_unwrap_cast_in_comparison
        // to move the cast from the column to literal expressions when possible (e.g., col = 123)
        // since that's much cheaper to evaluate.
        if !can_cast_types(physical_field.data_type(), logical_field.data_type()) {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr = Arc::new(CastExpr::new(
            Arc::new(column),
            logical_field.data_type().clone(),
            None,
        ));

        Ok(Transformed::yes(cast_expr))
    }

    fn get_partition_value(&self, column_name: &str) -> Option<ScalarValue> {
        self.partition_fields
            .iter()
            .zip(self.partition_values.iter())
            .find(|(field, _)| field.name() == column_name)
            .map(|(_, value)| value.clone())
    }

    /// Attempt to optimize cast expressions in binary comparisons by unwrapping the cast
    /// and applying it to the literal instead.
    ///
    /// For example: `cast(column as INT64) = 123i64` becomes `column = 123i32`
    /// This is much more efficient as the cast is applied once to the literal rather
    /// than to every row value.
    fn try_unwrap_cast_in_comparison(
        &self,
        binary_expr: &BinaryExpr,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let op = binary_expr.op();
        let left = binary_expr.left();
        let right = binary_expr.right();

        // Check if left side is a cast and right side is a literal
        if let (Some(cast_expr), Some(literal)) = (
            left.as_any().downcast_ref::<CastExpr>(),
            right.as_any().downcast_ref::<Literal>(),
        ) {
            if let Some(optimized) = self.unwrap_cast_with_literal(cast_expr, literal, *op)? {
                return Ok(Some(Arc::new(BinaryExpr::new(
                    optimized.0,
                    *op,
                    optimized.1,
                ))));
            }
        }

        // Check if right side is a cast and left side is a literal
        if let (Some(literal), Some(cast_expr)) = (
            left.as_any().downcast_ref::<Literal>(),
            right.as_any().downcast_ref::<CastExpr>(),
        ) {
            if let Some(optimized) = self.unwrap_cast_with_literal(cast_expr, literal, *op)? {
                return Ok(Some(Arc::new(BinaryExpr::new(
                    optimized.1,
                    *op,
                    optimized.0,
                ))));
            }
        }

        Ok(None)
    }

    /// Unwrap a cast expression when used with a literal in a comparison
    fn unwrap_cast_with_literal(
        &self,
        cast_expr: &CastExpr,
        literal: &Literal,
        op: Operator,
    ) -> Result<Option<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> {
        // Get the inner expression (what's being cast)
        let inner_expr = cast_expr.expr();
        
        // Handle the case where inner expression might be another cast (due to schema rewriting)
        // This can happen when the schema rewriter adds a cast to a column, and then we have
        // an original cast on top of that.
        let (final_inner_expr, column) = if let Some(inner_cast) = inner_expr.as_any().downcast_ref::<CastExpr>() {
            // We have a nested cast, check if the inner cast's expression is a column
            let inner_inner_expr = inner_cast.expr();
            if let Some(col) = inner_inner_expr.as_any().downcast_ref::<Column>() {
                (inner_inner_expr, col)
            } else {
                return Ok(None);
            }
        } else if let Some(col) = inner_expr.as_any().downcast_ref::<Column>() {
            (inner_expr, col)
        } else {
            return Ok(None);
        };

        // Get the column's data type from the physical schema
        let column_data_type = match self.physical_file_schema.field_with_name(column.name()) {
            Ok(field) => field.data_type(),
            Err(_) => return Ok(None), // Column not found, can't optimize
        };

        // Try to cast the literal to the column's data type
        if let Some(casted_literal) = try_cast_literal_to_type(literal.value(), column_data_type, op) {
            return Ok(Some((
                Arc::clone(final_inner_expr),
                expressions::lit(casted_literal),
            )));
        }

        Ok(None)
    }
}

/// Try to cast a literal value to a target type, considering the comparison operator
/// This is adapted from the logical layer unwrap_cast functionality
fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
    op: Operator,
) -> Option<ScalarValue> {
    // First try operator-specific casting (e.g., string to int for equality)
    if let Some(result) = cast_literal_to_type_with_op(lit_value, target_type, op) {
        return Some(result);
    }

    // Fall back to general casting
    try_cast_literal_to_type_general(lit_value, target_type)
}

/// Cast literal with operator-specific logic
fn cast_literal_to_type_with_op(
    lit_value: &ScalarValue,
    target_type: &DataType,
    op: Operator,
) -> Option<ScalarValue> {
    
    match (op, lit_value) {
        (
            Operator::Eq | Operator::NotEq,
            ScalarValue::Utf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
            | ScalarValue::LargeUtf8(Some(_)),
        ) => {
            // Only try for integer types
            use DataType::*;
            if matches!(
                target_type,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
            ) {
                let casted = lit_value.cast_to(target_type).ok()?;
                let round_tripped = casted.cast_to(&lit_value.data_type()).ok()?;
                if lit_value != &round_tripped {
                    return None;
                }
                Some(casted)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// General literal casting logic adapted from the logical layer
fn try_cast_literal_to_type_general(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_type(&lit_data_type) || !is_supported_type(target_type) {
        return None;
    }
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return ScalarValue::try_from(target_type).ok();
    }
    try_cast_numeric_literal(lit_value, target_type)
        .or_else(|| try_cast_string_literal(lit_value, target_type))
        .or_else(|| try_cast_dictionary(lit_value, target_type))
        .or_else(|| try_cast_binary(lit_value, target_type))
}

/// Returns true if unwrap_cast_in_comparison supports this data type
fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
        || is_supported_binary_type(data_type)
}

/// Returns true if unwrap_cast_in_comparison support this numeric type
fn is_supported_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a string
fn is_supported_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a dictionary
fn is_supported_dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type,
                    DataType::Dictionary(_, inner) if is_supported_type(inner))
}

fn is_supported_binary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::FixedSizeBinary(_))
}

/// Convert a numeric value from one numeric data type to another
fn try_cast_numeric_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_numeric_type(&lit_data_type)
        || !is_supported_numeric_type(target_type)
    {
        return None;
    }

    let mul = match target_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => 1_i128,
        DataType::Timestamp(_, _) => 1_i128,
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        _ => return None,
    };
    let (target_min, target_max) = match target_type {
        DataType::UInt8 => (u8::MIN as i128, u8::MAX as i128),
        DataType::UInt16 => (u16::MIN as i128, u16::MAX as i128),
        DataType::UInt32 => (u32::MIN as i128, u32::MAX as i128),
        DataType::UInt64 => (u64::MIN as i128, u64::MAX as i128),
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Timestamp(_, _) => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal128(precision, _) => (
            MIN_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
            MAX_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
        ),
        _ => return None,
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampSecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                Some(*v / (lit_scale_mul / mul))
            } else {
                None
            }
        }
        _ => None,
    };

    match lit_value_target_type {
        None => None,
        Some(value) => {
            if value >= target_min && value <= target_max {
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampSecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMillisecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMicrosecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampNanosecond(value, tz.clone())
                    }
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    _ => {
                        return None;
                    }
                };
                Some(result_scalar)
            } else {
                None
            }
        }
    }
}

fn try_cast_string_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let string_value = lit_value.try_as_str()?.map(|s| s.to_string());
    let scalar_value = match target_type {
        DataType::Utf8 => ScalarValue::Utf8(string_value),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(string_value),
        DataType::Utf8View => ScalarValue::Utf8View(string_value),
        _ => return None,
    };
    Some(scalar_value)
}

/// Attempt to cast to/from a dictionary type by wrapping/unwrapping the dictionary
fn try_cast_dictionary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_value_type = lit_value.data_type();
    let result_scalar = match (lit_value, target_type) {
        // Unwrap dictionary when inner type matches target type
        (ScalarValue::Dictionary(_, inner_value), _)
            if inner_value.data_type() == *target_type =>
        {
            (**inner_value).clone()
        }
        // Wrap type when target type is dictionary
        (_, DataType::Dictionary(index_type, inner_type))
            if **inner_type == lit_value_type =>
        {
            ScalarValue::Dictionary(index_type.clone(), Box::new(lit_value.clone()))
        }
        _ => {
            return None;
        }
    };
    Some(result_scalar)
}

/// Cast a timestamp value from one unit to another
fn cast_between_timestamp(from: &DataType, to: &DataType, value: i128) -> Option<i64> {
    let value = value as i64;
    let from_scale = match from {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    let to_scale = match to {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    match from_scale.cmp(&to_scale) {
        Ordering::Less => value.checked_mul(to_scale / from_scale),
        Ordering::Greater => Some(value / (from_scale / to_scale)),
        Ordering::Equal => Some(value),
    }
}

fn try_cast_binary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    match (lit_value, target_type) {
        (ScalarValue::Binary(Some(v)), DataType::FixedSizeBinary(n))
            if v.len() == *n as usize =>
        {
            Some(ScalarValue::FixedSizeBinary(*n, Some(v.clone())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use std::sync::Arc;

    fn create_test_schema() -> (Schema, Schema) {
        let physical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Different type
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true), // Missing from physical
        ]);

        (physical_schema, logical_schema)
    }

    #[test]
    fn test_rewrite_column_with_type_cast() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("a", 0));

        let result = rewriter.rewrite(column_expr)?;

        // Should be wrapped in a cast expression
        assert!(result.as_any().downcast_ref::<CastExpr>().is_some());

        Ok(())
    }

    #[test]
    fn test_rewrite_missing_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("c", 2));

        let result = rewriter.rewrite(column_expr)?;

        // Should be replaced with a literal null
        if let Some(literal) = result.as_any().downcast_ref::<Literal>() {
            assert_eq!(*literal.value(), ScalarValue::Float64(None));
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_partition_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let partition_fields =
            vec![Arc::new(Field::new("partition_col", DataType::Utf8, false))];
        let partition_values = vec![ScalarValue::Utf8(Some("test_value".to_string()))];

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema)
            .with_partition_columns(partition_fields, partition_values);

        let column_expr = Arc::new(Column::new("partition_col", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be replaced with the partition value
        if let Some(literal) = result.as_any().downcast_ref::<Literal>() {
            assert_eq!(
                *literal.value(),
                ScalarValue::Utf8(Some("test_value".to_string()))
            );
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_no_change_needed() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let result = rewriter.rewrite(Arc::clone(&column_expr))?;

        // Should be the same expression (no transformation needed)
        // We compare the underlying pointer through the trait object
        assert!(std::ptr::eq(
            column_expr.as_ref() as *const dyn PhysicalExpr,
            result.as_ref() as *const dyn PhysicalExpr
        ));

        Ok(())
    }

    #[test]
    fn test_non_nullable_missing_column_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false), // Non-nullable missing column
        ]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1));

        let result = rewriter.rewrite(column_expr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Non-nullable column 'b' is missing"));
    }

    #[test]
    fn test_unwrap_cast_optimization() -> Result<()> {
        // Test case: cast(int32_column as int64) = 123i64 should become int32_column = 123i32
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create: cast(column("a") as Int64) = 123i64
        let column_expr = Arc::new(Column::new("a", 0));
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = expressions::lit(ScalarValue::Int64(Some(123)));
        let binary_expr = Arc::new(BinaryExpr::new(
            cast_expr,
            Operator::Eq,
            literal_expr,
        ));

        let result = rewriter.rewrite(binary_expr.clone() as Arc<dyn PhysicalExpr>)?;

        // The result should be a binary expression with the cast unwrapped
        let result_binary = result.as_any().downcast_ref::<BinaryExpr>().unwrap();
        
        // Left side should be the original column (no cast)
        assert!(result_binary.left().as_any().downcast_ref::<Column>().is_some());
        
        // Right side should be a literal with the value cast to Int32
        let right_literal = result_binary.right().as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(*right_literal.value(), ScalarValue::Int32(Some(123)));

        Ok(())
    }

    #[test]
    fn test_unwrap_cast_optimization_reverse_order() -> Result<()> {
        // Test case: 123i64 = cast(int32_column as int64) should become 123i32 = int32_column
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create: 123i64 = cast(column("a") as Int64)
        let literal_expr = expressions::lit(ScalarValue::Int64(Some(123)));
        let column_expr = Arc::new(Column::new("a", 0));
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let binary_expr = Arc::new(BinaryExpr::new(
            literal_expr,
            Operator::Eq,
            cast_expr,
        ));

        let result = rewriter.rewrite(binary_expr)?;

        // The result should be a binary expression with the cast unwrapped
        let result_binary = result.as_any().downcast_ref::<BinaryExpr>().unwrap();
        
        // Left side should be a literal with the value cast to Int32
        let left_literal = result_binary.left().as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(*left_literal.value(), ScalarValue::Int32(Some(123)));
        
        // Right side should be the original column (no cast)
        assert!(result_binary.right().as_any().downcast_ref::<Column>().is_some());

        Ok(())
    }

    #[test]
    fn test_unwrap_cast_optimization_string_to_int() -> Result<()> {
        // Test case: cast(int32_column as Utf8) = "123" should become int32_column = 123i32
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create: cast(column("a") as Utf8) = "123"
        let column_expr = Arc::new(Column::new("a", 0));
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Utf8, None));
        let literal_expr = expressions::lit(ScalarValue::Utf8(Some("123".to_string())));
        let binary_expr = Arc::new(BinaryExpr::new(
            cast_expr,
            Operator::Eq,
            literal_expr,
        ));

        let result = rewriter.rewrite(binary_expr)?;

        // The result should be a binary expression with the cast unwrapped
        let result_binary = result.as_any().downcast_ref::<BinaryExpr>().unwrap();
        
        // Left side should be the original column (no cast)
        assert!(result_binary.left().as_any().downcast_ref::<Column>().is_some());
        
        // Right side should be a literal with the value cast to Int32
        let right_literal = result_binary.right().as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(*right_literal.value(), ScalarValue::Int32(Some(123)));

        Ok(())
    }

    #[test]
    fn test_no_unwrap_cast_optimization_when_not_applicable() -> Result<()> {
        // Test case where optimization should not apply - unsupported cast
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let logical_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create: cast(column("a") as Int64) = 123i64
        // Float32 to Int64 casting might not be optimizable due to precision
        let column_expr = Arc::new(Column::new("a", 0));
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = expressions::lit(ScalarValue::Int64(Some(123)));
        let binary_expr = Arc::new(BinaryExpr::new(
            cast_expr,
            Operator::Eq,
            literal_expr,
        ));

        let result = rewriter.rewrite(binary_expr)?;

        // The result should still be a binary expression with a cast on the left side
        // since Float32 is not in our supported types for unwrap cast optimization
        let result_binary = result.as_any().downcast_ref::<BinaryExpr>().unwrap();
        assert!(result_binary.left().as_any().downcast_ref::<CastExpr>().is_some());

        Ok(())
    }
}
