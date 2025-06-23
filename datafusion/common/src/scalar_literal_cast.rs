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

//! Utilities for casting scalar literals to different data types
//!
//! This module contains functions for casting ScalarValue literals
//! to different data types, originally extracted from the optimizer's
//! unwrap_cast module to be shared between logical and physical layers.

use std::cmp::Ordering;

use arrow::datatypes::{
    DataType, TimeUnit, MAX_DECIMAL128_FOR_EACH_PRECISION,
    MIN_DECIMAL128_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};

use crate::ScalarValue;

/// Convert a literal value from one data type to another
pub fn try_cast_literal_to_type(
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
pub fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
        || is_supported_binary_type(data_type)
}

/// Returns true if unwrap_cast_in_comparison support this numeric type
pub fn is_supported_numeric_type(data_type: &DataType) -> bool {
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
pub fn is_supported_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a dictionary
pub fn is_supported_dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type,
                    DataType::Dictionary(_, inner) if is_supported_type(inner))
}

pub fn is_supported_binary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::FixedSizeBinary(_))
}

/// Convert a numeric value from one numeric data type to another
pub fn try_cast_numeric_literal(
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
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
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
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(*v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        _ => None,
    };

    match lit_value_target_type {
        None => None,
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
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

pub fn try_cast_string_literal(
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
pub fn try_cast_dictionary(
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
pub fn cast_between_timestamp(from: &DataType, to: &DataType, value: i128) -> Option<i64> {
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

pub fn try_cast_binary(
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