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

//! [`ScalarValue`]: stores single  values

mod cache;
mod consts;
mod struct_builder;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter::repeat_n;
use std::mem::{size_of, size_of_val};
use std::str::FromStr;
use std::sync::Arc;

use crate::cast::{
    as_binary_array, as_binary_view_array, as_boolean_array, as_date32_array,
    as_date64_array, as_decimal128_array, as_decimal256_array, as_dictionary_array,
    as_duration_microsecond_array, as_duration_millisecond_array,
    as_duration_nanosecond_array, as_duration_second_array, as_fixed_size_binary_array,
    as_fixed_size_list_array, as_float16_array, as_float32_array, as_float64_array,
    as_int16_array, as_int32_array, as_int64_array, as_int8_array, as_interval_dt_array,
    as_interval_mdn_array, as_interval_ym_array, as_large_binary_array,
    as_large_list_array, as_large_string_array, as_string_array, as_string_view_array,
    as_time32_millisecond_array, as_time32_second_array, as_time64_microsecond_array,
    as_time64_nanosecond_array, as_timestamp_microsecond_array,
    as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
    as_timestamp_second_array, as_uint16_array, as_uint32_array, as_uint64_array,
    as_uint8_array, as_union_array,
};
use crate::error::{DataFusionError, Result, _exec_err, _internal_err, _not_impl_err};
use crate::format::DEFAULT_CAST_OPTIONS;
use crate::hash_utils::create_hashes;
use crate::utils::SingleRowListArrayBuilder;
use crate::{_internal_datafusion_err, arrow_datafusion_err};
use arrow::array::{
    new_empty_array, new_null_array, Array, ArrayData, ArrayRef, ArrowNativeTypeOp,
    ArrowPrimitiveType, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Date64Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array,
    Float32Array, Float64Array, GenericListArray, Int16Array, Int32Array, Int64Array,
    Int8Array, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    MutableArrayData, PrimitiveArray, Scalar, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array, UnionArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::compute::kernels::cast::{cast_with_options, CastOptions};
use arrow::compute::kernels::numeric::{
    add, add_wrapping, div, mul, mul_wrapping, rem, sub, sub_wrapping,
};
use arrow::datatypes::{
    i256, validate_decimal_precision_and_scale, ArrowDictionaryKeyType, ArrowNativeType,
    ArrowTimestampType, DataType, Date32Type, Decimal128Type, Decimal256Type, Field,
    Float32Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTime,
    IntervalDayTimeType, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type, UnionFields, UnionMode, DECIMAL128_MAX_PRECISION,
};
use arrow::util::display::{array_value_to_string, ArrayFormatter, FormatOptions};
use cache::{get_or_create_cached_key_array, get_or_create_cached_null_array};
use chrono::{Duration, NaiveDate};
use half::f16;
pub use struct_builder::ScalarStructBuilder;

/// A dynamically typed, nullable single value.
///
/// While an arrow  [`Array`]) stores one or more values of the same type, in a
/// single column, a `ScalarValue` stores a single value of a single type, the
/// equivalent of 1 row and one column.
///
/// ```text
///  ┌────────┐
///  │ value1 │
///  │ value2 │                  ┌────────┐
///  │ value3 │                  │ value2 │
///  │  ...   │                  └────────┘
///  │ valueN │
///  └────────┘
///
///    Array                     ScalarValue
///
/// stores multiple,             stores a single,
/// possibly null, values of     possible null, value
/// the same type
/// ```
///
/// # Performance
///
/// In general, performance will be better using arrow [`Array`]s rather than
/// [`ScalarValue`], as it is far more efficient to process multiple values at
/// once (vectorized processing).
///
/// # Example
/// ```
/// # use datafusion_common::ScalarValue;
/// // Create single scalar value for an Int32 value
/// let s1 = ScalarValue::Int32(Some(10));
///
/// // You can also create values using the From impl:
/// let s2 = ScalarValue::from(10i32);
/// assert_eq!(s1, s2);
/// ```
///
/// # Null Handling
///
/// `ScalarValue` represents null values in the same way as Arrow. Nulls are
/// "typed" in the sense that a null value in an [`Int32Array`] is different
/// from a null value in a [`Float64Array`], and is different from the values in
/// a [`NullArray`].
///
/// ```
/// # fn main() -> datafusion_common::Result<()> {
/// # use std::collections::hash_set::Difference;
/// # use datafusion_common::ScalarValue;
/// # use arrow::datatypes::DataType;
/// // You can create a 'null' Int32 value directly:
/// let s1 = ScalarValue::Int32(None);
///
/// // You can also create a null value for a given datatype:
/// let s2 = ScalarValue::try_from(&DataType::Int32)?;
/// assert_eq!(s1, s2);
///
/// // Note that this is DIFFERENT than a `ScalarValue::Null`
/// let s3 = ScalarValue::Null;
/// assert_ne!(s1, s3);
/// # Ok(())
/// # }
/// ```
///
/// # Nested Types
///
/// `List` / `LargeList` / `FixedSizeList` / `Struct` / `Map` are represented as a
/// single element array of the corresponding type.
///
/// ## Example: Creating [`ScalarValue::Struct`] using [`ScalarStructBuilder`]
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field};
/// # use datafusion_common::{ScalarValue, scalar::ScalarStructBuilder};
/// // Build a struct like: {a: 1, b: "foo"}
/// let field_a = Field::new("a", DataType::Int32, false);
/// let field_b = Field::new("b", DataType::Utf8, false);
///
/// let s1 = ScalarStructBuilder::new()
///    .with_scalar(field_a, ScalarValue::from(1i32))
///    .with_scalar(field_b, ScalarValue::from("foo"))
///    .build();
/// ```
///
/// ## Example: Creating a null [`ScalarValue::Struct`] using [`ScalarStructBuilder`]
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field};
/// # use datafusion_common::{ScalarValue, scalar::ScalarStructBuilder};
/// // Build a struct representing a NULL value
/// let fields = vec![
///     Field::new("a", DataType::Int32, false),
///     Field::new("b", DataType::Utf8, false),
/// ];
///
/// let s1 = ScalarStructBuilder::new_null(fields);
/// ```
///
/// ## Example: Creating [`ScalarValue::Struct`] directly
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Fields};
/// # use arrow::array::{ArrayRef, Int32Array, StructArray, StringArray};
/// # use datafusion_common::ScalarValue;
/// // Build a struct like: {a: 1, b: "foo"}
/// // Field description
/// let fields = Fields::from(vec![
///   Field::new("a", DataType::Int32, false),
///   Field::new("b", DataType::Utf8, false),
/// ]);
/// // one row arrays for each field
/// let arrays: Vec<ArrayRef> = vec![
///   Arc::new(Int32Array::from(vec![1])),
///   Arc::new(StringArray::from(vec!["foo"])),
/// ];
/// // no nulls for this array
/// let nulls = None;
/// let arr = StructArray::new(fields, arrays, nulls);
///
/// // Create a ScalarValue::Struct directly
/// let s1 = ScalarValue::Struct(Arc::new(arr));
/// ```
///
///
/// # Further Reading
/// See [datatypes](https://arrow.apache.org/docs/python/api/datatypes.html) for
/// details on datatypes and the [format](https://github.com/apache/arrow/blob/master/format/Schema.fbs#L354-L375)
/// for the definitive reference.
///
/// [`NullArray`]: arrow::array::NullArray
#[derive(Clone)]
pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(Option<bool>),
    /// 16bit float
    Float16(Option<f16>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// 128bit decimal, using the i128 to represent the decimal, precision scale
    Decimal128(Option<i128>, u8, i8),
    /// 256bit decimal, using the i256 to represent the decimal, precision scale
    Decimal256(Option<i256>, u8, i8),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// utf-8 encoded string but from view types.
    Utf8View(Option<String>),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// binary
    Binary(Option<Vec<u8>>),
    /// binary but from view types.
    BinaryView(Option<Vec<u8>>),
    /// fixed size binary
    FixedSizeBinary(i32, Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// Fixed size list scalar.
    ///
    /// The array must be a FixedSizeListArray with length 1.
    FixedSizeList(Arc<FixedSizeListArray>),
    /// Represents a single element of a [`ListArray`] as an [`ArrayRef`]
    ///
    /// The array must be a ListArray with length 1.
    List(Arc<ListArray>),
    /// The array must be a LargeListArray with length 1.
    LargeList(Arc<LargeListArray>),
    /// Represents a single element [`StructArray`] as an [`ArrayRef`]. See
    /// [`ScalarValue`] for examples of how to create instances of this type.
    Struct(Arc<StructArray>),
    /// Represents a single element [`MapArray`] as an [`ArrayRef`].
    Map(Arc<MapArray>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Time stored as a signed 32bit int as seconds since midnight
    Time32Second(Option<i32>),
    /// Time stored as a signed 32bit int as milliseconds since midnight
    Time32Millisecond(Option<i32>),
    /// Time stored as a signed 64bit int as microseconds since midnight
    Time64Microsecond(Option<i64>),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64Nanosecond(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<IntervalDayTime>),
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// Months and days are encoded as 32-bit signed integers.
    /// Nanoseconds is encoded as a 64-bit signed integer (no leap seconds).
    IntervalMonthDayNano(Option<IntervalMonthDayNano>),
    /// Duration in seconds
    DurationSecond(Option<i64>),
    /// Duration in milliseconds
    DurationMillisecond(Option<i64>),
    /// Duration in microseconds
    DurationMicrosecond(Option<i64>),
    /// Duration in nanoseconds
    DurationNanosecond(Option<i64>),
    /// A nested datatype that can represent slots of differing types. Components:
    /// `.0`: a tuple of union `type_id` and the single value held by this Scalar
    /// `.1`: the list of fields, zero-to-one of which will by set in `.0`
    /// `.2`: the physical storage of the source/destination UnionArray from which this Scalar came
    Union(Option<(i8, Box<ScalarValue>)>, UnionFields, UnionMode),
    /// Dictionary type: index type and value
    Dictionary(Box<DataType>, Box<ScalarValue>),
}

impl Hash for Fl<f16> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

// manual implementation of `PartialEq`
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                v1.eq(v2) && p1.eq(p2) && s1.eq(s2)
            }
            (Decimal128(_, _, _), _) => false,
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                v1.eq(v2) && p1.eq(p2) && s1.eq(s2)
            }
            (Decimal256(_, _, _), _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float16(v1), Float16(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float32(_), _) => false,
            (Float16(_), _) => false,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1.eq(v2),
            (Utf8(_), _) => false,
            (Utf8View(v1), Utf8View(v2)) => v1.eq(v2),
            (Utf8View(_), _) => false,
            (LargeUtf8(v1), LargeUtf8(v2)) => v1.eq(v2),
            (LargeUtf8(_), _) => false,
            (Binary(v1), Binary(v2)) => v1.eq(v2),
            (Binary(_), _) => false,
            (BinaryView(v1), BinaryView(v2)) => v1.eq(v2),
            (BinaryView(_), _) => false,
            (FixedSizeBinary(_, v1), FixedSizeBinary(_, v2)) => v1.eq(v2),
            (FixedSizeBinary(_, _), _) => false,
            (LargeBinary(v1), LargeBinary(v2)) => v1.eq(v2),
            (LargeBinary(_), _) => false,
            (FixedSizeList(v1), FixedSizeList(v2)) => v1.eq(v2),
            (FixedSizeList(_), _) => false,
            (List(v1), List(v2)) => v1.eq(v2),
            (List(_), _) => false,
            (LargeList(v1), LargeList(v2)) => v1.eq(v2),
            (LargeList(_), _) => false,
            (Struct(v1), Struct(v2)) => v1.eq(v2),
            (Struct(_), _) => false,
            (Map(v1), Map(v2)) => v1.eq(v2),
            (Map(_), _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
            (Time32Second(v1), Time32Second(v2)) => v1.eq(v2),
            (Time32Second(_), _) => false,
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1.eq(v2),
            (Time32Millisecond(_), _) => false,
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1.eq(v2),
            (Time64Microsecond(_), _) => false,
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1.eq(v2),
            (Time64Nanosecond(_), _) => false,
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.eq(v2),
            (TimestampSecond(_, _), _) => false,
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => v1.eq(v2),
            (TimestampMillisecond(_, _), _) => false,
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => v1.eq(v2),
            (TimestampMicrosecond(_, _), _) => false,
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => v1.eq(v2),
            (TimestampNanosecond(_, _), _) => false,
            (DurationSecond(v1), DurationSecond(v2)) => v1.eq(v2),
            (DurationSecond(_), _) => false,
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1.eq(v2),
            (DurationMillisecond(_), _) => false,
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1.eq(v2),
            (DurationMicrosecond(_), _) => false,
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1.eq(v2),
            (DurationNanosecond(_), _) => false,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.eq(v2),
            (IntervalYearMonth(_), _) => false,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.eq(v2),
            (IntervalDayTime(_), _) => false,
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1.eq(v2),
            (IntervalMonthDayNano(_), _) => false,
            (Union(val1, fields1, mode1), Union(val2, fields2, mode2)) => {
                val1.eq(val2) && fields1.eq(fields2) && mode1.eq(mode2)
            }
            (Union(_, _, _), _) => false,
            (Dictionary(k1, v1), Dictionary(k2, v2)) => k1.eq(k2) && v1.eq(v2),
            (Dictionary(_, _), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
        }
    }
}

// manual implementation of `PartialOrd`
impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScalarValue::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.partial_cmp(v2)
                } else {
                    // Two decimal values can be compared if they have the same precision and scale.
                    None
                }
            }
            (Decimal128(_, _, _), _) => None,
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.partial_cmp(v2)
                } else {
                    // Two decimal values can be compared if they have the same precision and scale.
                    None
                }
            }
            (Decimal256(_, _, _), _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float16(v1), Float16(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float32(_), _) => None,
            (Float16(_), _) => None,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
            (Utf8(_), _) => None,
            (LargeUtf8(v1), LargeUtf8(v2)) => v1.partial_cmp(v2),
            (LargeUtf8(_), _) => None,
            (Utf8View(v1), Utf8View(v2)) => v1.partial_cmp(v2),
            (Utf8View(_), _) => None,
            (Binary(v1), Binary(v2)) => v1.partial_cmp(v2),
            (Binary(_), _) => None,
            (BinaryView(v1), BinaryView(v2)) => v1.partial_cmp(v2),
            (BinaryView(_), _) => None,
            (FixedSizeBinary(_, v1), FixedSizeBinary(_, v2)) => v1.partial_cmp(v2),
            (FixedSizeBinary(_, _), _) => None,
            (LargeBinary(v1), LargeBinary(v2)) => v1.partial_cmp(v2),
            (LargeBinary(_), _) => None,
            // ScalarValue::List / ScalarValue::FixedSizeList / ScalarValue::LargeList are ensure to have length 1
            (List(arr1), List(arr2)) => partial_cmp_list(arr1.as_ref(), arr2.as_ref()),
            (FixedSizeList(arr1), FixedSizeList(arr2)) => {
                partial_cmp_list(arr1.as_ref(), arr2.as_ref())
            }
            (LargeList(arr1), LargeList(arr2)) => {
                partial_cmp_list(arr1.as_ref(), arr2.as_ref())
            }
            (List(_), _) | (LargeList(_), _) | (FixedSizeList(_), _) => None,
            (Struct(struct_arr1), Struct(struct_arr2)) => {
                partial_cmp_struct(struct_arr1.as_ref(), struct_arr2.as_ref())
            }
            (Struct(_), _) => None,
            (Map(map_arr1), Map(map_arr2)) => partial_cmp_map(map_arr1, map_arr2),
            (Map(_), _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
            (Time32Second(v1), Time32Second(v2)) => v1.partial_cmp(v2),
            (Time32Second(_), _) => None,
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1.partial_cmp(v2),
            (Time32Millisecond(_), _) => None,
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1.partial_cmp(v2),
            (Time64Microsecond(_), _) => None,
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1.partial_cmp(v2),
            (Time64Nanosecond(_), _) => None,
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.partial_cmp(v2),
            (TimestampSecond(_, _), _) => None,
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampMillisecond(_, _), _) => None,
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampMicrosecond(_, _), _) => None,
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampNanosecond(_, _), _) => None,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.partial_cmp(v2),
            (IntervalYearMonth(_), _) => None,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.partial_cmp(v2),
            (IntervalDayTime(_), _) => None,
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1.partial_cmp(v2),
            (IntervalMonthDayNano(_), _) => None,
            (DurationSecond(v1), DurationSecond(v2)) => v1.partial_cmp(v2),
            (DurationSecond(_), _) => None,
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1.partial_cmp(v2),
            (DurationMillisecond(_), _) => None,
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1.partial_cmp(v2),
            (DurationMicrosecond(_), _) => None,
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1.partial_cmp(v2),
            (DurationNanosecond(_), _) => None,
            (Union(v1, t1, m1), Union(v2, t2, m2)) => {
                if t1.eq(t2) && m1.eq(m2) {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Union(_, _, _), _) => None,
            (Dictionary(k1, v1), Dictionary(k2, v2)) => {
                // Don't compare if the key types don't match (it is effectively a different datatype)
                if k1 == k2 {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Dictionary(_, _), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
        }
    }
}

/// List/LargeList/FixedSizeList scalars always have a single element
/// array. This function returns that array
fn first_array_for_list(arr: &dyn Array) -> ArrayRef {
    assert_eq!(arr.len(), 1);
    if let Some(arr) = arr.as_list_opt::<i32>() {
        arr.value(0)
    } else if let Some(arr) = arr.as_list_opt::<i64>() {
        arr.value(0)
    } else if let Some(arr) = arr.as_fixed_size_list_opt() {
        arr.value(0)
    } else {
        unreachable!("Since only List / LargeList / FixedSizeList are supported, this should never happen")
    }
}

/// Compares two List/LargeList/FixedSizeList scalars
fn partial_cmp_list(arr1: &dyn Array, arr2: &dyn Array) -> Option<Ordering> {
    if arr1.data_type() != arr2.data_type() {
        return None;
    }
    let arr1 = first_array_for_list(arr1);
    let arr2 = first_array_for_list(arr2);

    let min_length = arr1.len().min(arr2.len());
    let arr1_trimmed = arr1.slice(0, min_length);
    let arr2_trimmed = arr2.slice(0, min_length);

    let lt_res = arrow::compute::kernels::cmp::lt(&arr1_trimmed, &arr2_trimmed).ok()?;
    let eq_res = arrow::compute::kernels::cmp::eq(&arr1_trimmed, &arr2_trimmed).ok()?;

    for j in 0..lt_res.len() {
        // In Postgres, NULL values in lists are always considered to be greater than non-NULL values:
        //
        // $ SELECT ARRAY[NULL]::integer[] > ARRAY[1]
        // true
        //
        // These next two if statements are introduced for replicating Postgres behavior, as
        // arrow::compute does not account for this.
        if arr1_trimmed.is_null(j) && !arr2_trimmed.is_null(j) {
            return Some(Ordering::Greater);
        }
        if !arr1_trimmed.is_null(j) && arr2_trimmed.is_null(j) {
            return Some(Ordering::Less);
        }

        if lt_res.is_valid(j) && lt_res.value(j) {
            return Some(Ordering::Less);
        }
        if eq_res.is_valid(j) && !eq_res.value(j) {
            return Some(Ordering::Greater);
        }
    }

    Some(arr1.len().cmp(&arr2.len()))
}

fn flatten<'a>(array: &'a StructArray, columns: &mut Vec<&'a ArrayRef>) {
    for i in 0..array.num_columns() {
        let column = array.column(i);
        if let Some(nested_struct) = column.as_any().downcast_ref::<StructArray>() {
            // If it's a nested struct, recursively expand
            flatten(nested_struct, columns);
        } else {
            // If it's a primitive type, add directly
            columns.push(column);
        }
    }
}

pub fn partial_cmp_struct(s1: &StructArray, s2: &StructArray) -> Option<Ordering> {
    if s1.len() != s2.len() {
        return None;
    }

    if s1.data_type() != s2.data_type() {
        return None;
    }

    let mut expanded_columns1 = Vec::with_capacity(s1.num_columns());
    let mut expanded_columns2 = Vec::with_capacity(s2.num_columns());

    flatten(s1, &mut expanded_columns1);
    flatten(s2, &mut expanded_columns2);

    for col_index in 0..expanded_columns1.len() {
        let arr1 = expanded_columns1[col_index];
        let arr2 = expanded_columns2[col_index];

        let lt_res = arrow::compute::kernels::cmp::lt(arr1, arr2).ok()?;
        let eq_res = arrow::compute::kernels::cmp::eq(arr1, arr2).ok()?;

        for j in 0..lt_res.len() {
            if lt_res.is_valid(j) && lt_res.value(j) {
                return Some(Ordering::Less);
            }
            if eq_res.is_valid(j) && !eq_res.value(j) {
                return Some(Ordering::Greater);
            }
        }
    }
    Some(Ordering::Equal)
}

fn partial_cmp_map(m1: &Arc<MapArray>, m2: &Arc<MapArray>) -> Option<Ordering> {
    if m1.len() != m2.len() {
        return None;
    }

    if m1.data_type() != m2.data_type() {
        return None;
    }

    for col_index in 0..m1.len() {
        let arr1 = m1.entries().column(col_index);
        let arr2 = m2.entries().column(col_index);

        let lt_res = arrow::compute::kernels::cmp::lt(arr1, arr2).ok()?;
        let eq_res = arrow::compute::kernels::cmp::eq(arr1, arr2).ok()?;

        for j in 0..lt_res.len() {
            if lt_res.is_valid(j) && lt_res.value(j) {
                return Some(Ordering::Less);
            }
            if eq_res.is_valid(j) && !eq_res.value(j) {
                return Some(Ordering::Greater);
            }
        }
    }
    Some(Ordering::Equal)
}

impl Eq for ScalarValue {}

//Float wrapper over f32/f64. Just because we cannot build std::hash::Hash for floats directly we have to do it through type wrapper
struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl std::hash::Hash for Fl<$t> {
            #[inline]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        })+
    };
}

hash_float_value!((f64, u64), (f32, u32));

// manual implementation of `Hash`
//
// # Panics
//
// Panics if there is an error when creating hash values for rows
impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state)
            }
            Decimal256(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state)
            }
            Boolean(v) => v.hash(state),
            Float16(v) => v.map(Fl).hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) | LargeUtf8(v) | Utf8View(v) => v.hash(state),
            Binary(v) | FixedSizeBinary(_, v) | LargeBinary(v) | BinaryView(v) => {
                v.hash(state)
            }
            List(arr) => {
                hash_nested_array(arr.to_owned() as ArrayRef, state);
            }
            LargeList(arr) => {
                hash_nested_array(arr.to_owned() as ArrayRef, state);
            }
            FixedSizeList(arr) => {
                hash_nested_array(arr.to_owned() as ArrayRef, state);
            }
            Struct(arr) => {
                hash_nested_array(arr.to_owned() as ArrayRef, state);
            }
            Map(arr) => {
                hash_nested_array(arr.to_owned() as ArrayRef, state);
            }
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Time32Second(v) => v.hash(state),
            Time32Millisecond(v) => v.hash(state),
            Time64Microsecond(v) => v.hash(state),
            Time64Nanosecond(v) => v.hash(state),
            TimestampSecond(v, _) => v.hash(state),
            TimestampMillisecond(v, _) => v.hash(state),
            TimestampMicrosecond(v, _) => v.hash(state),
            TimestampNanosecond(v, _) => v.hash(state),
            DurationSecond(v) => v.hash(state),
            DurationMillisecond(v) => v.hash(state),
            DurationMicrosecond(v) => v.hash(state),
            DurationNanosecond(v) => v.hash(state),
            IntervalYearMonth(v) => v.hash(state),
            IntervalDayTime(v) => v.hash(state),
            IntervalMonthDayNano(v) => v.hash(state),
            Union(v, t, m) => {
                v.hash(state);
                t.hash(state);
                m.hash(state);
            }
            Dictionary(k, v) => {
                k.hash(state);
                v.hash(state);
            }
            // stable hash for Null value
            Null => 1.hash(state),
        }
    }
}

fn hash_nested_array<H: Hasher>(arr: ArrayRef, state: &mut H) {
    let arrays = vec![arr.to_owned()];
    let hashes_buffer = &mut vec![0; arr.len()];
    let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    let hashes = create_hashes(&arrays, &random_state, hashes_buffer).unwrap();
    // Hash back to std::hash::Hasher
    hashes.hash(state);
}

/// Return a reference to the values array and the index into it for a
/// dictionary array
///
/// # Errors
///
/// Errors if the array cannot be downcasted to DictionaryArray
#[inline]
pub fn get_dict_value<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    index: usize,
) -> Result<(&ArrayRef, Option<usize>)> {
    let dict_array = as_dictionary_array::<K>(array)?;
    Ok((dict_array.values(), dict_array.key(index)))
}

/// Create a dictionary array representing `value` repeated `size`
/// times
fn dict_from_scalar<K: ArrowDictionaryKeyType>(
    value: &ScalarValue,
    size: usize,
) -> Result<ArrayRef> {
    // values array is one element long (the value)
    let values_array = value.to_array_of_size(1)?;

    // Create a key array with `size` elements, each of 0
    // Use cache to avoid repeated allocations for the same size
    let key_array: PrimitiveArray<K> =
        get_or_create_cached_key_array::<K>(size, value.is_null());

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    Ok(Arc::new(
        DictionaryArray::<K>::try_new(key_array, values_array)?, // should always be valid by construction above
    ))
}

/// Create a dictionary array representing all the values in values
fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: ArrayRef,
) -> Result<ArrayRef> {
    // Create a key array with `size` elements of 0..array_len for all
    // non-null value elements
    let key_array: PrimitiveArray<K> = (0..values_array.len())
        .map(|index| {
            if values_array.is_valid(index) {
                let native_index = K::Native::from_usize(index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not create index of type {} from value {}",
                        K::DATA_TYPE,
                        index
                    ))
                })?;
                Ok(Some(native_index))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    let dict_array = DictionaryArray::<K>::try_new(key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

macro_rules! typed_cast_tz {
    ($array:expr, $index:expr, $array_cast:ident, $SCALAR:ident, $TZ:expr) => {{
        let array = $array_cast($array)?;
        Ok::<ScalarValue, DataFusionError>(ScalarValue::$SCALAR(
            match array.is_null($index) {
                true => None,
                false => Some(array.value($index).into()),
            },
            $TZ.clone(),
        ))
    }};
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $array_cast:ident, $SCALAR:ident) => {{
        let array = $array_cast($array)?;
        Ok::<ScalarValue, DataFusionError>(ScalarValue::$SCALAR(
            match array.is_null($index) {
                true => None,
                false => Some(array.value($index).into()),
            },
        ))
    }};
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE($ENUM), $SIZE),
        }
    }};
}

macro_rules! build_timestamp_array_from_option {
    ($TIME_UNIT:expr, $TZ:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {
        match $EXPR {
            Some(value) => {
                Arc::new($ARRAY_TYPE::from_value(*value, $SIZE).with_timezone_opt($TZ))
            }
            None => new_null_array(&DataType::Timestamp($TIME_UNIT, $TZ), $SIZE),
        }
    };
}

macro_rules! eq_array_primitive {
    ($array:expr, $index:expr, $array_cast:ident, $VALUE:expr) => {{
        let array = $array_cast($array)?;
        let is_valid = array.is_valid($index);
        Ok::<bool, DataFusionError>(match $VALUE {
            Some(val) => is_valid && &array.value($index) == val,
            None => !is_valid,
        })
    }};
}

impl ScalarValue {
    /// Create a [`Result<ScalarValue>`] with the provided value and datatype
    ///
    /// # Panics
    ///
    /// Panics if d is not compatible with T
    pub fn new_primitive<T: ArrowPrimitiveType>(
        a: Option<T::Native>,
        d: &DataType,
    ) -> Result<Self> {
        match a {
            None => d.try_into(),
            Some(v) => {
                let array = PrimitiveArray::<T>::new(vec![v].into(), None)
                    .with_data_type(d.clone());
                Self::try_from_array(&array, 0)
            }
        }
    }

    /// Create a decimal Scalar from value/precision and scale.
    pub fn try_new_decimal128(value: i128, precision: u8, scale: i8) -> Result<Self> {
        // make sure the precision and scale is valid
        if precision <= DECIMAL128_MAX_PRECISION && scale.unsigned_abs() <= precision {
            return Ok(ScalarValue::Decimal128(Some(value), precision, scale));
        }
        _internal_err!(
            "Can not new a decimal type ScalarValue for precision {precision} and scale {scale}"
        )
    }

    /// Create a Null instance of ScalarValue for this datatype
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::datatypes::DataType;
    ///
    /// let scalar = ScalarValue::try_new_null(&DataType::Int32).unwrap();
    /// assert_eq!(scalar.is_null(), true);
    /// assert_eq!(scalar.data_type(), DataType::Int32);
    /// ```
    pub fn try_new_null(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Float16 => ScalarValue::Float16(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Decimal128(precision, scale) => {
                ScalarValue::Decimal128(None, *precision, *scale)
            }
            DataType::Decimal256(precision, scale) => {
                ScalarValue::Decimal256(None, *precision, *scale)
            }
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Utf8View => ScalarValue::Utf8View(None),
            DataType::Binary => ScalarValue::Binary(None),
            DataType::BinaryView => ScalarValue::BinaryView(None),
            DataType::FixedSizeBinary(len) => ScalarValue::FixedSizeBinary(*len, None),
            DataType::LargeBinary => ScalarValue::LargeBinary(None),
            DataType::Date32 => ScalarValue::Date32(None),
            DataType::Date64 => ScalarValue::Date64(None),
            DataType::Time32(TimeUnit::Second) => ScalarValue::Time32Second(None),
            DataType::Time32(TimeUnit::Millisecond) => {
                ScalarValue::Time32Millisecond(None)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                ScalarValue::Time64Microsecond(None)
            }
            DataType::Time64(TimeUnit::Nanosecond) => ScalarValue::Time64Nanosecond(None),
            DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                ScalarValue::TimestampSecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                ScalarValue::TimestampMillisecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                ScalarValue::TimestampMicrosecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                ScalarValue::TimestampNanosecond(None, tz_opt.clone())
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(None)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(None)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(None)
            }
            DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(None),
            DataType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(None)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(None)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(None)
            }
            DataType::Dictionary(index_type, value_type) => ScalarValue::Dictionary(
                index_type.clone(),
                Box::new(value_type.as_ref().try_into()?),
            ),
            // `ScalaValue::List` contains single element `ListArray`.
            DataType::List(field_ref) => ScalarValue::List(Arc::new(
                GenericListArray::new_null(Arc::clone(field_ref), 1),
            )),
            // `ScalarValue::LargeList` contains single element `LargeListArray`.
            DataType::LargeList(field_ref) => ScalarValue::LargeList(Arc::new(
                GenericListArray::new_null(Arc::clone(field_ref), 1),
            )),
            // `ScalaValue::FixedSizeList` contains single element `FixedSizeList`.
            DataType::FixedSizeList(field_ref, fixed_length) => {
                ScalarValue::FixedSizeList(Arc::new(FixedSizeListArray::new_null(
                    Arc::clone(field_ref),
                    *fixed_length,
                    1,
                )))
            }
            DataType::Struct(fields) => ScalarValue::Struct(
                new_null_array(&DataType::Struct(fields.to_owned()), 1)
                    .as_struct()
                    .to_owned()
                    .into(),
            ),
            DataType::Map(fields, sorted) => ScalarValue::Map(
                new_null_array(&DataType::Map(fields.to_owned(), sorted.to_owned()), 1)
                    .as_map()
                    .to_owned()
                    .into(),
            ),
            DataType::Union(fields, mode) => {
                ScalarValue::Union(None, fields.clone(), *mode)
            }
            DataType::Null => ScalarValue::Null,
            _ => {
                return _not_impl_err!(
                    "Can't create a null scalar from data_type \"{data_type:?}\""
                );
            }
        })
    }

    /// Returns a [`ScalarValue::Utf8`] representing `val`
    pub fn new_utf8(val: impl Into<String>) -> Self {
        ScalarValue::from(val.into())
    }

    /// Returns a [`ScalarValue::Utf8View`] representing `val`
    pub fn new_utf8view(val: impl Into<String>) -> Self {
        ScalarValue::Utf8View(Some(val.into()))
    }

    /// Returns a [`ScalarValue::IntervalYearMonth`] representing
    /// `years` years and `months` months
    pub fn new_interval_ym(years: i32, months: i32) -> Self {
        let val = IntervalYearMonthType::make_value(years, months);
        ScalarValue::IntervalYearMonth(Some(val))
    }

    /// Returns a [`ScalarValue::IntervalDayTime`] representing
    /// `days` days and `millis` milliseconds
    pub fn new_interval_dt(days: i32, millis: i32) -> Self {
        let val = IntervalDayTimeType::make_value(days, millis);
        Self::IntervalDayTime(Some(val))
    }

    /// Returns a [`ScalarValue::IntervalMonthDayNano`] representing
    /// `months` months and `days` days, and `nanos` nanoseconds
    pub fn new_interval_mdn(months: i32, days: i32, nanos: i64) -> Self {
        let val = IntervalMonthDayNanoType::make_value(months, days, nanos);
        ScalarValue::IntervalMonthDayNano(Some(val))
    }

    /// Returns a [`ScalarValue`] representing
    /// `value` and `tz_opt` timezone
    pub fn new_timestamp<T: ArrowTimestampType>(
        value: Option<i64>,
        tz_opt: Option<Arc<str>>,
    ) -> Self {
        match T::UNIT {
            TimeUnit::Second => ScalarValue::TimestampSecond(value, tz_opt),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, tz_opt),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, tz_opt),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, tz_opt),
        }
    }

    /// Returns a [`ScalarValue`] representing PI
    pub fn new_pi(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(std::f32::consts::PI)),
            DataType::Float64 => Ok(ScalarValue::from(std::f64::consts::PI)),
            _ => _internal_err!("PI is not supported for data type: {:?}", datatype),
        }
    }

    /// Returns a [`ScalarValue`] representing PI's upper bound
    pub fn new_pi_upper(datatype: &DataType) -> Result<ScalarValue> {
        // TODO: replace the constants with next_up/next_down when
        // they are stabilized: https://doc.rust-lang.org/std/primitive.f64.html#method.next_up
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(consts::PI_UPPER_F32)),
            DataType::Float64 => Ok(ScalarValue::from(consts::PI_UPPER_F64)),
            _ => {
                _internal_err!("PI_UPPER is not supported for data type: {:?}", datatype)
            }
        }
    }

    /// Returns a [`ScalarValue`] representing -PI's lower bound
    pub fn new_negative_pi_lower(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(consts::NEGATIVE_PI_LOWER_F32)),
            DataType::Float64 => Ok(ScalarValue::from(consts::NEGATIVE_PI_LOWER_F64)),
            _ => {
                _internal_err!("-PI_LOWER is not supported for data type: {:?}", datatype)
            }
        }
    }

    /// Returns a [`ScalarValue`] representing FRAC_PI_2's upper bound
    pub fn new_frac_pi_2_upper(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(consts::FRAC_PI_2_UPPER_F32)),
            DataType::Float64 => Ok(ScalarValue::from(consts::FRAC_PI_2_UPPER_F64)),
            _ => {
                _internal_err!(
                    "PI_UPPER/2 is not supported for data type: {:?}",
                    datatype
                )
            }
        }
    }

    // Returns a [`ScalarValue`] representing FRAC_PI_2's lower bound
    pub fn new_neg_frac_pi_2_lower(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => {
                Ok(ScalarValue::from(consts::NEGATIVE_FRAC_PI_2_LOWER_F32))
            }
            DataType::Float64 => {
                Ok(ScalarValue::from(consts::NEGATIVE_FRAC_PI_2_LOWER_F64))
            }
            _ => {
                _internal_err!(
                    "-PI/2_LOWER is not supported for data type: {:?}",
                    datatype
                )
            }
        }
    }

    /// Returns a [`ScalarValue`] representing -PI
    pub fn new_negative_pi(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(-std::f32::consts::PI)),
            DataType::Float64 => Ok(ScalarValue::from(-std::f64::consts::PI)),
            _ => _internal_err!("-PI is not supported for data type: {:?}", datatype),
        }
    }

    /// Returns a [`ScalarValue`] representing PI/2
    pub fn new_frac_pi_2(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(std::f32::consts::FRAC_PI_2)),
            DataType::Float64 => Ok(ScalarValue::from(std::f64::consts::FRAC_PI_2)),
            _ => _internal_err!("PI/2 is not supported for data type: {:?}", datatype),
        }
    }

    /// Returns a [`ScalarValue`] representing -PI/2
    pub fn new_neg_frac_pi_2(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(-std::f32::consts::FRAC_PI_2)),
            DataType::Float64 => Ok(ScalarValue::from(-std::f64::consts::FRAC_PI_2)),
            _ => _internal_err!("-PI/2 is not supported for data type: {:?}", datatype),
        }
    }

    /// Returns a [`ScalarValue`] representing infinity
    pub fn new_infinity(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(f32::INFINITY)),
            DataType::Float64 => Ok(ScalarValue::from(f64::INFINITY)),
            _ => {
                _internal_err!("Infinity is not supported for data type: {:?}", datatype)
            }
        }
    }

    /// Returns a [`ScalarValue`] representing negative infinity
    pub fn new_neg_infinity(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            DataType::Float32 => Ok(ScalarValue::from(f32::NEG_INFINITY)),
            DataType::Float64 => Ok(ScalarValue::from(f64::NEG_INFINITY)),
            _ => {
                _internal_err!(
                    "Negative Infinity is not supported for data type: {:?}",
                    datatype
                )
            }
        }
    }

    /// Create a zero value in the given type.
    pub fn new_zero(datatype: &DataType) -> Result<ScalarValue> {
        Ok(match datatype {
            DataType::Boolean => ScalarValue::Boolean(Some(false)),
            DataType::Int8 => ScalarValue::Int8(Some(0)),
            DataType::Int16 => ScalarValue::Int16(Some(0)),
            DataType::Int32 => ScalarValue::Int32(Some(0)),
            DataType::Int64 => ScalarValue::Int64(Some(0)),
            DataType::UInt8 => ScalarValue::UInt8(Some(0)),
            DataType::UInt16 => ScalarValue::UInt16(Some(0)),
            DataType::UInt32 => ScalarValue::UInt32(Some(0)),
            DataType::UInt64 => ScalarValue::UInt64(Some(0)),
            DataType::Float16 => ScalarValue::Float16(Some(f16::from_f32(0.0))),
            DataType::Float32 => ScalarValue::Float32(Some(0.0)),
            DataType::Float64 => ScalarValue::Float64(Some(0.0)),
            DataType::Decimal128(precision, scale) => {
                ScalarValue::Decimal128(Some(0), *precision, *scale)
            }
            DataType::Decimal256(precision, scale) => {
                ScalarValue::Decimal256(Some(i256::ZERO), *precision, *scale)
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                ScalarValue::TimestampSecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                ScalarValue::TimestampMillisecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ScalarValue::TimestampMicrosecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                ScalarValue::TimestampNanosecond(Some(0), tz.clone())
            }
            DataType::Time32(TimeUnit::Second) => ScalarValue::Time32Second(Some(0)),
            DataType::Time32(TimeUnit::Millisecond) => {
                ScalarValue::Time32Millisecond(Some(0))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                ScalarValue::Time64Microsecond(Some(0))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                ScalarValue::Time64Nanosecond(Some(0))
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(Some(0))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::ZERO))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::ZERO))
            }
            DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(Some(0)),
            DataType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(Some(0))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(Some(0))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(Some(0))
            }
            DataType::Date32 => ScalarValue::Date32(Some(0)),
            DataType::Date64 => ScalarValue::Date64(Some(0)),
            _ => {
                return _not_impl_err!(
                    "Can't create a zero scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// Returns a default value for the given `DataType`.
    ///
    /// This function is useful when you need to initialize a column with
    /// non-null values in a DataFrame or when you need a "zero" value
    /// for a specific data type.
    ///
    /// # Default Values
    ///
    /// - **Numeric types**: Returns zero (via [`new_zero`])
    /// - **String types**: Returns empty string (`""`)
    /// - **Binary types**: Returns empty byte array
    /// - **Temporal types**: Returns zero/epoch value
    /// - **List types**: Returns empty list
    /// - **Struct types**: Returns struct with all fields set to their defaults
    /// - **Dictionary types**: Returns dictionary with default value
    /// - **Map types**: Returns empty map
    /// - **Union types**: Returns first variant with default value
    ///
    /// # Errors
    ///
    /// Returns an error for data types that don't have a clear default value
    /// or are not yet supported (e.g., `RunEndEncoded`).
    ///
    /// [`new_zero`]: Self::new_zero
    pub fn new_default(datatype: &DataType) -> Result<ScalarValue> {
        match datatype {
            // Null type
            DataType::Null => Ok(ScalarValue::Null),

            // Numeric types
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Date32
            | DataType::Date64 => ScalarValue::new_zero(datatype),

            // String types
            DataType::Utf8 => Ok(ScalarValue::Utf8(Some("".to_string()))),
            DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some("".to_string()))),
            DataType::Utf8View => Ok(ScalarValue::Utf8View(Some("".to_string()))),

            // Binary types
            DataType::Binary => Ok(ScalarValue::Binary(Some(vec![]))),
            DataType::LargeBinary => Ok(ScalarValue::LargeBinary(Some(vec![]))),
            DataType::BinaryView => Ok(ScalarValue::BinaryView(Some(vec![]))),

            // Fixed-size binary
            DataType::FixedSizeBinary(size) => Ok(ScalarValue::FixedSizeBinary(
                *size,
                Some(vec![0; *size as usize]),
            )),

            // List types
            DataType::List(field) => {
                let list =
                    ScalarValue::new_list(&[], field.data_type(), field.is_nullable());
                Ok(ScalarValue::List(list))
            }
            DataType::FixedSizeList(field, _size) => {
                let empty_arr = new_empty_array(field.data_type());
                let values = Arc::new(
                    SingleRowListArrayBuilder::new(empty_arr)
                        .with_nullable(field.is_nullable())
                        .build_fixed_size_list_array(0),
                );
                Ok(ScalarValue::FixedSizeList(values))
            }
            DataType::LargeList(field) => {
                let list = ScalarValue::new_large_list(&[], field.data_type());
                Ok(ScalarValue::LargeList(list))
            }

            // Struct types
            DataType::Struct(fields) => {
                let values = fields
                    .iter()
                    .map(|f| ScalarValue::new_default(f.data_type()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarValue::Struct(Arc::new(StructArray::new(
                    fields.clone(),
                    values
                        .into_iter()
                        .map(|v| v.to_array())
                        .collect::<Result<_>>()?,
                    None,
                ))))
            }

            // Dictionary types
            DataType::Dictionary(key_type, value_type) => Ok(ScalarValue::Dictionary(
                key_type.clone(),
                Box::new(ScalarValue::new_default(value_type)?),
            )),

            // Map types
            DataType::Map(field, _) => Ok(ScalarValue::Map(Arc::new(MapArray::from(
                ArrayData::new_empty(field.data_type()),
            )))),

            // Union types - return first variant with default value
            DataType::Union(fields, mode) => {
                if let Some((type_id, field)) = fields.iter().next() {
                    let default_value = ScalarValue::new_default(field.data_type())?;
                    Ok(ScalarValue::Union(
                        Some((type_id, Box::new(default_value))),
                        fields.clone(),
                        *mode,
                    ))
                } else {
                    _internal_err!("Union type must have at least one field")
                }
            }

            // Unsupported types for now
            _ => {
                _not_impl_err!(
                    "Default value for data_type \"{datatype:?}\" is not implemented yet"
                )
            }
        }
    }

    /// Create an one value in the given type.
    pub fn new_one(datatype: &DataType) -> Result<ScalarValue> {
        Ok(match datatype {
            DataType::Int8 => ScalarValue::Int8(Some(1)),
            DataType::Int16 => ScalarValue::Int16(Some(1)),
            DataType::Int32 => ScalarValue::Int32(Some(1)),
            DataType::Int64 => ScalarValue::Int64(Some(1)),
            DataType::UInt8 => ScalarValue::UInt8(Some(1)),
            DataType::UInt16 => ScalarValue::UInt16(Some(1)),
            DataType::UInt32 => ScalarValue::UInt32(Some(1)),
            DataType::UInt64 => ScalarValue::UInt64(Some(1)),
            DataType::Float16 => ScalarValue::Float16(Some(f16::from_f32(1.0))),
            DataType::Float32 => ScalarValue::Float32(Some(1.0)),
            DataType::Float64 => ScalarValue::Float64(Some(1.0)),
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(
                    *precision, *scale,
                )?;
                if *scale < 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i128::from(10).checked_pow(*scale as u32) {
                    Some(value) => {
                        ScalarValue::Decimal128(Some(value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            DataType::Decimal256(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal256Type>(
                    *precision, *scale,
                )?;
                if *scale < 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i256::from(10).checked_pow(*scale as u32) {
                    Some(value) => {
                        ScalarValue::Decimal256(Some(value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            _ => {
                return _not_impl_err!(
                    "Can't create an one scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// Create a negative one value in the given type.
    pub fn new_negative_one(datatype: &DataType) -> Result<ScalarValue> {
        Ok(match datatype {
            DataType::Int8 | DataType::UInt8 => ScalarValue::Int8(Some(-1)),
            DataType::Int16 | DataType::UInt16 => ScalarValue::Int16(Some(-1)),
            DataType::Int32 | DataType::UInt32 => ScalarValue::Int32(Some(-1)),
            DataType::Int64 | DataType::UInt64 => ScalarValue::Int64(Some(-1)),
            DataType::Float16 => ScalarValue::Float16(Some(f16::from_f32(-1.0))),
            DataType::Float32 => ScalarValue::Float32(Some(-1.0)),
            DataType::Float64 => ScalarValue::Float64(Some(-1.0)),
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(
                    *precision, *scale,
                )?;
                if *scale < 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i128::from(10).checked_pow(*scale as u32) {
                    Some(value) => {
                        ScalarValue::Decimal128(Some(-value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            DataType::Decimal256(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal256Type>(
                    *precision, *scale,
                )?;
                if *scale < 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i256::from(10).checked_pow(*scale as u32) {
                    Some(value) => {
                        ScalarValue::Decimal256(Some(-value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            _ => {
                return _not_impl_err!(
                    "Can't create a negative one scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    pub fn new_ten(datatype: &DataType) -> Result<ScalarValue> {
        Ok(match datatype {
            DataType::Int8 => ScalarValue::Int8(Some(10)),
            DataType::Int16 => ScalarValue::Int16(Some(10)),
            DataType::Int32 => ScalarValue::Int32(Some(10)),
            DataType::Int64 => ScalarValue::Int64(Some(10)),
            DataType::UInt8 => ScalarValue::UInt8(Some(10)),
            DataType::UInt16 => ScalarValue::UInt16(Some(10)),
            DataType::UInt32 => ScalarValue::UInt32(Some(10)),
            DataType::UInt64 => ScalarValue::UInt64(Some(10)),
            DataType::Float16 => ScalarValue::Float16(Some(f16::from_f32(10.0))),
            DataType::Float32 => ScalarValue::Float32(Some(10.0)),
            DataType::Float64 => ScalarValue::Float64(Some(10.0)),
            DataType::Decimal128(precision, scale) => {
                if let Err(err) = validate_decimal_precision_and_scale::<Decimal128Type>(
                    *precision, *scale,
                ) {
                    return _internal_err!("Invalid precision and scale {err}");
                }
                if *scale <= 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i128::from(10).checked_pow((*scale + 1) as u32) {
                    Some(value) => {
                        ScalarValue::Decimal128(Some(value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            DataType::Decimal256(precision, scale) => {
                if let Err(err) = validate_decimal_precision_and_scale::<Decimal256Type>(
                    *precision, *scale,
                ) {
                    return _internal_err!("Invalid precision and scale {err}");
                }
                if *scale <= 0 {
                    return _internal_err!("Negative scale is not supported");
                }
                match i256::from(10).checked_pow((*scale + 1) as u32) {
                    Some(value) => {
                        ScalarValue::Decimal256(Some(value), *precision, *scale)
                    }
                    None => return _internal_err!("Unsupported scale {scale}"),
                }
            }
            _ => {
                return _not_impl_err!(
                    "Can't create a ten scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// return the [`DataType`] of this `ScalarValue`
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Decimal128(_, precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            ScalarValue::Decimal256(_, precision, scale) => {
                DataType::Decimal256(*precision, *scale)
            }
            ScalarValue::TimestampSecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Second, tz_opt.clone())
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Millisecond, tz_opt.clone())
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt.clone())
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
            }
            ScalarValue::Float16(_) => DataType::Float16,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Utf8View(_) => DataType::Utf8View,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::BinaryView(_) => DataType::BinaryView,
            ScalarValue::FixedSizeBinary(sz, _) => DataType::FixedSizeBinary(*sz),
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::List(arr) => arr.data_type().to_owned(),
            ScalarValue::LargeList(arr) => arr.data_type().to_owned(),
            ScalarValue::FixedSizeList(arr) => arr.data_type().to_owned(),
            ScalarValue::Struct(arr) => arr.data_type().to_owned(),
            ScalarValue::Map(arr) => arr.data_type().to_owned(),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Time32Second(_) => DataType::Time32(TimeUnit::Second),
            ScalarValue::Time32Millisecond(_) => DataType::Time32(TimeUnit::Millisecond),
            ScalarValue::Time64Microsecond(_) => DataType::Time64(TimeUnit::Microsecond),
            ScalarValue::Time64Nanosecond(_) => DataType::Time64(TimeUnit::Nanosecond),
            ScalarValue::IntervalYearMonth(_) => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::IntervalMonthDayNano(_) => {
                DataType::Interval(IntervalUnit::MonthDayNano)
            }
            ScalarValue::DurationSecond(_) => DataType::Duration(TimeUnit::Second),
            ScalarValue::DurationMillisecond(_) => {
                DataType::Duration(TimeUnit::Millisecond)
            }
            ScalarValue::DurationMicrosecond(_) => {
                DataType::Duration(TimeUnit::Microsecond)
            }
            ScalarValue::DurationNanosecond(_) => {
                DataType::Duration(TimeUnit::Nanosecond)
            }
            ScalarValue::Union(_, fields, mode) => DataType::Union(fields.clone(), *mode),
            ScalarValue::Dictionary(k, v) => {
                DataType::Dictionary(k.clone(), Box::new(v.data_type()))
            }
            ScalarValue::Null => DataType::Null,
        }
    }

    /// Calculate arithmetic negation for a scalar value
    pub fn arithmetic_negate(&self) -> Result<Self> {
        fn neg_checked_with_ctx<T: ArrowNativeTypeOp>(
            v: T,
            ctx: impl Fn() -> String,
        ) -> Result<T> {
            v.neg_checked()
                .map_err(|e| arrow_datafusion_err!(e).context(ctx()))
        }
        match self {
            ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Float16(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None) => Ok(self.clone()),
            ScalarValue::Float16(Some(v)) => {
                Ok(ScalarValue::Float16(Some(f16::from_f32(-v.to_f32()))))
            }
            ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(-v))),
            ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(-v))),
            ScalarValue::Int8(Some(v)) => Ok(ScalarValue::Int8(Some(v.neg_checked()?))),
            ScalarValue::Int16(Some(v)) => Ok(ScalarValue::Int16(Some(v.neg_checked()?))),
            ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(v.neg_checked()?))),
            ScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(Some(v.neg_checked()?))),
            ScalarValue::IntervalYearMonth(Some(v)) => Ok(
                ScalarValue::IntervalYearMonth(Some(neg_checked_with_ctx(*v, || {
                    format!("In negation of IntervalYearMonth({v})")
                })?)),
            ),
            ScalarValue::IntervalDayTime(Some(v)) => {
                let (days, ms) = IntervalDayTimeType::to_parts(*v);
                let val = IntervalDayTimeType::make_value(
                    neg_checked_with_ctx(days, || {
                        format!("In negation of days {days} in IntervalDayTime")
                    })?,
                    neg_checked_with_ctx(ms, || {
                        format!("In negation of milliseconds {ms} in IntervalDayTime")
                    })?,
                );
                Ok(ScalarValue::IntervalDayTime(Some(val)))
            }
            ScalarValue::IntervalMonthDayNano(Some(v)) => {
                let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
                let val = IntervalMonthDayNanoType::make_value(
                    neg_checked_with_ctx(months, || {
                        format!("In negation of months {months} of IntervalMonthDayNano")
                    })?,
                    neg_checked_with_ctx(days, || {
                        format!("In negation of days {days} of IntervalMonthDayNano")
                    })?,
                    neg_checked_with_ctx(nanos, || {
                        format!("In negation of nanos {nanos} of IntervalMonthDayNano")
                    })?,
                );
                Ok(ScalarValue::IntervalMonthDayNano(Some(val)))
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                Ok(ScalarValue::Decimal128(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of Decimal128({v}, {precision}, {scale})")
                    })?),
                    *precision,
                    *scale,
                ))
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                Ok(ScalarValue::Decimal256(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of Decimal256({v}, {precision}, {scale})")
                    })?),
                    *precision,
                    *scale,
                ))
            }
            ScalarValue::TimestampSecond(Some(v), tz) => {
                Ok(ScalarValue::TimestampSecond(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of TimestampSecond({v})")
                    })?),
                    tz.clone(),
                ))
            }
            ScalarValue::TimestampNanosecond(Some(v), tz) => {
                Ok(ScalarValue::TimestampNanosecond(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of TimestampNanoSecond({v})")
                    })?),
                    tz.clone(),
                ))
            }
            ScalarValue::TimestampMicrosecond(Some(v), tz) => {
                Ok(ScalarValue::TimestampMicrosecond(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of TimestampMicroSecond({v})")
                    })?),
                    tz.clone(),
                ))
            }
            ScalarValue::TimestampMillisecond(Some(v), tz) => {
                Ok(ScalarValue::TimestampMillisecond(
                    Some(neg_checked_with_ctx(*v, || {
                        format!("In negation of TimestampMilliSecond({v})")
                    })?),
                    tz.clone(),
                ))
            }
            value => _internal_err!(
                "Can not run arithmetic negative on scalar value {value:?}"
            ),
        }
    }

    /// Wrapping addition of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn add<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = add_wrapping(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }
    /// Checked addition of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn add_checked<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = add(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Wrapping subtraction of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn sub<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = sub_wrapping(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Checked subtraction of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn sub_checked<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = sub(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Wrapping multiplication of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels.
    pub fn mul<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = mul_wrapping(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Checked multiplication of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels.
    pub fn mul_checked<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = mul(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Performs `lhs / rhs`
    ///
    /// Overflow or division by zero will result in an error, with exception to
    /// floating point numbers, which instead follow the IEEE 754 rules.
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels.
    pub fn div<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = div(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Performs `lhs % rhs`
    ///
    /// Overflow or division by zero will result in an error, with exception to
    /// floating point numbers, which instead follow the IEEE 754 rules.
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels.
    pub fn rem<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = rem(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        )
    }

    /// whether this value is null or not.
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Null => true,
            ScalarValue::Float16(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
            ScalarValue::Decimal256(v, _, _) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Utf8(v)
            | ScalarValue::Utf8View(v)
            | ScalarValue::LargeUtf8(v) => v.is_none(),
            ScalarValue::Binary(v)
            | ScalarValue::BinaryView(v)
            | ScalarValue::FixedSizeBinary(_, v)
            | ScalarValue::LargeBinary(v) => v.is_none(),
            // arr.len() should be 1 for a list scalar, but we don't seem to
            // enforce that anywhere, so we still check against array length.
            ScalarValue::List(arr) => arr.len() == arr.null_count(),
            ScalarValue::LargeList(arr) => arr.len() == arr.null_count(),
            ScalarValue::FixedSizeList(arr) => arr.len() == arr.null_count(),
            ScalarValue::Struct(arr) => arr.len() == arr.null_count(),
            ScalarValue::Map(arr) => arr.len() == arr.null_count(),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::Time32Second(v) => v.is_none(),
            ScalarValue::Time32Millisecond(v) => v.is_none(),
            ScalarValue::Time64Microsecond(v) => v.is_none(),
            ScalarValue::Time64Nanosecond(v) => v.is_none(),
            ScalarValue::TimestampSecond(v, _) => v.is_none(),
            ScalarValue::TimestampMillisecond(v, _) => v.is_none(),
            ScalarValue::TimestampMicrosecond(v, _) => v.is_none(),
            ScalarValue::TimestampNanosecond(v, _) => v.is_none(),
            ScalarValue::IntervalYearMonth(v) => v.is_none(),
            ScalarValue::IntervalDayTime(v) => v.is_none(),
            ScalarValue::IntervalMonthDayNano(v) => v.is_none(),
            ScalarValue::DurationSecond(v) => v.is_none(),
            ScalarValue::DurationMillisecond(v) => v.is_none(),
            ScalarValue::DurationMicrosecond(v) => v.is_none(),
            ScalarValue::DurationNanosecond(v) => v.is_none(),
            ScalarValue::Union(v, _, _) => match v {
                Some((_, s)) => s.is_null(),
                None => true,
            },
            ScalarValue::Dictionary(_, v) => v.is_null(),
        }
    }

    /// Absolute distance between two numeric values (of the same type). This method will return
    /// None if either one of the arguments are null. It might also return None if the resulting
    /// distance is greater than [`usize::MAX`]. If the type is a float, then the distance will be
    /// rounded to the nearest integer.
    ///
    ///
    /// Note: the datatype itself must support subtraction.
    pub fn distance(&self, other: &ScalarValue) -> Option<usize> {
        match (self, other) {
            (Self::Int8(Some(l)), Self::Int8(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int16(Some(l)), Self::Int16(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int32(Some(l)), Self::Int32(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int64(Some(l)), Self::Int64(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt8(Some(l)), Self::UInt8(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt16(Some(l)), Self::UInt16(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt32(Some(l)), Self::UInt32(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt64(Some(l)), Self::UInt64(Some(r))) => Some(l.abs_diff(*r) as _),
            // TODO: we might want to look into supporting ceil/floor here for floats.
            (Self::Float16(Some(l)), Self::Float16(Some(r))) => {
                Some((f16::to_f32(*l) - f16::to_f32(*r)).abs().round() as _)
            }
            (Self::Float32(Some(l)), Self::Float32(Some(r))) => {
                Some((l - r).abs().round() as _)
            }
            (Self::Float64(Some(l)), Self::Float64(Some(r))) => {
                Some((l - r).abs().round() as _)
            }
            (
                Self::Decimal128(Some(l), lprecision, lscale),
                Self::Decimal128(Some(r), rprecision, rscale),
            ) => {
                if lprecision == rprecision && lscale == rscale {
                    l.checked_sub(*r)?.checked_abs()?.to_usize()
                } else {
                    None
                }
            }
            (
                Self::Decimal256(Some(l), lprecision, lscale),
                Self::Decimal256(Some(r), rprecision, rscale),
            ) => {
                if lprecision == rprecision && lscale == rscale {
                    l.checked_sub(*r)?.checked_abs()?.to_usize()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Converts a scalar value into an 1-row array.
    ///
    /// # Errors
    ///
    /// Errors if the ScalarValue cannot be converted into a 1-row array
    pub fn to_array(&self) -> Result<ArrayRef> {
        self.to_array_of_size(1)
    }

    /// Converts a scalar into an arrow [`Scalar`] (which implements
    /// the [`Datum`] interface).
    ///
    /// This can be used to call arrow compute kernels such as `lt`
    ///
    /// # Errors
    ///
    /// Errors if the ScalarValue cannot be converted into a 1-row array
    ///
    /// # Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{BooleanArray, Int32Array};
    ///
    /// let arr = Int32Array::from(vec![Some(1), None, Some(10)]);
    /// let five = ScalarValue::Int32(Some(5));
    ///
    /// let result = arrow::compute::kernels::cmp::lt(
    ///   &arr,
    ///   &five.to_scalar().unwrap(),
    /// ).unwrap();
    ///
    /// let expected = BooleanArray::from(vec![
    ///     Some(true),
    ///     None,
    ///     Some(false)
    ///   ]
    /// );
    ///
    /// assert_eq!(&result, &expected);
    /// ```
    /// [`Datum`]: arrow::array::Datum
    pub fn to_scalar(&self) -> Result<Scalar<ArrayRef>> {
        Ok(Scalar::new(self.to_array_of_size(1)?))
    }

    /// Converts an iterator of references [`ScalarValue`] into an [`ArrayRef`]
    /// corresponding to those values. For example, an iterator of
    /// [`ScalarValue::Int32`] would be converted to an [`Int32Array`].
    ///
    /// Returns an error if the iterator is empty or if the
    /// [`ScalarValue`]s are not all the same type
    ///
    /// # Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{ArrayRef, BooleanArray};
    ///
    /// let scalars = vec![
    ///   ScalarValue::Boolean(Some(true)),
    ///   ScalarValue::Boolean(None),
    ///   ScalarValue::Boolean(Some(false)),
    /// ];
    ///
    /// // Build an Array from the list of ScalarValues
    /// let array = ScalarValue::iter_to_array(scalars.into_iter())
    ///   .unwrap();
    ///
    /// let expected: ArrayRef = std::sync::Arc::new(
    ///   BooleanArray::from(vec![
    ///     Some(true),
    ///     None,
    ///     Some(false)
    ///   ]
    /// ));
    ///
    /// assert_eq!(&array, &expected);
    /// ```
    pub fn iter_to_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<ArrayRef> {
        let mut scalars = scalars.into_iter().peekable();

        // figure out the type based on the first element
        let data_type = match scalars.peek() {
            None => {
                return _exec_err!("Empty iterator passed to ScalarValue::iter_to_array");
            }
            Some(sv) => sv.data_type(),
        };

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for primitive types
        macro_rules! build_array_primitive {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            _exec_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        macro_rules! build_array_primitive_tz {
            ($ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v, _) = sv {
                            Ok(v)
                        } else {
                            _exec_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array.with_timezone_opt($TZ.clone()))
                }
            }};
        }

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for "string-like" types.
        macro_rules! build_array_string {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            _exec_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        let array: ArrayRef = match &data_type {
            DataType::Decimal128(precision, scale) => {
                let decimal_array =
                    ScalarValue::iter_to_decimal_array(scalars, *precision, *scale)?;
                Arc::new(decimal_array)
            }
            DataType::Decimal256(precision, scale) => {
                let decimal_array =
                    ScalarValue::iter_to_decimal256_array(scalars, *precision, *scale)?;
                Arc::new(decimal_array)
            }
            DataType::Null => ScalarValue::iter_to_null_array(scalars)?,
            DataType::Boolean => build_array_primitive!(BooleanArray, Boolean),
            DataType::Float16 => build_array_primitive!(Float16Array, Float16),
            DataType::Float32 => build_array_primitive!(Float32Array, Float32),
            DataType::Float64 => build_array_primitive!(Float64Array, Float64),
            DataType::Int8 => build_array_primitive!(Int8Array, Int8),
            DataType::Int16 => build_array_primitive!(Int16Array, Int16),
            DataType::Int32 => build_array_primitive!(Int32Array, Int32),
            DataType::Int64 => build_array_primitive!(Int64Array, Int64),
            DataType::UInt8 => build_array_primitive!(UInt8Array, UInt8),
            DataType::UInt16 => build_array_primitive!(UInt16Array, UInt16),
            DataType::UInt32 => build_array_primitive!(UInt32Array, UInt32),
            DataType::UInt64 => build_array_primitive!(UInt64Array, UInt64),
            DataType::Utf8View => build_array_string!(StringViewArray, Utf8View),
            DataType::Utf8 => build_array_string!(StringArray, Utf8),
            DataType::LargeUtf8 => build_array_string!(LargeStringArray, LargeUtf8),
            DataType::BinaryView => build_array_string!(BinaryViewArray, BinaryView),
            DataType::Binary => build_array_string!(BinaryArray, Binary),
            DataType::LargeBinary => build_array_string!(LargeBinaryArray, LargeBinary),
            DataType::Date32 => build_array_primitive!(Date32Array, Date32),
            DataType::Date64 => build_array_primitive!(Date64Array, Date64),
            DataType::Time32(TimeUnit::Second) => {
                build_array_primitive!(Time32SecondArray, Time32Second)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                build_array_primitive!(Time32MillisecondArray, Time32Millisecond)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                build_array_primitive!(Time64MicrosecondArray, Time64Microsecond)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                build_array_primitive!(Time64NanosecondArray, Time64Nanosecond)
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                build_array_primitive_tz!(TimestampSecondArray, TimestampSecond, tz)
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                build_array_primitive_tz!(
                    TimestampMillisecondArray,
                    TimestampMillisecond,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                build_array_primitive_tz!(
                    TimestampMicrosecondArray,
                    TimestampMicrosecond,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                build_array_primitive_tz!(
                    TimestampNanosecondArray,
                    TimestampNanosecond,
                    tz
                )
            }
            DataType::Duration(TimeUnit::Second) => {
                build_array_primitive!(DurationSecondArray, DurationSecond)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                build_array_primitive!(DurationMillisecondArray, DurationMillisecond)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                build_array_primitive!(DurationMicrosecondArray, DurationMicrosecond)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                build_array_primitive!(DurationNanosecondArray, DurationNanosecond)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                build_array_primitive!(IntervalDayTimeArray, IntervalDayTime)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                build_array_primitive!(IntervalYearMonthArray, IntervalYearMonth)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                build_array_primitive!(IntervalMonthDayNanoArray, IntervalMonthDayNano)
            }
            DataType::FixedSizeList(_, _) => {
                // arrow::compute::concat does not allow inconsistent types including the size of FixedSizeList.
                // The length of nulls here we got is 1, so we need to resize the length of nulls to
                // the length of non-nulls.
                let mut arrays =
                    scalars.map(|s| s.to_array()).collect::<Result<Vec<_>>>()?;
                let first_non_null_data_type = arrays
                    .iter()
                    .find(|sv| !sv.is_null(0))
                    .map(|sv| sv.data_type().to_owned());
                if let Some(DataType::FixedSizeList(f, l)) = first_non_null_data_type {
                    for array in arrays.iter_mut() {
                        if array.is_null(0) {
                            *array = Arc::new(FixedSizeListArray::new_null(
                                Arc::clone(&f),
                                l,
                                1,
                            ));
                        }
                    }
                }
                let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                arrow::compute::concat(arrays.as_slice())?
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _) => {
                let arrays = scalars.map(|s| s.to_array()).collect::<Result<Vec<_>>>()?;
                let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                arrow::compute::concat(arrays.as_slice())?
            }
            DataType::Dictionary(key_type, value_type) => {
                // create the values array
                let value_scalars = scalars
                    .map(|scalar| match scalar {
                        ScalarValue::Dictionary(inner_key_type, scalar) => {
                            if &inner_key_type == key_type {
                                Ok(*scalar)
                            } else {
                                _exec_err!("Expected inner key type of {key_type} but found: {inner_key_type}, value was ({scalar:?})")
                            }
                        }
                        _ => {
                            _exec_err!(
                                "Expected scalar of type {value_type} but found: {scalar} {scalar:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let values = Self::iter_to_array(value_scalars)?;
                assert_eq!(values.data_type(), value_type.as_ref());

                match key_type.as_ref() {
                    DataType::Int8 => dict_from_values::<Int8Type>(values)?,
                    DataType::Int16 => dict_from_values::<Int16Type>(values)?,
                    DataType::Int32 => dict_from_values::<Int32Type>(values)?,
                    DataType::Int64 => dict_from_values::<Int64Type>(values)?,
                    DataType::UInt8 => dict_from_values::<UInt8Type>(values)?,
                    DataType::UInt16 => dict_from_values::<UInt16Type>(values)?,
                    DataType::UInt32 => dict_from_values::<UInt32Type>(values)?,
                    DataType::UInt64 => dict_from_values::<UInt64Type>(values)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            DataType::FixedSizeBinary(size) => {
                let array = scalars
                    .map(|sv| {
                        if let ScalarValue::FixedSizeBinary(_, v) = sv {
                            Ok(v)
                        } else {
                            _exec_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                Expected {data_type:?}, got {sv:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    array.into_iter(),
                    *size,
                )?;
                Arc::new(array)
            }
            // explicitly enumerate unsupported types so newly added
            // types must be aknowledged, Time32 and Time64 types are
            // not supported if the TimeUnit is not valid (Time32 can
            // only be used with Second and Millisecond, Time64 only
            // with Microsecond and Nanosecond)
            DataType::Time32(TimeUnit::Microsecond)
            | DataType::Time32(TimeUnit::Nanosecond)
            | DataType::Time64(TimeUnit::Second)
            | DataType::Time64(TimeUnit::Millisecond)
            | DataType::RunEndEncoded(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => {
                return _not_impl_err!(
                    "Unsupported creation of {:?} array from ScalarValue {:?}",
                    data_type,
                    scalars.peek()
                );
            }
        };
        Ok(array)
    }

    fn iter_to_null_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<ArrayRef> {
        let length = scalars.into_iter().try_fold(
            0usize,
            |r, element: ScalarValue| match element {
                ScalarValue::Null => Ok::<usize, DataFusionError>(r + 1),
                s => {
                    _internal_err!("Expected ScalarValue::Null element. Received {s:?}")
                }
            },
        )?;
        Ok(new_null_array(&DataType::Null, length))
    }

    fn iter_to_decimal_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal128Array> {
        let array = scalars
            .into_iter()
            .map(|element: ScalarValue| match element {
                ScalarValue::Decimal128(v1, _, _) => Ok(v1),
                s => {
                    _internal_err!("Expected ScalarValue::Null element. Received {s:?}")
                }
            })
            .collect::<Result<Decimal128Array>>()?
            .with_precision_and_scale(precision, scale)?;
        Ok(array)
    }

    fn iter_to_decimal256_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal256Array> {
        let array = scalars
            .into_iter()
            .map(|element: ScalarValue| match element {
                ScalarValue::Decimal256(v1, _, _) => Ok(v1),
                s => {
                    _internal_err!(
                        "Expected ScalarValue::Decimal256 element. Received {s:?}"
                    )
                }
            })
            .collect::<Result<Decimal256Array>>()?
            .with_precision_and_scale(precision, scale)?;
        Ok(array)
    }

    fn build_decimal_array(
        value: Option<i128>,
        precision: u8,
        scale: i8,
        size: usize,
    ) -> Result<Decimal128Array> {
        Ok(match value {
            Some(val) => Decimal128Array::from(vec![val; size])
                .with_precision_and_scale(precision, scale)?,
            None => {
                let mut builder = Decimal128Array::builder(size)
                    .with_precision_and_scale(precision, scale)?;
                builder.append_nulls(size);
                builder.finish()
            }
        })
    }

    fn build_decimal256_array(
        value: Option<i256>,
        precision: u8,
        scale: i8,
        size: usize,
    ) -> Result<Decimal256Array> {
        Ok(repeat_n(value, size)
            .collect::<Decimal256Array>()
            .with_precision_and_scale(precision, scale)?)
    }

    /// Converts `Vec<ScalarValue>` where each element has type corresponding to
    /// `data_type`, to a single element [`ListArray`].
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{ListArray, Int32Array};
    /// use arrow::datatypes::{DataType, Int32Type};
    /// use datafusion_common::cast::as_list_array;
    ///
    /// let scalars = vec![
    ///    ScalarValue::Int32(Some(1)),
    ///    ScalarValue::Int32(None),
    ///    ScalarValue::Int32(Some(2))
    /// ];
    ///
    /// let result = ScalarValue::new_list(&scalars, &DataType::Int32, true);
    ///
    /// let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
    ///     vec![
    ///        Some(vec![Some(1), None, Some(2)])
    ///     ]);
    ///
    /// assert_eq!(*result, expected);
    /// ```
    pub fn new_list(
        values: &[ScalarValue],
        data_type: &DataType,
        nullable: bool,
    ) -> Arc<ListArray> {
        let values = if values.is_empty() {
            new_empty_array(data_type)
        } else {
            Self::iter_to_array(values.iter().cloned()).unwrap()
        };
        Arc::new(
            SingleRowListArrayBuilder::new(values)
                .with_nullable(nullable)
                .build_list_array(),
        )
    }

    /// Same as [`ScalarValue::new_list`] but with nullable set to true.
    pub fn new_list_nullable(
        values: &[ScalarValue],
        data_type: &DataType,
    ) -> Arc<ListArray> {
        Self::new_list(values, data_type, true)
    }

    /// Create ListArray with Null with specific data type
    ///
    /// - new_null_list(i32, nullable, 1): `ListArray[NULL]`
    pub fn new_null_list(data_type: DataType, nullable: bool, null_len: usize) -> Self {
        let data_type = DataType::List(Field::new_list_field(data_type, nullable).into());
        Self::List(Arc::new(ListArray::from(ArrayData::new_null(
            &data_type, null_len,
        ))))
    }

    /// Converts `IntoIterator<Item = ScalarValue>` where each element has type corresponding to
    /// `data_type`, to a [`ListArray`].
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{ListArray, Int32Array};
    /// use arrow::datatypes::{DataType, Int32Type};
    /// use datafusion_common::cast::as_list_array;
    ///
    /// let scalars = vec![
    ///    ScalarValue::Int32(Some(1)),
    ///    ScalarValue::Int32(None),
    ///    ScalarValue::Int32(Some(2))
    /// ];
    ///
    /// let result = ScalarValue::new_list_from_iter(scalars.into_iter(), &DataType::Int32, true);
    ///
    /// let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
    ///     vec![
    ///        Some(vec![Some(1), None, Some(2)])
    ///     ]);
    ///
    /// assert_eq!(*result, expected);
    /// ```
    pub fn new_list_from_iter(
        values: impl IntoIterator<Item = ScalarValue> + ExactSizeIterator,
        data_type: &DataType,
        nullable: bool,
    ) -> Arc<ListArray> {
        let values = if values.len() == 0 {
            new_empty_array(data_type)
        } else {
            Self::iter_to_array(values).unwrap()
        };
        Arc::new(
            SingleRowListArrayBuilder::new(values)
                .with_nullable(nullable)
                .build_list_array(),
        )
    }

    /// Converts `Vec<ScalarValue>` where each element has type corresponding to
    /// `data_type`, to a [`LargeListArray`].
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{LargeListArray, Int32Array};
    /// use arrow::datatypes::{DataType, Int32Type};
    /// use datafusion_common::cast::as_large_list_array;
    ///
    /// let scalars = vec![
    ///    ScalarValue::Int32(Some(1)),
    ///    ScalarValue::Int32(None),
    ///    ScalarValue::Int32(Some(2))
    /// ];
    ///
    /// let result = ScalarValue::new_large_list(&scalars, &DataType::Int32);
    ///
    /// let expected = LargeListArray::from_iter_primitive::<Int32Type, _, _>(
    ///     vec![
    ///        Some(vec![Some(1), None, Some(2)])
    ///     ]);
    ///
    /// assert_eq!(*result, expected);
    /// ```
    pub fn new_large_list(
        values: &[ScalarValue],
        data_type: &DataType,
    ) -> Arc<LargeListArray> {
        let values = if values.is_empty() {
            new_empty_array(data_type)
        } else {
            Self::iter_to_array(values.iter().cloned()).unwrap()
        };
        Arc::new(SingleRowListArrayBuilder::new(values).build_large_list_array())
    }

    /// Converts a scalar value into an array of `size` rows.
    ///
    /// # Errors
    ///
    /// Errors if `self` is
    /// - a decimal that fails be converted to a decimal array of size
    /// - a `FixedsizeList` that fails to be concatenated into an array of size
    /// - a `List` that fails to be concatenated into an array of size
    /// - a `Dictionary` that fails be converted to a dictionary array of size
    pub fn to_array_of_size(&self, size: usize) -> Result<ArrayRef> {
        Ok(match self {
            ScalarValue::Decimal128(e, precision, scale) => Arc::new(
                ScalarValue::build_decimal_array(*e, *precision, *scale, size)?,
            ),
            ScalarValue::Decimal256(e, precision, scale) => Arc::new(
                ScalarValue::build_decimal256_array(*e, *precision, *scale, size)?,
            ),
            ScalarValue::Boolean(e) => {
                Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float64(e) => {
                build_array_from_option!(Float64, Float64Array, e, size)
            }
            ScalarValue::Float32(e) => {
                build_array_from_option!(Float32, Float32Array, e, size)
            }
            ScalarValue::Float16(e) => {
                build_array_from_option!(Float16, Float16Array, e, size)
            }
            ScalarValue::Int8(e) => build_array_from_option!(Int8, Int8Array, e, size),
            ScalarValue::Int16(e) => build_array_from_option!(Int16, Int16Array, e, size),
            ScalarValue::Int32(e) => build_array_from_option!(Int32, Int32Array, e, size),
            ScalarValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            ScalarValue::UInt8(e) => build_array_from_option!(UInt8, UInt8Array, e, size),
            ScalarValue::UInt16(e) => {
                build_array_from_option!(UInt16, UInt16Array, e, size)
            }
            ScalarValue::UInt32(e) => {
                build_array_from_option!(UInt32, UInt32Array, e, size)
            }
            ScalarValue::UInt64(e) => {
                build_array_from_option!(UInt64, UInt64Array, e, size)
            }
            ScalarValue::TimestampSecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Second,
                    tz_opt.clone(),
                    TimestampSecondArray,
                    e,
                    size
                )
            }
            ScalarValue::TimestampMillisecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Millisecond,
                    tz_opt.clone(),
                    TimestampMillisecondArray,
                    e,
                    size
                )
            }

            ScalarValue::TimestampMicrosecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Microsecond,
                    tz_opt.clone(),
                    TimestampMicrosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::TimestampNanosecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Nanosecond,
                    tz_opt.clone(),
                    TimestampNanosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Utf8(e) => match e {
                Some(value) => {
                    Arc::new(StringArray::from_iter_values(repeat_n(value, size)))
                }
                None => new_null_array(&DataType::Utf8, size),
            },
            ScalarValue::Utf8View(e) => match e {
                Some(value) => {
                    Arc::new(StringViewArray::from_iter_values(repeat_n(value, size)))
                }
                None => new_null_array(&DataType::Utf8View, size),
            },
            ScalarValue::LargeUtf8(e) => match e {
                Some(value) => {
                    Arc::new(LargeStringArray::from_iter_values(repeat_n(value, size)))
                }
                None => new_null_array(&DataType::LargeUtf8, size),
            },
            ScalarValue::Binary(e) => match e {
                Some(value) => Arc::new(
                    repeat_n(Some(value.as_slice()), size).collect::<BinaryArray>(),
                ),
                None => Arc::new(repeat_n(None::<&str>, size).collect::<BinaryArray>()),
            },
            ScalarValue::BinaryView(e) => match e {
                Some(value) => Arc::new(
                    repeat_n(Some(value.as_slice()), size).collect::<BinaryViewArray>(),
                ),
                None => {
                    Arc::new(repeat_n(None::<&str>, size).collect::<BinaryViewArray>())
                }
            },
            ScalarValue::FixedSizeBinary(s, e) => match e {
                Some(value) => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat_n(Some(value.as_slice()), size),
                        *s,
                    )
                    .unwrap(),
                ),
                None => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat_n(None::<&[u8]>, size),
                        *s,
                    )
                    .unwrap(),
                ),
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(value) => Arc::new(
                    repeat_n(Some(value.as_slice()), size).collect::<LargeBinaryArray>(),
                ),
                None => {
                    Arc::new(repeat_n(None::<&str>, size).collect::<LargeBinaryArray>())
                }
            },
            ScalarValue::List(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
            ScalarValue::LargeList(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
            ScalarValue::FixedSizeList(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
            ScalarValue::Struct(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
            ScalarValue::Map(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
            ScalarValue::Date32(e) => {
                build_array_from_option!(Date32, Date32Array, e, size)
            }
            ScalarValue::Date64(e) => {
                build_array_from_option!(Date64, Date64Array, e, size)
            }
            ScalarValue::Time32Second(e) => {
                build_array_from_option!(
                    Time32,
                    TimeUnit::Second,
                    Time32SecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time32Millisecond(e) => {
                build_array_from_option!(
                    Time32,
                    TimeUnit::Millisecond,
                    Time32MillisecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time64Microsecond(e) => {
                build_array_from_option!(
                    Time64,
                    TimeUnit::Microsecond,
                    Time64MicrosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time64Nanosecond(e) => {
                build_array_from_option!(
                    Time64,
                    TimeUnit::Nanosecond,
                    Time64NanosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::IntervalDayTime(e) => build_array_from_option!(
                Interval,
                IntervalUnit::DayTime,
                IntervalDayTimeArray,
                e,
                size
            ),
            ScalarValue::IntervalYearMonth(e) => build_array_from_option!(
                Interval,
                IntervalUnit::YearMonth,
                IntervalYearMonthArray,
                e,
                size
            ),
            ScalarValue::IntervalMonthDayNano(e) => build_array_from_option!(
                Interval,
                IntervalUnit::MonthDayNano,
                IntervalMonthDayNanoArray,
                e,
                size
            ),
            ScalarValue::DurationSecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Second,
                DurationSecondArray,
                e,
                size
            ),
            ScalarValue::DurationMillisecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Millisecond,
                DurationMillisecondArray,
                e,
                size
            ),
            ScalarValue::DurationMicrosecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Microsecond,
                DurationMicrosecondArray,
                e,
                size
            ),
            ScalarValue::DurationNanosecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Nanosecond,
                DurationNanosecondArray,
                e,
                size
            ),
            ScalarValue::Union(value, fields, mode) => match value {
                Some((v_id, value)) => {
                    let mut new_fields = Vec::with_capacity(fields.len());
                    let mut child_arrays = Vec::<ArrayRef>::with_capacity(fields.len());
                    for (f_id, field) in fields.iter() {
                        let ar = if f_id == *v_id {
                            value.to_array_of_size(size)?
                        } else {
                            let dt = field.data_type();
                            match mode {
                                UnionMode::Sparse => new_null_array(dt, size),
                                // In a dense union, only the child with values needs to be
                                // allocated
                                UnionMode::Dense => new_null_array(dt, 0),
                            }
                        };
                        let field = (**field).clone();
                        child_arrays.push(ar);
                        new_fields.push(field.clone());
                    }
                    let type_ids = repeat_n(*v_id, size);
                    let type_ids = ScalarBuffer::<i8>::from_iter(type_ids);
                    let value_offsets = match mode {
                        UnionMode::Sparse => None,
                        UnionMode::Dense => Some(ScalarBuffer::from_iter(0..size as i32)),
                    };
                    let ar = UnionArray::try_new(
                        fields.clone(),
                        type_ids,
                        value_offsets,
                        child_arrays,
                    )
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    Arc::new(ar)
                }
                None => {
                    let dt = self.data_type();
                    new_null_array(&dt, size)
                }
            },
            ScalarValue::Dictionary(key_type, v) => {
                // values array is one element long (the value)
                match key_type.as_ref() {
                    DataType::Int8 => dict_from_scalar::<Int8Type>(v, size)?,
                    DataType::Int16 => dict_from_scalar::<Int16Type>(v, size)?,
                    DataType::Int32 => dict_from_scalar::<Int32Type>(v, size)?,
                    DataType::Int64 => dict_from_scalar::<Int64Type>(v, size)?,
                    DataType::UInt8 => dict_from_scalar::<UInt8Type>(v, size)?,
                    DataType::UInt16 => dict_from_scalar::<UInt16Type>(v, size)?,
                    DataType::UInt32 => dict_from_scalar::<UInt32Type>(v, size)?,
                    DataType::UInt64 => dict_from_scalar::<UInt64Type>(v, size)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            ScalarValue::Null => get_or_create_cached_null_array(size),
        })
    }

    fn get_decimal_value_from_array(
        array: &dyn Array,
        index: usize,
        precision: u8,
        scale: i8,
    ) -> Result<ScalarValue> {
        match array.data_type() {
            DataType::Decimal128(_, _) => {
                let array = as_decimal128_array(array)?;
                if array.is_null(index) {
                    Ok(ScalarValue::Decimal128(None, precision, scale))
                } else {
                    let value = array.value(index);
                    Ok(ScalarValue::Decimal128(Some(value), precision, scale))
                }
            }
            DataType::Decimal256(_, _) => {
                let array = as_decimal256_array(array)?;
                if array.is_null(index) {
                    Ok(ScalarValue::Decimal256(None, precision, scale))
                } else {
                    let value = array.value(index);
                    Ok(ScalarValue::Decimal256(Some(value), precision, scale))
                }
            }
            _ => _internal_err!("Unsupported decimal type"),
        }
    }

    fn list_to_array_of_size(arr: &dyn Array, size: usize) -> Result<ArrayRef> {
        let arrays = repeat_n(arr, size).collect::<Vec<_>>();
        let ret = match !arrays.is_empty() {
            true => arrow::compute::concat(arrays.as_slice())?,
            false => arr.slice(0, 0),
        };
        Ok(ret)
    }

    /// Retrieve ScalarValue for each row in `array`
    ///
    /// Example 1: Array (ScalarValue::Int32)
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::ListArray;
    /// use arrow::datatypes::{DataType, Int32Type};
    ///
    /// // Equivalent to [[1,2,3], [4,5]]
    /// let list_arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
    ///    Some(vec![Some(1), Some(2), Some(3)]),
    ///    Some(vec![Some(4), Some(5)])
    /// ]);
    ///
    /// // Convert the array into Scalar Values for each row
    /// let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&list_arr).unwrap();
    ///
    /// let expected = vec![
    /// vec![
    ///     ScalarValue::Int32(Some(1)),
    ///     ScalarValue::Int32(Some(2)),
    ///     ScalarValue::Int32(Some(3)),
    /// ],
    /// vec![
    ///    ScalarValue::Int32(Some(4)),
    ///    ScalarValue::Int32(Some(5)),
    /// ],
    /// ];
    ///
    /// assert_eq!(scalar_vec, expected);
    /// ```
    ///
    /// Example 2: Nested array (ScalarValue::List)
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::ListArray;
    /// use arrow::datatypes::{DataType, Int32Type};
    /// use datafusion_common::utils::SingleRowListArrayBuilder;
    /// use std::sync::Arc;
    ///
    /// let list_arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
    ///    Some(vec![Some(1), Some(2), Some(3)]),
    ///    Some(vec![Some(4), Some(5)])
    /// ]);
    ///
    /// // Wrap into another layer of list, we got nested array as [ [[1,2,3], [4,5]] ]
    /// let list_arr = SingleRowListArrayBuilder::new(Arc::new(list_arr)).build_list_array();
    ///
    /// // Convert the array into Scalar Values for each row, we got 1D arrays in this example
    /// let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&list_arr).unwrap();
    ///
    /// let l1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
    ///     Some(vec![Some(1), Some(2), Some(3)]),
    /// ]);
    /// let l2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
    ///     Some(vec![Some(4), Some(5)]),
    /// ]);
    ///
    /// let expected = vec![
    ///   vec![
    ///     ScalarValue::List(Arc::new(l1)),
    ///     ScalarValue::List(Arc::new(l2)),
    ///   ],
    /// ];
    ///
    /// assert_eq!(scalar_vec, expected);
    /// ```
    pub fn convert_array_to_scalar_vec(array: &dyn Array) -> Result<Vec<Vec<Self>>> {
        let mut scalars = Vec::with_capacity(array.len());

        for index in 0..array.len() {
            let nested_array = array.as_list::<i32>().value(index);
            let scalar_values = (0..nested_array.len())
                .map(|i| ScalarValue::try_from_array(&nested_array, i))
                .collect::<Result<Vec<_>>>()?;
            scalars.push(scalar_values);
        }

        Ok(scalars)
    }

    #[deprecated(
        since = "46.0.0",
        note = "This function is obsolete. Use `to_array` instead"
    )]
    pub fn raw_data(&self) -> Result<ArrayRef> {
        match self {
            ScalarValue::List(arr) => Ok(arr.to_owned()),
            _ => _internal_err!("ScalarValue is not a list"),
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return array.data_type().try_into();
        }

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Decimal128(precision, scale) => {
                ScalarValue::get_decimal_value_from_array(
                    array, index, *precision, *scale,
                )?
            }
            DataType::Decimal256(precision, scale) => {
                ScalarValue::get_decimal_value_from_array(
                    array, index, *precision, *scale,
                )?
            }
            DataType::Boolean => typed_cast!(array, index, as_boolean_array, Boolean)?,
            DataType::Float64 => typed_cast!(array, index, as_float64_array, Float64)?,
            DataType::Float32 => typed_cast!(array, index, as_float32_array, Float32)?,
            DataType::Float16 => typed_cast!(array, index, as_float16_array, Float16)?,
            DataType::UInt64 => typed_cast!(array, index, as_uint64_array, UInt64)?,
            DataType::UInt32 => typed_cast!(array, index, as_uint32_array, UInt32)?,
            DataType::UInt16 => typed_cast!(array, index, as_uint16_array, UInt16)?,
            DataType::UInt8 => typed_cast!(array, index, as_uint8_array, UInt8)?,
            DataType::Int64 => typed_cast!(array, index, as_int64_array, Int64)?,
            DataType::Int32 => typed_cast!(array, index, as_int32_array, Int32)?,
            DataType::Int16 => typed_cast!(array, index, as_int16_array, Int16)?,
            DataType::Int8 => typed_cast!(array, index, as_int8_array, Int8)?,
            DataType::Binary => typed_cast!(array, index, as_binary_array, Binary)?,
            DataType::LargeBinary => {
                typed_cast!(array, index, as_large_binary_array, LargeBinary)?
            }
            DataType::BinaryView => {
                typed_cast!(array, index, as_binary_view_array, BinaryView)?
            }
            DataType::Utf8 => typed_cast!(array, index, as_string_array, Utf8)?,
            DataType::LargeUtf8 => {
                typed_cast!(array, index, as_large_string_array, LargeUtf8)?
            }
            DataType::Utf8View => {
                typed_cast!(array, index, as_string_view_array, Utf8View)?
            }
            DataType::List(field) => {
                let list_array = array.as_list::<i32>();
                let nested_array = list_array.value(index);
                // Produces a single element `ListArray` with the value at `index`.
                SingleRowListArrayBuilder::new(nested_array)
                    .with_field(field)
                    .build_list_scalar()
            }
            DataType::LargeList(field) => {
                let list_array = as_large_list_array(array)?;
                let nested_array = list_array.value(index);
                // Produces a single element `LargeListArray` with the value at `index`.
                SingleRowListArrayBuilder::new(nested_array)
                    .with_field(field)
                    .build_large_list_scalar()
            }
            // TODO: There is no test for FixedSizeList now, add it later
            DataType::FixedSizeList(field, _) => {
                let list_array = as_fixed_size_list_array(array)?;
                let nested_array = list_array.value(index);
                // Produces a single element `FixedSizeListArray` with the value at `index`.
                let list_size = nested_array.len();
                SingleRowListArrayBuilder::new(nested_array)
                    .with_field(field)
                    .build_fixed_size_list_scalar(list_size)
            }
            DataType::Date32 => typed_cast!(array, index, as_date32_array, Date32)?,
            DataType::Date64 => typed_cast!(array, index, as_date64_array, Date64)?,
            DataType::Time32(TimeUnit::Second) => {
                typed_cast!(array, index, as_time32_second_array, Time32Second)?
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                typed_cast!(array, index, as_time32_millisecond_array, Time32Millisecond)?
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                typed_cast!(array, index, as_time64_microsecond_array, Time64Microsecond)?
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                typed_cast!(array, index, as_time64_nanosecond_array, Time64Nanosecond)?
            }
            DataType::Timestamp(TimeUnit::Second, tz_opt) => typed_cast_tz!(
                array,
                index,
                as_timestamp_second_array,
                TimestampSecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                as_timestamp_millisecond_array,
                TimestampMillisecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                as_timestamp_microsecond_array,
                TimestampMicrosecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                as_timestamp_nanosecond_array,
                TimestampNanosecond,
                tz_opt
            )?,
            DataType::Dictionary(key_type, _) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index)?,
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index)?,
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index)?,
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index)?,
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index)?,
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index)?,
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index)?,
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                };
                // look up the index in the values dictionary
                let value = match values_index {
                    Some(values_index) => {
                        ScalarValue::try_from_array(values_array, values_index)
                    }
                    // else entry was null, so return null
                    None => values_array.data_type().try_into(),
                }?;

                Self::Dictionary(key_type.clone(), Box::new(value))
            }
            DataType::Struct(_) => {
                let a = array.slice(index, 1);
                Self::Struct(Arc::new(a.as_struct().to_owned()))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;
                let size = match array.data_type() {
                    DataType::FixedSizeBinary(size) => *size,
                    _ => unreachable!(),
                };
                ScalarValue::FixedSizeBinary(
                    size,
                    match array.is_null(index) {
                        true => None,
                        false => Some(array.value(index).into()),
                    },
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                typed_cast!(array, index, as_interval_dt_array, IntervalDayTime)?
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                typed_cast!(array, index, as_interval_ym_array, IntervalYearMonth)?
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                typed_cast!(array, index, as_interval_mdn_array, IntervalMonthDayNano)?
            }

            DataType::Duration(TimeUnit::Second) => {
                typed_cast!(array, index, as_duration_second_array, DurationSecond)?
            }
            DataType::Duration(TimeUnit::Millisecond) => typed_cast!(
                array,
                index,
                as_duration_millisecond_array,
                DurationMillisecond
            )?,
            DataType::Duration(TimeUnit::Microsecond) => typed_cast!(
                array,
                index,
                as_duration_microsecond_array,
                DurationMicrosecond
            )?,
            DataType::Duration(TimeUnit::Nanosecond) => typed_cast!(
                array,
                index,
                as_duration_nanosecond_array,
                DurationNanosecond
            )?,
            DataType::Map(_, _) => {
                let a = array.slice(index, 1);
                Self::Map(Arc::new(a.as_map().to_owned()))
            }
            DataType::Union(fields, mode) => {
                let array = as_union_array(array)?;
                let ti = array.type_id(index);
                let index = array.value_offset(index);
                let value = ScalarValue::try_from_array(array.child(ti), index)?;
                ScalarValue::Union(Some((ti, Box::new(value))), fields.clone(), *mode)
            }
            other => {
                return _not_impl_err!(
                    "Can't create a scalar from array of type \"{other:?}\""
                );
            }
        })
    }

    /// Try to parse `value` into a ScalarValue of type `target_type`
    pub fn try_from_string(value: String, target_type: &DataType) -> Result<Self> {
        ScalarValue::from(value).cast_to(target_type)
    }

    /// Returns the Some(`&str`) representation of `ScalarValue` of logical string type
    ///
    /// Returns `None` if this `ScalarValue` is not a logical string type or the
    /// `ScalarValue` represents the `NULL` value.
    ///
    /// Note you can use [`Option::flatten`] to check for non null logical
    /// strings.
    ///
    /// For example, [`ScalarValue::Utf8`], [`ScalarValue::LargeUtf8`], and
    /// [`ScalarValue::Dictionary`] with a logical string value and store
    /// strings and can be accessed as `&str` using this method.
    ///
    /// # Example: logical strings
    /// ```
    /// # use datafusion_common::ScalarValue;
    /// /// non strings return None
    /// let scalar = ScalarValue::from(42);
    /// assert_eq!(scalar.try_as_str(), None);
    /// // Non null logical string returns Some(Some(&str))
    /// let scalar = ScalarValue::from("hello");
    /// assert_eq!(scalar.try_as_str(), Some(Some("hello")));
    /// // Null logical string returns Some(None)
    /// let scalar = ScalarValue::Utf8(None);
    /// assert_eq!(scalar.try_as_str(), Some(None));
    /// ```
    ///
    /// # Example: use [`Option::flatten`] to check for non-null logical strings
    /// ```
    /// # use datafusion_common::ScalarValue;
    /// // Non null logical string returns Some(Some(&str))
    /// let scalar = ScalarValue::from("hello");
    /// assert_eq!(scalar.try_as_str().flatten(), Some("hello"));
    /// ```
    pub fn try_as_str(&self) -> Option<Option<&str>> {
        let v = match self {
            ScalarValue::Utf8(v) => v,
            ScalarValue::LargeUtf8(v) => v,
            ScalarValue::Utf8View(v) => v,
            ScalarValue::Dictionary(_, v) => return v.try_as_str(),
            _ => return None,
        };
        Some(v.as_ref().map(|v| v.as_str()))
    }

    /// Try to cast this value to a ScalarValue of type `data_type`
    pub fn cast_to(&self, target_type: &DataType) -> Result<Self> {
        self.cast_to_with_options(target_type, &DEFAULT_CAST_OPTIONS)
    }

    /// Try to cast this value to a ScalarValue of type `data_type` with [`CastOptions`]
    pub fn cast_to_with_options(
        &self,
        target_type: &DataType,
        cast_options: &CastOptions<'static>,
    ) -> Result<Self> {
        let scalar_array = self.to_array()?;
        let cast_arr = cast_with_options(&scalar_array, target_type, cast_options)?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }

    fn eq_array_decimal(
        array: &ArrayRef,
        index: usize,
        value: Option<&i128>,
        precision: u8,
        scale: i8,
    ) -> Result<bool> {
        let array = as_decimal128_array(array)?;
        if array.precision() != precision || array.scale() != scale {
            return Ok(false);
        }
        let is_null = array.is_null(index);
        if let Some(v) = value {
            Ok(!array.is_null(index) && array.value(index) == *v)
        } else {
            Ok(is_null)
        }
    }

    fn eq_array_decimal256(
        array: &ArrayRef,
        index: usize,
        value: Option<&i256>,
        precision: u8,
        scale: i8,
    ) -> Result<bool> {
        let array = as_decimal256_array(array)?;
        if array.precision() != precision || array.scale() != scale {
            return Ok(false);
        }
        let is_null = array.is_null(index);
        if let Some(v) = value {
            Ok(!array.is_null(index) && array.value(index) == *v)
        } else {
            Ok(is_null)
        }
    }

    /// Compares a single row of array @ index for equality with self,
    /// in an optimized fashion.
    ///
    /// This method implements an optimized version of:
    ///
    /// ```text
    ///     let arr_scalar = Self::try_from_array(array, index).unwrap();
    ///     arr_scalar.eq(self)
    /// ```
    ///
    /// *Performance note*: the arrow compute kernels should be
    /// preferred over this function if at all possible as they can be
    /// vectorized and are generally much faster.
    ///
    /// This function has a few narrow use cases such as hash table key
    /// comparisons where comparing a single row at a time is necessary.
    ///
    /// # Errors
    ///
    /// Errors if
    /// - it fails to downcast `array` to the data type of `self`
    /// - `self` is a `Struct`
    ///
    /// # Panics
    ///
    /// Panics if `self` is a dictionary with invalid key type
    #[inline]
    pub fn eq_array(&self, array: &ArrayRef, index: usize) -> Result<bool> {
        Ok(match self {
            ScalarValue::Decimal128(v, precision, scale) => {
                ScalarValue::eq_array_decimal(
                    array,
                    index,
                    v.as_ref(),
                    *precision,
                    *scale,
                )?
            }
            ScalarValue::Decimal256(v, precision, scale) => {
                ScalarValue::eq_array_decimal256(
                    array,
                    index,
                    v.as_ref(),
                    *precision,
                    *scale,
                )?
            }
            ScalarValue::Boolean(val) => {
                eq_array_primitive!(array, index, as_boolean_array, val)?
            }
            ScalarValue::Float16(val) => {
                eq_array_primitive!(array, index, as_float16_array, val)?
            }
            ScalarValue::Float32(val) => {
                eq_array_primitive!(array, index, as_float32_array, val)?
            }
            ScalarValue::Float64(val) => {
                eq_array_primitive!(array, index, as_float64_array, val)?
            }
            ScalarValue::Int8(val) => {
                eq_array_primitive!(array, index, as_int8_array, val)?
            }
            ScalarValue::Int16(val) => {
                eq_array_primitive!(array, index, as_int16_array, val)?
            }
            ScalarValue::Int32(val) => {
                eq_array_primitive!(array, index, as_int32_array, val)?
            }
            ScalarValue::Int64(val) => {
                eq_array_primitive!(array, index, as_int64_array, val)?
            }
            ScalarValue::UInt8(val) => {
                eq_array_primitive!(array, index, as_uint8_array, val)?
            }
            ScalarValue::UInt16(val) => {
                eq_array_primitive!(array, index, as_uint16_array, val)?
            }
            ScalarValue::UInt32(val) => {
                eq_array_primitive!(array, index, as_uint32_array, val)?
            }
            ScalarValue::UInt64(val) => {
                eq_array_primitive!(array, index, as_uint64_array, val)?
            }
            ScalarValue::Utf8(val) => {
                eq_array_primitive!(array, index, as_string_array, val)?
            }
            ScalarValue::Utf8View(val) => {
                eq_array_primitive!(array, index, as_string_view_array, val)?
            }
            ScalarValue::LargeUtf8(val) => {
                eq_array_primitive!(array, index, as_large_string_array, val)?
            }
            ScalarValue::Binary(val) => {
                eq_array_primitive!(array, index, as_binary_array, val)?
            }
            ScalarValue::BinaryView(val) => {
                eq_array_primitive!(array, index, as_binary_view_array, val)?
            }
            ScalarValue::FixedSizeBinary(_, val) => {
                eq_array_primitive!(array, index, as_fixed_size_binary_array, val)?
            }
            ScalarValue::LargeBinary(val) => {
                eq_array_primitive!(array, index, as_large_binary_array, val)?
            }
            ScalarValue::List(arr) => {
                Self::eq_array_list(&(arr.to_owned() as ArrayRef), array, index)
            }
            ScalarValue::LargeList(arr) => {
                Self::eq_array_list(&(arr.to_owned() as ArrayRef), array, index)
            }
            ScalarValue::FixedSizeList(arr) => {
                Self::eq_array_list(&(arr.to_owned() as ArrayRef), array, index)
            }
            ScalarValue::Struct(arr) => {
                Self::eq_array_list(&(arr.to_owned() as ArrayRef), array, index)
            }
            ScalarValue::Map(arr) => {
                Self::eq_array_list(&(arr.to_owned() as ArrayRef), array, index)
            }
            ScalarValue::Date32(val) => {
                eq_array_primitive!(array, index, as_date32_array, val)?
            }
            ScalarValue::Date64(val) => {
                eq_array_primitive!(array, index, as_date64_array, val)?
            }
            ScalarValue::Time32Second(val) => {
                eq_array_primitive!(array, index, as_time32_second_array, val)?
            }
            ScalarValue::Time32Millisecond(val) => {
                eq_array_primitive!(array, index, as_time32_millisecond_array, val)?
            }
            ScalarValue::Time64Microsecond(val) => {
                eq_array_primitive!(array, index, as_time64_microsecond_array, val)?
            }
            ScalarValue::Time64Nanosecond(val) => {
                eq_array_primitive!(array, index, as_time64_nanosecond_array, val)?
            }
            ScalarValue::TimestampSecond(val, _) => {
                eq_array_primitive!(array, index, as_timestamp_second_array, val)?
            }
            ScalarValue::TimestampMillisecond(val, _) => {
                eq_array_primitive!(array, index, as_timestamp_millisecond_array, val)?
            }
            ScalarValue::TimestampMicrosecond(val, _) => {
                eq_array_primitive!(array, index, as_timestamp_microsecond_array, val)?
            }
            ScalarValue::TimestampNanosecond(val, _) => {
                eq_array_primitive!(array, index, as_timestamp_nanosecond_array, val)?
            }
            ScalarValue::IntervalYearMonth(val) => {
                eq_array_primitive!(array, index, as_interval_ym_array, val)?
            }
            ScalarValue::IntervalDayTime(val) => {
                eq_array_primitive!(array, index, as_interval_dt_array, val)?
            }
            ScalarValue::IntervalMonthDayNano(val) => {
                eq_array_primitive!(array, index, as_interval_mdn_array, val)?
            }
            ScalarValue::DurationSecond(val) => {
                eq_array_primitive!(array, index, as_duration_second_array, val)?
            }
            ScalarValue::DurationMillisecond(val) => {
                eq_array_primitive!(array, index, as_duration_millisecond_array, val)?
            }
            ScalarValue::DurationMicrosecond(val) => {
                eq_array_primitive!(array, index, as_duration_microsecond_array, val)?
            }
            ScalarValue::DurationNanosecond(val) => {
                eq_array_primitive!(array, index, as_duration_nanosecond_array, val)?
            }
            ScalarValue::Union(value, _, _) => {
                let array = as_union_array(array)?;
                let ti = array.type_id(index);
                let index = array.value_offset(index);
                if let Some((ti_v, value)) = value {
                    ti_v == &ti && value.eq_array(array.child(ti), index)?
                } else {
                    array.child(ti).is_null(index)
                }
            }
            ScalarValue::Dictionary(key_type, v) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index)?,
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index)?,
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index)?,
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index)?,
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index)?,
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index)?,
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index)?,
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                };
                // was the value in the array non null?
                match values_index {
                    Some(values_index) => v.eq_array(values_array, values_index)?,
                    None => v.is_null(),
                }
            }
            ScalarValue::Null => array.is_null(index),
        })
    }

    fn eq_array_list(arr1: &ArrayRef, arr2: &ArrayRef, index: usize) -> bool {
        let right = arr2.slice(index, 1);
        arr1 == &right
    }

    /// Compare `self` with `other` and return an `Ordering`.
    ///
    /// This is the same as [`PartialOrd`] except that it returns
    /// `Err` if the values cannot be compared, e.g., they have incompatible data types.
    pub fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        self.partial_cmp(other).ok_or_else(|| {
            _internal_datafusion_err!("Uncomparable values: {self:?}, {other:?}")
        })
    }

    /// Estimate size if bytes including `Self`. For values with internal containers such as `String`
    /// includes the allocated size (`capacity`) rather than the current length (`len`)
    pub fn size(&self) -> usize {
        size_of_val(self)
            + match self {
                ScalarValue::Null
                | ScalarValue::Boolean(_)
                | ScalarValue::Float16(_)
                | ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Decimal256(_, _, _)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Date32(_)
                | ScalarValue::Date64(_)
                | ScalarValue::Time32Second(_)
                | ScalarValue::Time32Millisecond(_)
                | ScalarValue::Time64Microsecond(_)
                | ScalarValue::Time64Nanosecond(_)
                | ScalarValue::IntervalYearMonth(_)
                | ScalarValue::IntervalDayTime(_)
                | ScalarValue::IntervalMonthDayNano(_)
                | ScalarValue::DurationSecond(_)
                | ScalarValue::DurationMillisecond(_)
                | ScalarValue::DurationMicrosecond(_)
                | ScalarValue::DurationNanosecond(_) => 0,
                ScalarValue::Utf8(s)
                | ScalarValue::LargeUtf8(s)
                | ScalarValue::Utf8View(s) => {
                    s.as_ref().map(|s| s.capacity()).unwrap_or_default()
                }
                ScalarValue::TimestampSecond(_, s)
                | ScalarValue::TimestampMillisecond(_, s)
                | ScalarValue::TimestampMicrosecond(_, s)
                | ScalarValue::TimestampNanosecond(_, s) => {
                    s.as_ref().map(|s| s.len()).unwrap_or_default()
                }
                ScalarValue::Binary(b)
                | ScalarValue::FixedSizeBinary(_, b)
                | ScalarValue::LargeBinary(b)
                | ScalarValue::BinaryView(b) => {
                    b.as_ref().map(|b| b.capacity()).unwrap_or_default()
                }
                ScalarValue::List(arr) => arr.get_array_memory_size(),
                ScalarValue::LargeList(arr) => arr.get_array_memory_size(),
                ScalarValue::FixedSizeList(arr) => arr.get_array_memory_size(),
                ScalarValue::Struct(arr) => arr.get_array_memory_size(),
                ScalarValue::Map(arr) => arr.get_array_memory_size(),
                ScalarValue::Union(vals, fields, _mode) => {
                    vals.as_ref()
                        .map(|(_id, sv)| sv.size() - size_of_val(sv))
                        .unwrap_or_default()
                        // `fields` is boxed, so it is NOT already included in `self`
                        + size_of_val(fields)
                        + (size_of::<Field>() * fields.len())
                        + fields.iter().map(|(_idx, field)| field.size() - size_of_val(field)).sum::<usize>()
                }
                ScalarValue::Dictionary(dt, sv) => {
                    // `dt` and `sv` are boxed, so they are NOT already included in `self`
                    dt.size() + sv.size()
                }
            }
    }

    /// Estimates [size](Self::size) of [`Vec`] in bytes.
    ///
    /// Includes the size of the [`Vec`] container itself.
    pub fn size_of_vec(vec: &Vec<Self>) -> usize {
        size_of_val(vec)
            + (size_of::<ScalarValue>() * vec.capacity())
            + vec
                .iter()
                .map(|sv| sv.size() - size_of_val(sv))
                .sum::<usize>()
    }

    /// Estimates [size](Self::size) of [`VecDeque`] in bytes.
    ///
    /// Includes the size of the [`VecDeque`] container itself.
    pub fn size_of_vec_deque(vec_deque: &VecDeque<Self>) -> usize {
        size_of_val(vec_deque)
            + (size_of::<ScalarValue>() * vec_deque.capacity())
            + vec_deque
                .iter()
                .map(|sv| sv.size() - size_of_val(sv))
                .sum::<usize>()
    }

    /// Estimates [size](Self::size) of [`HashSet`] in bytes.
    ///
    /// Includes the size of the [`HashSet`] container itself.
    pub fn size_of_hashset<S>(set: &HashSet<Self, S>) -> usize {
        size_of_val(set)
            + (size_of::<ScalarValue>() * set.capacity())
            + set
                .iter()
                .map(|sv| sv.size() - size_of_val(sv))
                .sum::<usize>()
    }

    /// Compacts the allocation referenced by `self` to the minimum, copying the data if
    /// necessary.
    ///
    /// This can be relevant when `self` is a list or contains a list as a nested value, as
    /// a single list holds an Arc to its entire original array buffer.
    pub fn compact(&mut self) {
        match self {
            ScalarValue::Null
            | ScalarValue::Boolean(_)
            | ScalarValue::Float16(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Decimal256(_, _, _)
            | ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Time32Second(_)
            | ScalarValue::Time32Millisecond(_)
            | ScalarValue::Time64Microsecond(_)
            | ScalarValue::Time64Nanosecond(_)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::DurationSecond(_)
            | ScalarValue::DurationMillisecond(_)
            | ScalarValue::DurationMicrosecond(_)
            | ScalarValue::DurationNanosecond(_)
            | ScalarValue::Utf8(_)
            | ScalarValue::LargeUtf8(_)
            | ScalarValue::Utf8View(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::Binary(_)
            | ScalarValue::FixedSizeBinary(_, _)
            | ScalarValue::LargeBinary(_)
            | ScalarValue::BinaryView(_) => (),
            ScalarValue::FixedSizeList(arr) => {
                let array = copy_array_data(&arr.to_data());
                *Arc::make_mut(arr) = FixedSizeListArray::from(array);
            }
            ScalarValue::List(arr) => {
                let array = copy_array_data(&arr.to_data());
                *Arc::make_mut(arr) = ListArray::from(array);
            }
            ScalarValue::LargeList(arr) => {
                let array = copy_array_data(&arr.to_data());
                *Arc::make_mut(arr) = LargeListArray::from(array)
            }
            ScalarValue::Struct(arr) => {
                let array = copy_array_data(&arr.to_data());
                *Arc::make_mut(arr) = StructArray::from(array);
            }
            ScalarValue::Map(arr) => {
                let array = copy_array_data(&arr.to_data());
                *Arc::make_mut(arr) = MapArray::from(array);
            }
            ScalarValue::Union(val, _, _) => {
                if let Some((_, value)) = val.as_mut() {
                    value.compact();
                }
            }
            ScalarValue::Dictionary(_, value) => {
                value.compact();
            }
        }
    }

    /// Compacts ([ScalarValue::compact]) the current [ScalarValue] and returns it.
    pub fn compacted(mut self) -> Self {
        self.compact();
        self
    }

    /// Returns the minimum value for the given numeric `DataType`.
    ///
    /// This function returns the smallest representable value for numeric
    /// and temporal data types. For non-numeric types, it returns `None`.
    ///
    /// # Supported Types
    ///
    /// - **Integer types**: `i8::MIN`, `i16::MIN`, etc.
    /// - **Unsigned types**: Always 0 (`u8::MIN`, `u16::MIN`, etc.)
    /// - **Float types**: Negative infinity (IEEE 754)
    /// - **Decimal types**: Smallest value based on precision
    /// - **Temporal types**: Minimum timestamp/date values
    /// - **Time types**: 0 (midnight)
    /// - **Duration types**: `i64::MIN`
    pub fn min(datatype: &DataType) -> Option<ScalarValue> {
        match datatype {
            DataType::Int8 => Some(ScalarValue::Int8(Some(i8::MIN))),
            DataType::Int16 => Some(ScalarValue::Int16(Some(i16::MIN))),
            DataType::Int32 => Some(ScalarValue::Int32(Some(i32::MIN))),
            DataType::Int64 => Some(ScalarValue::Int64(Some(i64::MIN))),
            DataType::UInt8 => Some(ScalarValue::UInt8(Some(u8::MIN))),
            DataType::UInt16 => Some(ScalarValue::UInt16(Some(u16::MIN))),
            DataType::UInt32 => Some(ScalarValue::UInt32(Some(u32::MIN))),
            DataType::UInt64 => Some(ScalarValue::UInt64(Some(u64::MIN))),
            DataType::Float16 => Some(ScalarValue::Float16(Some(f16::NEG_INFINITY))),
            DataType::Float32 => Some(ScalarValue::Float32(Some(f32::NEG_INFINITY))),
            DataType::Float64 => Some(ScalarValue::Float64(Some(f64::NEG_INFINITY))),
            DataType::Decimal128(precision, scale) => {
                // For decimal, min is -10^(precision-scale) + 10^(-scale)
                // But for simplicity, we use the minimum i128 value that fits the precision
                let max_digits = 10_i128.pow(*precision as u32) - 1;
                Some(ScalarValue::Decimal128(
                    Some(-max_digits),
                    *precision,
                    *scale,
                ))
            }
            DataType::Decimal256(precision, scale) => {
                // Similar to Decimal128 but with i256
                // For now, use a large negative value
                let max_digits = i256::from_i128(10_i128)
                    .checked_pow(*precision as u32)
                    .and_then(|v| v.checked_sub(i256::from_i128(1)))
                    .unwrap_or(i256::MAX);
                Some(ScalarValue::Decimal256(
                    Some(max_digits.neg_wrapping()),
                    *precision,
                    *scale,
                ))
            }
            DataType::Date32 => Some(ScalarValue::Date32(Some(i32::MIN))),
            DataType::Date64 => Some(ScalarValue::Date64(Some(i64::MIN))),
            DataType::Time32(TimeUnit::Second) => {
                Some(ScalarValue::Time32Second(Some(0)))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Some(ScalarValue::Time32Millisecond(Some(0)))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Some(ScalarValue::Time64Microsecond(Some(0)))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Some(ScalarValue::Time64Nanosecond(Some(0)))
            }
            DataType::Timestamp(unit, tz) => match unit {
                TimeUnit::Second => {
                    Some(ScalarValue::TimestampSecond(Some(i64::MIN), tz.clone()))
                }
                TimeUnit::Millisecond => Some(ScalarValue::TimestampMillisecond(
                    Some(i64::MIN),
                    tz.clone(),
                )),
                TimeUnit::Microsecond => Some(ScalarValue::TimestampMicrosecond(
                    Some(i64::MIN),
                    tz.clone(),
                )),
                TimeUnit::Nanosecond => {
                    Some(ScalarValue::TimestampNanosecond(Some(i64::MIN), tz.clone()))
                }
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => Some(ScalarValue::DurationSecond(Some(i64::MIN))),
                TimeUnit::Millisecond => {
                    Some(ScalarValue::DurationMillisecond(Some(i64::MIN)))
                }
                TimeUnit::Microsecond => {
                    Some(ScalarValue::DurationMicrosecond(Some(i64::MIN)))
                }
                TimeUnit::Nanosecond => {
                    Some(ScalarValue::DurationNanosecond(Some(i64::MIN)))
                }
            },
            _ => None,
        }
    }

    /// Returns the maximum value for the given numeric `DataType`.
    ///
    /// This function returns the largest representable value for numeric
    /// and temporal data types. For non-numeric types, it returns `None`.
    ///
    /// # Supported Types
    ///
    /// - **Integer types**: `i8::MAX`, `i16::MAX`, etc.
    /// - **Unsigned types**: `u8::MAX`, `u16::MAX`, etc.
    /// - **Float types**: Positive infinity (IEEE 754)
    /// - **Decimal types**: Largest value based on precision
    /// - **Temporal types**: Maximum timestamp/date values
    /// - **Time types**: Maximum time in the day (1 day - 1 unit)
    /// - **Duration types**: `i64::MAX`
    pub fn max(datatype: &DataType) -> Option<ScalarValue> {
        match datatype {
            DataType::Int8 => Some(ScalarValue::Int8(Some(i8::MAX))),
            DataType::Int16 => Some(ScalarValue::Int16(Some(i16::MAX))),
            DataType::Int32 => Some(ScalarValue::Int32(Some(i32::MAX))),
            DataType::Int64 => Some(ScalarValue::Int64(Some(i64::MAX))),
            DataType::UInt8 => Some(ScalarValue::UInt8(Some(u8::MAX))),
            DataType::UInt16 => Some(ScalarValue::UInt16(Some(u16::MAX))),
            DataType::UInt32 => Some(ScalarValue::UInt32(Some(u32::MAX))),
            DataType::UInt64 => Some(ScalarValue::UInt64(Some(u64::MAX))),
            DataType::Float16 => Some(ScalarValue::Float16(Some(f16::INFINITY))),
            DataType::Float32 => Some(ScalarValue::Float32(Some(f32::INFINITY))),
            DataType::Float64 => Some(ScalarValue::Float64(Some(f64::INFINITY))),
            DataType::Decimal128(precision, scale) => {
                // For decimal, max is 10^(precision-scale) - 10^(-scale)
                // But for simplicity, we use the maximum i128 value that fits the precision
                let max_digits = 10_i128.pow(*precision as u32) - 1;
                Some(ScalarValue::Decimal128(
                    Some(max_digits),
                    *precision,
                    *scale,
                ))
            }
            DataType::Decimal256(precision, scale) => {
                // Similar to Decimal128 but with i256
                let max_digits = i256::from_i128(10_i128)
                    .checked_pow(*precision as u32)
                    .and_then(|v| v.checked_sub(i256::from_i128(1)))
                    .unwrap_or(i256::MAX);
                Some(ScalarValue::Decimal256(
                    Some(max_digits),
                    *precision,
                    *scale,
                ))
            }
            DataType::Date32 => Some(ScalarValue::Date32(Some(i32::MAX))),
            DataType::Date64 => Some(ScalarValue::Date64(Some(i64::MAX))),
            DataType::Time32(TimeUnit::Second) => {
                // 86399 seconds = 23:59:59
                Some(ScalarValue::Time32Second(Some(86_399)))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                // 86_399_999 milliseconds = 23:59:59.999
                Some(ScalarValue::Time32Millisecond(Some(86_399_999)))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                // 86_399_999_999 microseconds = 23:59:59.999999
                Some(ScalarValue::Time64Microsecond(Some(86_399_999_999)))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                // 86_399_999_999_999 nanoseconds = 23:59:59.999999999
                Some(ScalarValue::Time64Nanosecond(Some(86_399_999_999_999)))
            }
            DataType::Timestamp(unit, tz) => match unit {
                TimeUnit::Second => {
                    Some(ScalarValue::TimestampSecond(Some(i64::MAX), tz.clone()))
                }
                TimeUnit::Millisecond => Some(ScalarValue::TimestampMillisecond(
                    Some(i64::MAX),
                    tz.clone(),
                )),
                TimeUnit::Microsecond => Some(ScalarValue::TimestampMicrosecond(
                    Some(i64::MAX),
                    tz.clone(),
                )),
                TimeUnit::Nanosecond => {
                    Some(ScalarValue::TimestampNanosecond(Some(i64::MAX), tz.clone()))
                }
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => Some(ScalarValue::DurationSecond(Some(i64::MAX))),
                TimeUnit::Millisecond => {
                    Some(ScalarValue::DurationMillisecond(Some(i64::MAX)))
                }
                TimeUnit::Microsecond => {
                    Some(ScalarValue::DurationMicrosecond(Some(i64::MAX)))
                }
                TimeUnit::Nanosecond => {
                    Some(ScalarValue::DurationNanosecond(Some(i64::MAX)))
                }
            },
            _ => None,
        }
    }
}

/// Compacts the data of an `ArrayData` into a new `ArrayData`.
///
/// This is useful when you want to minimize the memory footprint of an
/// `ArrayData`. For example, the value returned by [`Array::slice`] still
/// points at the same underlying data buffers as the original array, which may
/// hold many more values. Calling `copy_array_data` on the sliced array will
/// create a new, smaller, `ArrayData` that only contains the data for the
/// sliced array.
///
/// # Example
/// ```
/// # use arrow::array::{make_array, Array, Int32Array};
/// use datafusion_common::scalar::copy_array_data;
/// let array = Int32Array::from_iter_values(0..8192);
/// // Take only the first 2 elements
/// let sliced_array = array.slice(0, 2);
/// // The memory footprint of `sliced_array` is close to 8192 * 4 bytes
/// assert_eq!(32864, sliced_array.get_array_memory_size());
/// // however, we can copy the data to a new `ArrayData`
/// let new_array = make_array(copy_array_data(&sliced_array.into_data()));
/// // The memory footprint of `new_array` is now only 2 * 4 bytes
/// // and overhead:
/// assert_eq!(160, new_array.get_array_memory_size());
/// ```
///
/// See also [`ScalarValue::compact`] which applies to `ScalarValue` instances
/// as necessary.
pub fn copy_array_data(src_data: &ArrayData) -> ArrayData {
    let mut copy = MutableArrayData::new(vec![&src_data], true, src_data.len());
    copy.extend(0, 0, src_data.len());
    copy.freeze()
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Some(value).into()
    }
}

impl From<Option<&str>> for ScalarValue {
    fn from(value: Option<&str>) -> Self {
        let value = value.map(|s| s.to_string());
        ScalarValue::Utf8(value)
    }
}

/// Wrapper to create ScalarValue::Struct for convenience
impl From<Vec<(&str, ScalarValue)>> for ScalarValue {
    fn from(value: Vec<(&str, ScalarValue)>) -> Self {
        value
            .into_iter()
            .fold(ScalarStructBuilder::new(), |builder, (name, value)| {
                builder.with_name_and_scalar(name, value)
            })
            .build()
            .unwrap()
    }
}

impl FromStr for ScalarValue {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        ScalarValue::Utf8(Some(value))
    }
}

macro_rules! impl_try_from {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<ScalarValue> for $NATIVE {
            type Error = DataFusionError;

            fn try_from(value: ScalarValue) -> Result<Self> {
                match value {
                    ScalarValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => _internal_err!(
                        "Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ),
                }
            }
        }
    };
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);

// special implementation for i32 because of Date32 and Time32
impl TryFrom<ScalarValue> for i32 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int32(Some(inner_value))
            | ScalarValue::Date32(Some(inner_value))
            | ScalarValue::Time32Second(Some(inner_value))
            | ScalarValue::Time32Millisecond(Some(inner_value)) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i64 because of Date64, Time64 and Timestamp
impl TryFrom<ScalarValue> for i64 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int64(Some(inner_value))
            | ScalarValue::Date64(Some(inner_value))
            | ScalarValue::Time64Microsecond(Some(inner_value))
            | ScalarValue::Time64Nanosecond(Some(inner_value))
            | ScalarValue::TimestampNanosecond(Some(inner_value), _)
            | ScalarValue::TimestampMicrosecond(Some(inner_value), _)
            | ScalarValue::TimestampMillisecond(Some(inner_value), _)
            | ScalarValue::TimestampSecond(Some(inner_value), _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i128 because of Decimal128
impl TryFrom<ScalarValue> for i128 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Decimal128(Some(inner_value), _, _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i256 because of Decimal128
impl TryFrom<ScalarValue> for i256 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Decimal256(Some(inner_value), _, _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

impl_try_from!(UInt8, u8);
impl_try_from!(UInt16, u16);
impl_try_from!(UInt32, u32);
impl_try_from!(UInt64, u64);
impl_try_from!(Float32, f32);
impl_try_from!(Float64, f64);
impl_try_from!(Boolean, bool);

impl TryFrom<DataType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(datatype: DataType) -> Result<Self> {
        (&datatype).try_into()
    }
}

impl TryFrom<&DataType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(data_type: &DataType) -> Result<Self> {
        Self::try_new_null(data_type)
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{e}"),
            None => write!($F, "NULL"),
        }
    }};
}

// Implement Display trait for ScalarValue
//
// # Panics
//
// Panics if there is an error when creating a visual representation of columns via `arrow::util::pretty`
impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(v, p, s) => {
                write!(f, "{v:?},{p:?},{s:?}")?;
            }
            ScalarValue::Decimal256(v, p, s) => {
                write!(f, "{v:?},{p:?},{s:?}")?;
            }
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float16(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::TimestampSecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampMillisecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampMicrosecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampNanosecond(e, _) => format_option!(f, e)?,
            ScalarValue::Utf8(e)
            | ScalarValue::LargeUtf8(e)
            | ScalarValue::Utf8View(e) => format_option!(f, e)?,
            ScalarValue::Binary(e)
            | ScalarValue::FixedSizeBinary(_, e)
            | ScalarValue::LargeBinary(e)
            | ScalarValue::BinaryView(e) => match e {
                Some(bytes) => {
                    // print up to first 10 bytes, with trailing ... if needed
                    for b in bytes.iter().take(10) {
                        write!(f, "{b:02X}")?;
                    }
                    if bytes.len() > 10 {
                        write!(f, "...")?;
                    }
                }
                None => write!(f, "NULL")?,
            },
            ScalarValue::List(arr) => fmt_list(arr.to_owned() as ArrayRef, f)?,
            ScalarValue::LargeList(arr) => fmt_list(arr.to_owned() as ArrayRef, f)?,
            ScalarValue::FixedSizeList(arr) => fmt_list(arr.to_owned() as ArrayRef, f)?,
            ScalarValue::Date32(e) => format_option!(
                f,
                e.map(|v| {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(Duration::try_days(v as i64).unwrap())
                    {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                })
            )?,
            ScalarValue::Date64(e) => format_option!(
                f,
                e.map(|v| {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(Duration::try_milliseconds(v).unwrap())
                    {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                })
            )?,
            ScalarValue::Time32Second(e) => format_option!(f, e)?,
            ScalarValue::Time32Millisecond(e) => format_option!(f, e)?,
            ScalarValue::Time64Microsecond(e) => format_option!(f, e)?,
            ScalarValue::Time64Nanosecond(e) => format_option!(f, e)?,
            ScalarValue::IntervalYearMonth(e) => format_option!(f, e)?,
            ScalarValue::IntervalMonthDayNano(e) => {
                format_option!(f, e.map(|v| format!("{v:?}")))?
            }
            ScalarValue::IntervalDayTime(e) => {
                format_option!(f, e.map(|v| format!("{v:?}")))?;
            }
            ScalarValue::DurationSecond(e) => format_option!(f, e)?,
            ScalarValue::DurationMillisecond(e) => format_option!(f, e)?,
            ScalarValue::DurationMicrosecond(e) => format_option!(f, e)?,
            ScalarValue::DurationNanosecond(e) => format_option!(f, e)?,
            ScalarValue::Struct(struct_arr) => {
                // ScalarValue Struct should always have a single element
                assert_eq!(struct_arr.len(), 1);

                if struct_arr.null_count() == struct_arr.len() {
                    write!(f, "NULL")?;
                    return Ok(());
                }

                let columns = struct_arr.columns();
                let fields = struct_arr.fields();
                let nulls = struct_arr.nulls();

                write!(
                    f,
                    "{{{}}}",
                    columns
                        .iter()
                        .zip(fields.iter())
                        .map(|(column, field)| {
                            if nulls.is_some_and(|b| b.is_null(0)) {
                                format!("{}:NULL", field.name())
                            } else if let DataType::Struct(_) = field.data_type() {
                                let sv = ScalarValue::Struct(Arc::new(
                                    column.as_struct().to_owned(),
                                ));
                                format!("{}:{sv}", field.name())
                            } else {
                                let sv = array_value_to_string(column, 0).unwrap();
                                format!("{}:{sv}", field.name())
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )?
            }
            ScalarValue::Map(map_arr) => {
                if map_arr.null_count() == map_arr.len() {
                    write!(f, "NULL")?;
                    return Ok(());
                }

                write!(
                    f,
                    "[{}]",
                    map_arr
                        .iter()
                        .map(|struct_array| {
                            if let Some(arr) = struct_array {
                                let mut buffer = VecDeque::new();
                                for i in 0..arr.len() {
                                    let key =
                                        array_value_to_string(arr.column(0), i).unwrap();
                                    let value =
                                        array_value_to_string(arr.column(1), i).unwrap();
                                    buffer.push_back(format!("{key}:{value}"));
                                }
                                format!(
                                    "{{{}}}",
                                    buffer
                                        .into_iter()
                                        .collect::<Vec<_>>()
                                        .join(",")
                                        .as_str()
                                )
                            } else {
                                "NULL".to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )?
            }
            ScalarValue::Union(val, _fields, _mode) => match val {
                Some((id, val)) => write!(f, "{id}:{val}")?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Dictionary(_k, v) => write!(f, "{v}")?,
            ScalarValue::Null => write!(f, "NULL")?,
        };
        Ok(())
    }
}

fn fmt_list(arr: ArrayRef, f: &mut fmt::Formatter) -> fmt::Result {
    // ScalarValue List, LargeList, FixedSizeList should always have a single element
    assert_eq!(arr.len(), 1);
    let options = FormatOptions::default().with_display_error(true);
    let formatter =
        ArrayFormatter::try_new(arr.as_ref() as &dyn Array, &options).unwrap();
    let value_formatter = formatter.value(0);
    write!(f, "{value_formatter}")
}

/// writes a byte array to formatter. `[1, 2, 3]` ==> `"1,2,3"`
fn fmt_binary(data: &[u8], f: &mut fmt::Formatter) -> fmt::Result {
    let mut iter = data.iter();
    if let Some(b) = iter.next() {
        write!(f, "{b}")?;
    }
    for b in iter {
        write!(f, ",{b}")?;
    }
    Ok(())
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(_, _, _) => write!(f, "Decimal128({self})"),
            ScalarValue::Decimal256(_, _, _) => write!(f, "Decimal256({self})"),
            ScalarValue::Boolean(_) => write!(f, "Boolean({self})"),
            ScalarValue::Float16(_) => write!(f, "Float16({self})"),
            ScalarValue::Float32(_) => write!(f, "Float32({self})"),
            ScalarValue::Float64(_) => write!(f, "Float64({self})"),
            ScalarValue::Int8(_) => write!(f, "Int8({self})"),
            ScalarValue::Int16(_) => write!(f, "Int16({self})"),
            ScalarValue::Int32(_) => write!(f, "Int32({self})"),
            ScalarValue::Int64(_) => write!(f, "Int64({self})"),
            ScalarValue::UInt8(_) => write!(f, "UInt8({self})"),
            ScalarValue::UInt16(_) => write!(f, "UInt16({self})"),
            ScalarValue::UInt32(_) => write!(f, "UInt32({self})"),
            ScalarValue::UInt64(_) => write!(f, "UInt64({self})"),
            ScalarValue::TimestampSecond(_, tz_opt) => {
                write!(f, "TimestampSecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                write!(f, "TimestampMillisecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                write!(f, "TimestampMicrosecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                write!(f, "TimestampNanosecond({self}, {tz_opt:?})")
            }
            ScalarValue::Utf8(None) => write!(f, "Utf8({self})"),
            ScalarValue::Utf8(Some(_)) => write!(f, "Utf8(\"{self}\")"),
            ScalarValue::Utf8View(None) => write!(f, "Utf8View({self})"),
            ScalarValue::Utf8View(Some(_)) => write!(f, "Utf8View(\"{self}\")"),
            ScalarValue::LargeUtf8(None) => write!(f, "LargeUtf8({self})"),
            ScalarValue::LargeUtf8(Some(_)) => write!(f, "LargeUtf8(\"{self}\")"),
            ScalarValue::Binary(None) => write!(f, "Binary({self})"),
            ScalarValue::Binary(Some(b)) => {
                write!(f, "Binary(\"")?;
                fmt_binary(b.as_slice(), f)?;
                write!(f, "\")")
            }
            ScalarValue::BinaryView(None) => write!(f, "BinaryView({self})"),
            ScalarValue::BinaryView(Some(b)) => {
                write!(f, "BinaryView(\"")?;
                fmt_binary(b.as_slice(), f)?;
                write!(f, "\")")
            }
            ScalarValue::FixedSizeBinary(size, None) => {
                write!(f, "FixedSizeBinary({size}, {self})")
            }
            ScalarValue::FixedSizeBinary(size, Some(b)) => {
                write!(f, "FixedSizeBinary({size}, \"")?;
                fmt_binary(b.as_slice(), f)?;
                write!(f, "\")")
            }
            ScalarValue::LargeBinary(None) => write!(f, "LargeBinary({self})"),
            ScalarValue::LargeBinary(Some(b)) => {
                write!(f, "LargeBinary(\"")?;
                fmt_binary(b.as_slice(), f)?;
                write!(f, "\")")
            }
            ScalarValue::FixedSizeList(_) => write!(f, "FixedSizeList({self})"),
            ScalarValue::List(_) => write!(f, "List({self})"),
            ScalarValue::LargeList(_) => write!(f, "LargeList({self})"),
            ScalarValue::Struct(struct_arr) => {
                // ScalarValue Struct should always have a single element
                assert_eq!(struct_arr.len(), 1);

                let columns = struct_arr.columns();
                let fields = struct_arr.fields();

                write!(
                    f,
                    "Struct({{{}}})",
                    columns
                        .iter()
                        .zip(fields.iter())
                        .map(|(column, field)| {
                            let sv = array_value_to_string(column, 0).unwrap();
                            let name = field.name();
                            format!("{name}:{sv}")
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            ScalarValue::Map(map_arr) => {
                write!(
                    f,
                    "Map([{}])",
                    map_arr
                        .iter()
                        .map(|struct_array| {
                            if let Some(arr) = struct_array {
                                let buffer: Vec<String> = (0..arr.len())
                                    .map(|i| {
                                        let key = array_value_to_string(arr.column(0), i)
                                            .unwrap();
                                        let value =
                                            array_value_to_string(arr.column(1), i)
                                                .unwrap();
                                        format!("{key:?}:{value:?}")
                                    })
                                    .collect();
                                format!("{{{}}}", buffer.join(","))
                            } else {
                                "NULL".to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            ScalarValue::Date32(_) => write!(f, "Date32(\"{self}\")"),
            ScalarValue::Date64(_) => write!(f, "Date64(\"{self}\")"),
            ScalarValue::Time32Second(_) => write!(f, "Time32Second(\"{self}\")"),
            ScalarValue::Time32Millisecond(_) => {
                write!(f, "Time32Millisecond(\"{self}\")")
            }
            ScalarValue::Time64Microsecond(_) => {
                write!(f, "Time64Microsecond(\"{self}\")")
            }
            ScalarValue::Time64Nanosecond(_) => {
                write!(f, "Time64Nanosecond(\"{self}\")")
            }
            ScalarValue::IntervalDayTime(_) => {
                write!(f, "IntervalDayTime(\"{self}\")")
            }
            ScalarValue::IntervalYearMonth(_) => {
                write!(f, "IntervalYearMonth(\"{self}\")")
            }
            ScalarValue::IntervalMonthDayNano(_) => {
                write!(f, "IntervalMonthDayNano(\"{self}\")")
            }
            ScalarValue::DurationSecond(_) => write!(f, "DurationSecond(\"{self}\")"),
            ScalarValue::DurationMillisecond(_) => {
                write!(f, "DurationMillisecond(\"{self}\")")
            }
            ScalarValue::DurationMicrosecond(_) => {
                write!(f, "DurationMicrosecond(\"{self}\")")
            }
            ScalarValue::DurationNanosecond(_) => {
                write!(f, "DurationNanosecond(\"{self}\")")
            }
            ScalarValue::Union(val, _fields, _mode) => match val {
                Some((id, val)) => write!(f, "Union {id}:{val}"),
                None => write!(f, "Union(NULL)"),
            },
            ScalarValue::Dictionary(k, v) => write!(f, "Dictionary({k:?}, {v:?})"),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

/// Trait used to map a NativeType to a ScalarValue
pub trait ScalarType<T: ArrowNativeType> {
    /// returns a scalar from an optional T
    fn scalar(r: Option<T>) -> ScalarValue;
}

impl ScalarType<f32> for Float32Type {
    fn scalar(r: Option<f32>) -> ScalarValue {
        ScalarValue::Float32(r)
    }
}

impl ScalarType<i64> for TimestampSecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampSecond(r, None)
    }
}

impl ScalarType<i64> for TimestampMillisecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMillisecond(r, None)
    }
}

impl ScalarType<i64> for TimestampMicrosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(r, None)
    }
}

impl ScalarType<i64> for TimestampNanosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampNanosecond(r, None)
    }
}

impl ScalarType<i32> for Date32Type {
    fn scalar(r: Option<i32>) -> ScalarValue {
        ScalarValue::Date32(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::{as_list_array, as_map_array, as_struct_array};
    use crate::test_util::batches_to_string;
    use arrow::array::{
        FixedSizeListBuilder, Int32Builder, LargeListBuilder, ListBuilder, MapBuilder,
        NullArray, NullBufferBuilder, OffsetSizeTrait, PrimitiveBuilder, RecordBatch,
        StringBuilder, StringDictionaryBuilder, StructBuilder, UnionBuilder,
    };
    use arrow::buffer::{Buffer, OffsetBuffer};
    use arrow::compute::{is_null, kernels};
    use arrow::datatypes::{
        ArrowNumericType, Fields, Float64Type, DECIMAL256_MAX_PRECISION,
    };
    use arrow::error::ArrowError;
    use arrow::util::pretty::pretty_format_columns;
    use chrono::NaiveDate;
    use insta::assert_snapshot;
    use rand::Rng;

    #[test]
    fn test_scalar_value_from_for_map() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);
        let mut builder = MapBuilder::new(None, string_builder, int_builder);
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        let expected = builder.finish();

        let sv = ScalarValue::Map(Arc::new(expected.clone()));
        let map_arr = sv.to_array().unwrap();
        let actual = as_map_array(&map_arr).unwrap();
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_scalar_value_from_for_struct() {
        let boolean = Arc::new(BooleanArray::from(vec![false]));
        let int = Arc::new(Int32Array::from(vec![42]));

        let expected = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                Arc::clone(&boolean) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                Arc::clone(&int) as ArrayRef,
            ),
        ]);

        let sv = ScalarStructBuilder::new()
            .with_array(Field::new("b", DataType::Boolean, false), boolean)
            .with_array(Field::new("c", DataType::Int32, false), int)
            .build()
            .unwrap();

        let struct_arr = sv.to_array().unwrap();
        let actual = as_struct_array(&struct_arr).unwrap();
        assert_eq!(actual, &expected);
    }

    #[test]
    #[should_panic(
        expected = "InvalidArgumentError(\"Incorrect array length for StructArray field \\\"bool\\\", expected 1 got 4\")"
    )]
    fn test_scalar_value_from_for_struct_should_panic() {
        let _ = ScalarStructBuilder::new()
            .with_array(
                Field::new("bool", DataType::Boolean, false),
                Arc::new(BooleanArray::from(vec![false, true, false, false])),
            )
            .with_array(
                Field::new("i32", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
            )
            .build()
            .unwrap();
    }

    #[test]
    fn test_to_array_of_size_for_nested() {
        // Struct
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                Arc::clone(&boolean) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                Arc::clone(&int) as ArrayRef,
            ),
        ]);
        let sv = ScalarValue::Struct(Arc::new(struct_array));
        let actual_arr = sv.to_array_of_size(2).unwrap();

        let boolean = Arc::new(BooleanArray::from(vec![
            false, false, true, true, false, false, true, true,
        ]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31, 42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                Arc::clone(&boolean) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                Arc::clone(&int) as ArrayRef,
            ),
        ]);

        let actual = as_struct_array(&actual_arr).unwrap();
        assert_eq!(actual, &struct_array);

        // List
        let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(2),
        ])]);

        let sv = ScalarValue::List(Arc::new(arr));
        let actual_arr = sv
            .to_array_of_size(2)
            .expect("Failed to convert to array of size");
        let actual_list_arr = actual_arr.as_list::<i32>();

        let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(2)]),
            Some(vec![Some(1), None, Some(2)]),
        ]);

        assert_eq!(&arr, actual_list_arr);
    }

    #[test]
    fn test_to_array_of_size_for_fsl() {
        let values = Int32Array::from_iter([Some(1), None, Some(2)]);
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let arr = FixedSizeListArray::new(Arc::clone(&field), 3, Arc::new(values), None);
        let sv = ScalarValue::FixedSizeList(Arc::new(arr));
        let actual_arr = sv
            .to_array_of_size(2)
            .expect("Failed to convert to array of size");

        let expected_values =
            Int32Array::from_iter([Some(1), None, Some(2), Some(1), None, Some(2)]);
        let expected_arr =
            FixedSizeListArray::new(field, 3, Arc::new(expected_values), None);

        assert_eq!(
            &expected_arr,
            as_fixed_size_list_array(actual_arr.as_ref()).unwrap()
        );

        let empty_array = sv
            .to_array_of_size(0)
            .expect("Failed to convert to empty array");

        assert_eq!(empty_array.len(), 0);
    }

    #[test]
    fn test_list_to_array_string() {
        let scalars = vec![
            ScalarValue::from("rust"),
            ScalarValue::from("arrow"),
            ScalarValue::from("data-fusion"),
        ];

        let result = ScalarValue::new_list_nullable(scalars.as_slice(), &DataType::Utf8);

        let expected = single_row_list_array(vec!["rust", "arrow", "data-fusion"]);
        assert_eq!(*result, expected);
    }

    fn single_row_list_array(items: Vec<&str>) -> ListArray {
        SingleRowListArrayBuilder::new(Arc::new(StringArray::from(items)))
            .build_list_array()
    }

    fn build_list<O: OffsetSizeTrait>(
        values: Vec<Option<Vec<Option<i64>>>>,
    ) -> Vec<ScalarValue> {
        values
            .into_iter()
            .map(|v| {
                let arr = if v.is_some() {
                    Arc::new(
                        GenericListArray::<O>::from_iter_primitive::<Int64Type, _, _>(
                            vec![v],
                        ),
                    )
                } else if O::IS_LARGE {
                    new_null_array(
                        &DataType::LargeList(Arc::new(Field::new_list_field(
                            DataType::Int64,
                            true,
                        ))),
                        1,
                    )
                } else {
                    new_null_array(
                        &DataType::List(Arc::new(Field::new_list_field(
                            DataType::Int64,
                            true,
                        ))),
                        1,
                    )
                };

                if O::IS_LARGE {
                    ScalarValue::LargeList(arr.as_list::<i64>().to_owned().into())
                } else {
                    ScalarValue::List(arr.as_list::<i32>().to_owned().into())
                }
            })
            .collect()
    }

    #[test]
    fn test_iter_to_array_fixed_size_list() {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let f1 = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            3,
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        ));
        let f2 = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            3,
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            None,
        ));
        let f_nulls = Arc::new(FixedSizeListArray::new_null(field, 1, 1));

        let scalars = vec![
            ScalarValue::FixedSizeList(Arc::clone(&f_nulls)),
            ScalarValue::FixedSizeList(f1),
            ScalarValue::FixedSizeList(f2),
            ScalarValue::FixedSizeList(f_nulls),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();

        let expected = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                None,
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5), Some(6)]),
                None,
            ],
            3,
        );
        assert_eq!(array.as_ref(), &expected);
    }

    #[test]
    fn test_iter_to_array_struct() {
        let s1 = StructArray::from(vec![
            (
                Arc::new(Field::new("A", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("B", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ),
        ]);

        let s2 = StructArray::from(vec![
            (
                Arc::new(Field::new("A", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("B", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ),
        ]);

        let scalars = vec![
            ScalarValue::Struct(Arc::new(s1)),
            ScalarValue::Struct(Arc::new(s2)),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();

        let expected = StructArray::from(vec![
            (
                Arc::new(Field::new("A", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("B", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![42, 42])) as ArrayRef,
            ),
        ]);
        assert_eq!(array.as_ref(), &expected);
    }

    #[test]
    fn test_iter_to_array_struct_with_nulls() {
        // non-null
        let s1 = StructArray::from((
            vec![
                (
                    Arc::new(Field::new("A", DataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("B", DataType::Int64, false)),
                    Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                ),
            ],
            // Present the null mask, 1 is non-null, 0 is null
            Buffer::from(&[1]),
        ));

        // null
        let s2 = StructArray::from((
            vec![
                (
                    Arc::new(Field::new("A", DataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![3])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("B", DataType::Int64, false)),
                    Arc::new(Int64Array::from(vec![4])) as ArrayRef,
                ),
            ],
            Buffer::from(&[0]),
        ));

        let scalars = vec![
            ScalarValue::Struct(Arc::new(s1)),
            ScalarValue::Struct(Arc::new(s2)),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let struct_array = array.as_struct();
        assert!(struct_array.is_valid(0));
        assert!(struct_array.is_null(1));
    }

    #[test]
    fn iter_to_array_primitive_test() {
        // List[[1,2,3]], List[null], List[[4,5]]
        let scalars = build_list::<i32>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let list_array = as_list_array(&array).unwrap();
        // List[[1,2,3], null, [4,5]]
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(list_array, &expected);

        let scalars = build_list::<i64>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let list_array = as_large_list_array(&array).unwrap();
        let expected = LargeListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(list_array, &expected);
    }

    #[test]
    fn iter_to_array_string_test() {
        let arr1 = single_row_list_array(vec!["foo", "bar", "baz"]);
        let arr2 = single_row_list_array(vec!["rust", "world"]);

        let scalars = vec![
            ScalarValue::List(Arc::new(arr1)),
            ScalarValue::List(Arc::new(arr2)),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let result = array.as_list::<i32>();

        // build expected array
        let string_builder = StringBuilder::with_capacity(5, 25);
        let mut list_of_string_builder = ListBuilder::new(string_builder);

        list_of_string_builder.values().append_value("foo");
        list_of_string_builder.values().append_value("bar");
        list_of_string_builder.values().append_value("baz");
        list_of_string_builder.append(true);

        list_of_string_builder.values().append_value("rust");
        list_of_string_builder.values().append_value("world");
        list_of_string_builder.append(true);
        let expected = list_of_string_builder.finish();

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_list_scalar_eq_to_array() {
        let list_array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![None, Some(5)]),
            ]));

        let fsl_array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
            ]));

        for arr in [list_array, fsl_array] {
            for i in 0..arr.len() {
                let scalar =
                    ScalarValue::List(arr.slice(i, 1).as_list::<i32>().to_owned().into());
                assert!(scalar.eq_array(&arr, i).unwrap());
            }
        }
    }

    #[test]
    fn test_eq_array_err_message() {
        assert_starts_with(
            ScalarValue::Utf8(Some("123".to_string()))
                .eq_array(&(Arc::new(Int32Array::from(vec![123])) as ArrayRef), 0)
                .unwrap_err()
                .message(),
            "could not cast array of type Int32 to arrow_array::array::byte_array::GenericByteArray<arrow_array::types::GenericStringType<i32>>",
        );
    }

    #[test]
    fn scalar_add_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            (float_value.add(&float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        assert_eq!(
            (float_value.add(float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            float_value.sub(&float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        assert_eq!(
            float_value.sub(float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_test() -> Result<()> {
        let int_value = ScalarValue::Int32(Some(42));
        let int_value_2 = ScalarValue::Int32(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int32(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int32(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_overflow_test() {
        let int_value = ScalarValue::Int32(Some(i32::MAX));
        let int_value_2 = ScalarValue::Int32(Some(i32::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 - -2147483648"
        )
    }

    #[test]
    fn scalar_sub_trait_int64_test() -> Result<()> {
        let int_value = ScalarValue::Int64(Some(42));
        let int_value_2 = ScalarValue::Int64(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int64(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int64(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int64_overflow_test() {
        let int_value = ScalarValue::Int64(Some(i64::MAX));
        let int_value_2 = ScalarValue::Int64(Some(i64::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(err, "Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 - -9223372036854775808")
    }

    #[test]
    fn scalar_add_overflow_test() -> Result<()> {
        check_scalar_add_overflow::<Int8Type>(
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(i8::MAX)),
        );
        check_scalar_add_overflow::<UInt8Type>(
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(u8::MAX)),
        );
        check_scalar_add_overflow::<Int16Type>(
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(i16::MAX)),
        );
        check_scalar_add_overflow::<UInt16Type>(
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
        );
        check_scalar_add_overflow::<Int32Type>(
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(i32::MAX)),
        );
        check_scalar_add_overflow::<UInt32Type>(
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
        );
        check_scalar_add_overflow::<Int64Type>(
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(i64::MAX)),
        );
        check_scalar_add_overflow::<UInt64Type>(
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
        );

        Ok(())
    }

    // Verifies that ScalarValue has the same behavior with compute kernel when it overflows.
    fn check_scalar_add_overflow<T>(left: ScalarValue, right: ScalarValue)
    where
        T: ArrowNumericType,
    {
        let scalar_result = left.add_checked(&right);

        let left_array = left.to_array().expect("Failed to convert to array");
        let right_array = right.to_array().expect("Failed to convert to array");
        let arrow_left_array = left_array.as_primitive::<T>();
        let arrow_right_array = right_array.as_primitive::<T>();
        let arrow_result = add(arrow_left_array, arrow_right_array);

        assert_eq!(scalar_result.is_ok(), arrow_result.is_ok());
    }

    #[test]
    fn test_interval_add_timestamp() -> Result<()> {
        let interval = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
            months: 1,
            days: 2,
            nanoseconds: 3,
        }));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalYearMonth(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalDayTime(Some(IntervalDayTime {
            days: 1,
            milliseconds: 23,
        }));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);
        Ok(())
    }

    #[test]
    fn test_try_cmp() {
        assert_eq!(
            ScalarValue::try_cmp(
                &ScalarValue::Int32(Some(1)),
                &ScalarValue::Int32(Some(2))
            )
            .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            ScalarValue::try_cmp(&ScalarValue::Int32(None), &ScalarValue::Int32(Some(2)))
                .unwrap(),
            Ordering::Less
        );
        assert_starts_with(
            ScalarValue::try_cmp(
                &ScalarValue::Int32(Some(1)),
                &ScalarValue::Int64(Some(2)),
            )
            .unwrap_err()
            .message(),
            "Uncomparable values: Int32(1), Int64(2)",
        );
    }

    #[test]
    fn scalar_decimal_test() -> Result<()> {
        let decimal_value = ScalarValue::Decimal128(Some(123), 10, 1);
        assert_eq!(DataType::Decimal128(10, 1), decimal_value.data_type());
        let try_into_value: i128 = decimal_value.clone().try_into().unwrap();
        assert_eq!(123_i128, try_into_value);
        assert!(!decimal_value.is_null());
        let neg_decimal_value = decimal_value.arithmetic_negate()?;
        match neg_decimal_value {
            ScalarValue::Decimal128(v, _, _) => {
                assert_eq!(-123, v.unwrap());
            }
            _ => {
                unreachable!();
            }
        }

        // decimal scalar to array
        let array = decimal_value
            .to_array()
            .expect("Failed to convert to array");
        let array = as_decimal128_array(&array)?;
        assert_eq!(1, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array.value(0));

        // decimal scalar to array with size
        let array = decimal_value
            .to_array_of_size(10)
            .expect("Failed to convert to array of size");
        let array_decimal = as_decimal128_array(&array)?;
        assert_eq!(10, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array_decimal.value(0));
        assert_eq!(123i128, array_decimal.value(9));
        // test eq array
        assert!(decimal_value
            .eq_array(&array, 1)
            .expect("Failed to compare arrays"));
        assert!(decimal_value
            .eq_array(&array, 5)
            .expect("Failed to compare arrays"));
        // test try from array
        assert_eq!(
            decimal_value,
            ScalarValue::try_from_array(&array, 5).unwrap()
        );

        assert_eq!(
            decimal_value,
            ScalarValue::try_new_decimal128(123, 10, 1).unwrap()
        );

        // test compare
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        assert!(!left.eq(&right));
        let result = left < right;
        assert!(result);
        let result = left <= right;
        assert!(result);
        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        // make sure that two decimals with diff datatype can't be compared.
        let result = left.partial_cmp(&right);
        assert_eq!(None, result);

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
        ];
        // convert the vec to decimal array and check the result
        let array = ScalarValue::iter_to_array(decimal_vec).unwrap();
        assert_eq!(3, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
        ];
        let array = ScalarValue::iter_to_array(decimal_vec).unwrap();
        assert_eq!(4, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        assert!(ScalarValue::try_new_decimal128(1, 10, 2)
            .unwrap()
            .eq_array(&array, 0)
            .expect("Failed to compare arrays"));
        assert!(ScalarValue::try_new_decimal128(2, 10, 2)
            .unwrap()
            .eq_array(&array, 1)
            .expect("Failed to compare arrays"));
        assert!(ScalarValue::try_new_decimal128(3, 10, 2)
            .unwrap()
            .eq_array(&array, 2)
            .expect("Failed to compare arrays"));
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 3).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_new_one_decimal128() {
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal128(5, 0)).unwrap(),
            ScalarValue::Decimal128(Some(1), 5, 0)
        );
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal128(5, 1)).unwrap(),
            ScalarValue::Decimal128(Some(10), 5, 1)
        );
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal128(5, 2)).unwrap(),
            ScalarValue::Decimal128(Some(100), 5, 2)
        );
        // More precision
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal128(7, 2)).unwrap(),
            ScalarValue::Decimal128(Some(100), 7, 2)
        );
        // No negative scale
        assert!(ScalarValue::new_one(&DataType::Decimal128(5, -1)).is_err());
        // Invalid combination
        assert!(ScalarValue::new_one(&DataType::Decimal128(0, 2)).is_err());
        assert!(ScalarValue::new_one(&DataType::Decimal128(5, 7)).is_err());
    }

    #[test]
    fn test_new_one_decimal256() {
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal256(5, 0)).unwrap(),
            ScalarValue::Decimal256(Some(1.into()), 5, 0)
        );
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal256(5, 1)).unwrap(),
            ScalarValue::Decimal256(Some(10.into()), 5, 1)
        );
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal256(5, 2)).unwrap(),
            ScalarValue::Decimal256(Some(100.into()), 5, 2)
        );
        // More precision
        assert_eq!(
            ScalarValue::new_one(&DataType::Decimal256(7, 2)).unwrap(),
            ScalarValue::Decimal256(Some(100.into()), 7, 2)
        );
        // No negative scale
        assert!(ScalarValue::new_one(&DataType::Decimal256(5, -1)).is_err());
        // Invalid combination
        assert!(ScalarValue::new_one(&DataType::Decimal256(0, 2)).is_err());
        assert!(ScalarValue::new_one(&DataType::Decimal256(5, 7)).is_err());
    }

    #[test]
    fn test_new_ten_decimal128() {
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal128(5, 1)).unwrap(),
            ScalarValue::Decimal128(Some(100), 5, 1)
        );
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal128(5, 2)).unwrap(),
            ScalarValue::Decimal128(Some(1000), 5, 2)
        );
        // More precision
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal128(7, 2)).unwrap(),
            ScalarValue::Decimal128(Some(1000), 7, 2)
        );
        // No negative or zero scale
        assert!(ScalarValue::new_ten(&DataType::Decimal128(5, 0)).is_err());
        assert!(ScalarValue::new_ten(&DataType::Decimal128(5, -1)).is_err());
        // Invalid combination
        assert!(ScalarValue::new_ten(&DataType::Decimal128(0, 2)).is_err());
        assert!(ScalarValue::new_ten(&DataType::Decimal128(5, 7)).is_err());
    }

    #[test]
    fn test_new_ten_decimal256() {
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal256(5, 1)).unwrap(),
            ScalarValue::Decimal256(Some(100.into()), 5, 1)
        );
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal256(5, 2)).unwrap(),
            ScalarValue::Decimal256(Some(1000.into()), 5, 2)
        );
        // More precision
        assert_eq!(
            ScalarValue::new_ten(&DataType::Decimal256(7, 2)).unwrap(),
            ScalarValue::Decimal256(Some(1000.into()), 7, 2)
        );
        // No negative or zero scale
        assert!(ScalarValue::new_ten(&DataType::Decimal256(5, 0)).is_err());
        assert!(ScalarValue::new_ten(&DataType::Decimal256(5, -1)).is_err());
        // Invalid combination
        assert!(ScalarValue::new_ten(&DataType::Decimal256(0, 2)).is_err());
        assert!(ScalarValue::new_ten(&DataType::Decimal256(5, 7)).is_err());
    }

    #[test]
    fn test_new_negative_one_decimal128() {
        assert_eq!(
            ScalarValue::new_negative_one(&DataType::Decimal128(5, 0)).unwrap(),
            ScalarValue::Decimal128(Some(-1), 5, 0)
        );
        assert_eq!(
            ScalarValue::new_negative_one(&DataType::Decimal128(5, 2)).unwrap(),
            ScalarValue::Decimal128(Some(-100), 5, 2)
        );
    }

    #[test]
    fn test_list_partial_cmp() {
        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(30),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(30),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(2),
                    Some(3),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(2),
                    Some(3),
                    Some(4),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    None,
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a = ScalarValue::LargeList(Arc::new(LargeListArray::from_iter_primitive::<
            Int64Type,
            _,
            _,
        >(vec![Some(vec![
            None,
            Some(2),
            Some(3),
        ])])));
        let b = ScalarValue::LargeList(Arc::new(LargeListArray::from_iter_primitive::<
            Int64Type,
            _,
            _,
        >(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])])));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a = ScalarValue::FixedSizeList(Arc::new(
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
                vec![Some(vec![None, Some(2), Some(3)])],
                3,
            ),
        ));
        let b = ScalarValue::FixedSizeList(Arc::new(
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
                vec![Some(vec![Some(1), Some(2), Some(3)])],
                3,
            ),
        ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));
    }

    #[test]
    fn scalar_value_to_array_u64() -> Result<()> {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_value_to_array_u32() -> Result<()> {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array = ScalarValue::new_list_nullable(&[], &DataType::UInt64);

        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_large_list_null_to_array() {
        let list_array = ScalarValue::new_large_list(&[], &DataType::UInt64);

        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() -> Result<()> {
        let values = vec![
            ScalarValue::UInt64(Some(100)),
            ScalarValue::UInt64(None),
            ScalarValue::UInt64(Some(101)),
        ];
        let list_array = ScalarValue::new_list_nullable(&values, &DataType::UInt64);
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = as_uint64_array(&prim_array_ref)?;
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
        Ok(())
    }

    #[test]
    fn scalar_large_list_to_array() -> Result<()> {
        let values = vec![
            ScalarValue::UInt64(Some(100)),
            ScalarValue::UInt64(None),
            ScalarValue::UInt64(Some(101)),
        ];
        let list_array = ScalarValue::new_large_list(&values, &DataType::UInt64);
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = as_uint64_array(&prim_array_ref)?;
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
        Ok(())
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    macro_rules! check_scalar_iter {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> =
                $INPUT.iter().map(|v| ScalarValue::$SCALAR_T(*v)).collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    /// but for variants that carry a timezone field.
    macro_rules! check_scalar_iter_tz {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(*v, None))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for string  arrays
    macro_rules! check_scalar_iter_string {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_string())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for binary arrays
    macro_rules! check_scalar_iter_binary {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_vec())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: $ARRAYTYPE =
                $INPUT.iter().map(|v| v.map(|v| v.to_vec())).collect();

            let expected: ArrayRef = Arc::new(expected);

            assert_eq!(&array, &expected);
        }};
    }

    #[test]
    // despite clippy claiming they are useless, the code doesn't compile otherwise.
    #[allow(clippy::useless_vec)]
    fn scalar_iter_to_array_boolean() {
        check_scalar_iter!(Boolean, BooleanArray, vec![Some(true), None, Some(false)]);
        check_scalar_iter!(Float32, Float32Array, vec![Some(1.9), None, Some(-2.1)]);
        check_scalar_iter!(Float64, Float64Array, vec![Some(1.9), None, Some(-2.1)]);

        check_scalar_iter!(Int8, Int8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int16, Int16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int32, Int32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int64, Int64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter!(UInt8, UInt8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt16, UInt16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt32, UInt32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt64, UInt64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter_tz!(
            TimestampSecond,
            TimestampSecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampMillisecond,
            TimestampMillisecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampMicrosecond,
            TimestampMicrosecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampNanosecond,
            TimestampNanosecondArray,
            vec![Some(1), None, Some(3)]
        );

        check_scalar_iter_string!(
            Utf8,
            StringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_string!(
            LargeUtf8,
            LargeStringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_binary!(
            Binary,
            BinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
        check_scalar_iter_binary!(
            LargeBinary,
            LargeBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
    }

    #[test]
    fn scalar_iter_to_array_empty() {
        let scalars = vec![] as Vec<ScalarValue>;

        let result = ScalarValue::iter_to_array(scalars).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("Empty iterator passed to ScalarValue::iter_to_array"),
            "{}",
            result
        );
    }

    #[test]
    fn scalar_iter_to_dictionary() {
        fn make_val(v: Option<String>) -> ScalarValue {
            let key_type = DataType::Int32;
            let value = ScalarValue::Utf8(v);
            ScalarValue::Dictionary(Box::new(key_type), Box::new(value))
        }

        let scalars = [
            make_val(Some("Foo".into())),
            make_val(None),
            make_val(Some("Bar".into())),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let array = as_dictionary_array::<Int32Type>(&array).unwrap();
        let values_array = as_string_array(array.values()).unwrap();

        let values = array
            .keys_iter()
            .map(|k| {
                k.map(|k| {
                    assert!(values_array.is_valid(k));
                    values_array.value(k)
                })
            })
            .collect::<Vec<_>>();

        let expected = vec![Some("Foo"), None, Some("Bar")];
        assert_eq!(values, expected);
    }

    #[test]
    fn scalar_iter_to_array_mismatched_types() {
        use ScalarValue::*;
        // If the scalar values are not all the correct type, error here
        let scalars = [Boolean(Some(true)), Int32(Some(5))];

        let result = ScalarValue::iter_to_array(scalars).unwrap_err();
        assert!(result.to_string().contains("Inconsistent types in ScalarValue::iter_to_array. Expected Boolean, got Int32(5)"),
                "{}", result);
    }

    #[test]
    fn scalar_try_from_array_null() {
        let array = vec![Some(33), None].into_iter().collect::<Int64Array>();
        let array: ArrayRef = Arc::new(array);

        assert_eq!(
            ScalarValue::Int64(Some(33)),
            ScalarValue::try_from_array(&array, 0).unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            ScalarValue::try_from_array(&array, 1).unwrap()
        );
    }

    #[test]
    fn scalar_try_from_array_list_array_null() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);

        let non_null_list_scalar = ScalarValue::try_from_array(&list, 0).unwrap();
        let null_list_scalar = ScalarValue::try_from_array(&list, 1).unwrap();

        let data_type =
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));

        assert_eq!(non_null_list_scalar.data_type(), data_type);
        assert_eq!(null_list_scalar.data_type(), data_type);
    }

    #[test]
    fn scalar_try_from_list_datatypes() {
        let inner_field = Arc::new(Field::new_list_field(DataType::Int32, true));

        // Test for List
        let data_type = &DataType::List(Arc::clone(&inner_field));
        let scalar: ScalarValue = data_type.try_into().unwrap();
        let expected = ScalarValue::List(
            new_null_array(data_type, 1)
                .as_list::<i32>()
                .to_owned()
                .into(),
        );
        assert_eq!(expected, scalar);
        assert!(expected.is_null());

        // Test for LargeList
        let data_type = &DataType::LargeList(Arc::clone(&inner_field));
        let scalar: ScalarValue = data_type.try_into().unwrap();
        let expected = ScalarValue::LargeList(
            new_null_array(data_type, 1)
                .as_list::<i64>()
                .to_owned()
                .into(),
        );
        assert_eq!(expected, scalar);
        assert!(expected.is_null());

        // Test for FixedSizeList(5)
        let data_type = &DataType::FixedSizeList(Arc::clone(&inner_field), 5);
        let scalar: ScalarValue = data_type.try_into().unwrap();
        let expected = ScalarValue::FixedSizeList(
            new_null_array(data_type, 1)
                .as_fixed_size_list()
                .to_owned()
                .into(),
        );
        assert_eq!(expected, scalar);
        assert!(expected.is_null());
    }

    #[test]
    fn scalar_try_from_list_of_list() {
        let data_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        )));
        let data_type = &data_type;
        let scalar: ScalarValue = data_type.try_into().unwrap();

        let expected = ScalarValue::List(
            new_null_array(
                &DataType::List(Arc::new(Field::new_list_field(
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Int32,
                        true,
                    ))),
                    true,
                ))),
                1,
            )
            .as_list::<i32>()
            .to_owned()
            .into(),
        );

        assert_eq!(expected, scalar)
    }

    #[test]
    fn scalar_try_from_not_equal_list_nested_list() {
        let list_data_type =
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let data_type = &list_data_type;
        let list_scalar: ScalarValue = data_type.try_into().unwrap();

        let nested_list_data_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        )));
        let data_type = &nested_list_data_type;
        let nested_list_scalar: ScalarValue = data_type.try_into().unwrap();

        assert_ne!(list_scalar, nested_list_scalar);
    }

    #[test]
    fn scalar_try_from_dict_datatype() {
        let data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let data_type = &data_type;
        let expected = ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Utf8(None)),
        );
        assert_eq!(expected, data_type.try_into().unwrap())
    }

    #[test]
    fn size_of_scalar() {
        // Since ScalarValues are used in a non trivial number of places,
        // making it larger means significant more memory consumption
        // per distinct value.
        //
        // Thus this test ensures that no code change makes ScalarValue larger
        //
        // The alignment requirements differ across architectures and
        // thus the size of the enum appears to as well

        // The value may also change depending on rust version
        assert_eq!(size_of::<ScalarValue>(), 64);
    }

    #[test]
    fn memory_size() {
        let sv = ScalarValue::Binary(Some(Vec::with_capacity(10)));
        assert_eq!(sv.size(), size_of::<ScalarValue>() + 10,);
        let sv_size = sv.size();

        let mut v = Vec::with_capacity(10);
        // do NOT clone `sv` here because this may shrink the vector capacity
        v.push(sv);
        assert_eq!(v.capacity(), 10);
        assert_eq!(
            ScalarValue::size_of_vec(&v),
            size_of::<Vec<ScalarValue>>() + (9 * size_of::<ScalarValue>()) + sv_size,
        );

        let mut s = HashSet::with_capacity(0);
        // do NOT clone `sv` here because this may shrink the vector capacity
        s.insert(v.pop().unwrap());
        // hashsets may easily grow during insert, so capacity is dynamic
        let s_capacity = s.capacity();
        assert_eq!(
            ScalarValue::size_of_hashset(&s),
            size_of::<HashSet<ScalarValue>>()
                + ((s_capacity - 1) * size_of::<ScalarValue>())
                + sv_size,
        );
    }

    #[test]
    fn scalar_eq_array() {
        // Validate that eq_array has the same semantics as ScalarValue::eq
        macro_rules! make_typed_vec {
            ($INPUT:expr, $TYPE:ident) => {{
                $INPUT
                    .iter()
                    .map(|v| v.map(|v| v as $TYPE))
                    .collect::<Vec<_>>()
            }};
        }

        let bool_vals = [Some(true), None, Some(false)];
        let f32_vals = [Some(-1.0), None, Some(1.0)];
        let f64_vals = make_typed_vec!(f32_vals, f64);

        let i8_vals = [Some(-1), None, Some(1)];
        let i16_vals = make_typed_vec!(i8_vals, i16);
        let i32_vals = make_typed_vec!(i8_vals, i32);
        let i64_vals = make_typed_vec!(i8_vals, i64);

        let u8_vals = [Some(0), None, Some(1)];
        let u16_vals = make_typed_vec!(u8_vals, u16);
        let u32_vals = make_typed_vec!(u8_vals, u32);
        let u64_vals = make_typed_vec!(u8_vals, u64);

        let str_vals = [Some("foo"), None, Some("bar")];

        let interval_dt_vals = [
            Some(IntervalDayTime::MINUS_ONE),
            None,
            Some(IntervalDayTime::ONE),
        ];
        let interval_mdn_vals = [
            Some(IntervalMonthDayNano::MINUS_ONE),
            None,
            Some(IntervalMonthDayNano::ONE),
        ];

        /// Test each value in `scalar` with the corresponding element
        /// at `array`. Assumes each element is unique (aka not equal
        /// with all other indexes)
        #[derive(Debug)]
        struct TestCase {
            array: ArrayRef,
            scalars: Vec<ScalarValue>,
        }

        /// Create a test case for casing the input to the specified array type
        macro_rules! make_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};

            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                let tz = $TZ;
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(*v, tz.clone()))
                        .collect(),
                }
            }};
        }

        macro_rules! make_str_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.to_string())))
                        .collect(),
                }
            }};
        }

        macro_rules! make_binary_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::$SCALAR_TY(v.map(|v| v.as_bytes().to_vec()))
                        })
                        .collect(),
                }
            }};
        }

        /// create a test case for DictionaryArray<$INDEX_TY>
        macro_rules! make_str_dict_test_case {
            ($INPUT:expr, $INDEX_TY:ident) => {{
                TestCase {
                    array: Arc::new(
                        $INPUT
                            .iter()
                            .cloned()
                            .collect::<DictionaryArray<$INDEX_TY>>(),
                    ),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::Dictionary(
                                Box::new($INDEX_TY::DATA_TYPE),
                                Box::new(ScalarValue::Utf8(v.map(|v| v.to_string()))),
                            )
                        })
                        .collect(),
                }
            }};
        }

        let cases = vec![
            make_test_case!(bool_vals, BooleanArray, Boolean),
            make_test_case!(f32_vals, Float32Array, Float32),
            make_test_case!(f64_vals, Float64Array, Float64),
            make_test_case!(i8_vals, Int8Array, Int8),
            make_test_case!(i16_vals, Int16Array, Int16),
            make_test_case!(i32_vals, Int32Array, Int32),
            make_test_case!(i64_vals, Int64Array, Int64),
            make_test_case!(u8_vals, UInt8Array, UInt8),
            make_test_case!(u16_vals, UInt16Array, UInt16),
            make_test_case!(u32_vals, UInt32Array, UInt32),
            make_test_case!(u64_vals, UInt64Array, UInt64),
            make_str_test_case!(str_vals, StringArray, Utf8),
            make_str_test_case!(str_vals, LargeStringArray, LargeUtf8),
            make_binary_test_case!(str_vals, BinaryArray, Binary),
            make_binary_test_case!(str_vals, LargeBinaryArray, LargeBinary),
            make_test_case!(i32_vals, Date32Array, Date32),
            make_test_case!(i64_vals, Date64Array, Date64),
            make_test_case!(i32_vals, Time32SecondArray, Time32Second),
            make_test_case!(i32_vals, Time32MillisecondArray, Time32Millisecond),
            make_test_case!(i64_vals, Time64MicrosecondArray, Time64Microsecond),
            make_test_case!(i64_vals, Time64NanosecondArray, Time64Nanosecond),
            make_test_case!(i64_vals, TimestampSecondArray, TimestampSecond, None),
            make_test_case!(
                i64_vals,
                TimestampSecondArray,
                TimestampSecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                Some("UTC".into())
            ),
            make_test_case!(i32_vals, IntervalYearMonthArray, IntervalYearMonth),
            make_test_case!(interval_dt_vals, IntervalDayTimeArray, IntervalDayTime),
            make_test_case!(
                interval_mdn_vals,
                IntervalMonthDayNanoArray,
                IntervalMonthDayNano
            ),
            make_str_dict_test_case!(str_vals, Int8Type),
            make_str_dict_test_case!(str_vals, Int16Type),
            make_str_dict_test_case!(str_vals, Int32Type),
            make_str_dict_test_case!(str_vals, Int64Type),
            make_str_dict_test_case!(str_vals, UInt8Type),
            make_str_dict_test_case!(str_vals, UInt16Type),
            make_str_dict_test_case!(str_vals, UInt32Type),
            make_str_dict_test_case!(str_vals, UInt64Type),
        ];

        for case in cases {
            println!("**** Test Case *****");
            let TestCase { array, scalars } = case;
            println!("Input array type: {}", array.data_type());
            println!("Input scalars: {scalars:#?}");
            assert_eq!(array.len(), scalars.len());

            for (index, scalar) in scalars.into_iter().enumerate() {
                assert!(
                    scalar
                        .eq_array(&array, index)
                        .expect("Failed to compare arrays"),
                    "Expected {scalar:?} to be equal to {array:?} at index {index}"
                );

                // test that all other elements are *not* equal
                for other_index in 0..array.len() {
                    if index != other_index {
                        assert!(
                            !scalar.eq_array(&array, other_index).expect("Failed to compare arrays"),
                            "Expected {scalar:?} to be NOT equal to {array:?} at index {other_index}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_partial_ordering() {
        use ScalarValue::*;

        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(0))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Int64(Some(0)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Less)
        );
        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Equal)
        );
        // For different data type, `partial_cmp` returns None.
        assert_eq!(Int64(Some(33)).partial_cmp(&Int32(Some(33))), None);
        assert_eq!(Int32(Some(33)).partial_cmp(&Int64(Some(33))), None);

        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("A", ScalarValue::from(2.0)),
                ("B", ScalarValue::from("A")),
            ])),
            Some(Ordering::Less)
        );

        // For different struct fields, `partial_cmp` returns None.
        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("a", ScalarValue::from(2.0)),
                ("b", ScalarValue::from("A")),
            ])),
            None
        );
    }

    #[test]
    fn test_scalar_value_from_string() {
        let scalar = ScalarValue::from("foo");
        assert_eq!(scalar, ScalarValue::Utf8(Some("foo".to_string())));
        let scalar = ScalarValue::from("foo".to_string());
        assert_eq!(scalar, ScalarValue::Utf8(Some("foo".to_string())));
        let scalar = ScalarValue::from_str("foo").unwrap();
        assert_eq!(scalar, ScalarValue::Utf8(Some("foo".to_string())));
    }

    #[test]
    fn test_scalar_struct() {
        let field_a = Arc::new(Field::new("A", DataType::Int32, false));
        let field_b = Arc::new(Field::new("B", DataType::Boolean, false));
        let field_c = Arc::new(Field::new("C", DataType::Utf8, false));

        let field_e = Arc::new(Field::new("e", DataType::Int16, false));
        let field_f = Arc::new(Field::new("f", DataType::Int64, false));
        let field_d = Arc::new(Field::new(
            "D",
            DataType::Struct(vec![Arc::clone(&field_e), Arc::clone(&field_f)].into()),
            false,
        ));

        let struct_array = StructArray::from(vec![
            (
                Arc::clone(&field_e),
                Arc::new(Int16Array::from(vec![2])) as ArrayRef,
            ),
            (
                Arc::clone(&field_f),
                Arc::new(Int64Array::from(vec![3])) as ArrayRef,
            ),
        ]);

        let struct_array = StructArray::from(vec![
            (
                Arc::clone(&field_a),
                Arc::new(Int32Array::from(vec![23])) as ArrayRef,
            ),
            (
                Arc::clone(&field_b),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                Arc::clone(&field_c),
                Arc::new(StringArray::from(vec!["Hello"])) as ArrayRef,
            ),
            (Arc::clone(&field_d), Arc::new(struct_array) as ArrayRef),
        ]);
        let scalar = ScalarValue::Struct(Arc::new(struct_array));

        let array = scalar
            .to_array_of_size(2)
            .expect("Failed to convert to array of size");

        let expected = Arc::new(StructArray::from(vec![
            (
                Arc::clone(&field_a),
                Arc::new(Int32Array::from(vec![23, 23])) as ArrayRef,
            ),
            (
                Arc::clone(&field_b),
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ),
            (
                Arc::clone(&field_c),
                Arc::new(StringArray::from(vec!["Hello", "Hello"])) as ArrayRef,
            ),
            (
                Arc::clone(&field_d),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::clone(&field_e),
                        Arc::new(Int16Array::from(vec![2, 2])) as ArrayRef,
                    ),
                    (
                        Arc::clone(&field_f),
                        Arc::new(Int64Array::from(vec![3, 3])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);

        // Construct from second element of ArrayRef
        let constructed = ScalarValue::try_from_array(&expected, 1).unwrap();
        assert_eq!(constructed, scalar);

        // None version
        let none_scalar = ScalarValue::try_from(array.data_type()).unwrap();
        assert!(none_scalar.is_null());
        assert_eq!(
            format!("{none_scalar:?}"),
            String::from("Struct({A:,B:,C:,D:})")
        );

        // Construct with convenience From<Vec<(&str, ScalarValue)>>
        let constructed = ScalarValue::from(vec![
            ("A", ScalarValue::from(23)),
            ("B", ScalarValue::from(false)),
            ("C", ScalarValue::from("Hello")),
            (
                "D",
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ),
        ]);
        assert_eq!(constructed, scalar);

        // Build Array from Vec of structs
        let scalars = vec![
            ScalarValue::from(vec![
                ("A", ScalarValue::from(23)),
                ("B", ScalarValue::from(false)),
                ("C", ScalarValue::from("Hello")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(2i16)),
                        ("f", ScalarValue::from(3i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(7)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("World")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(4i16)),
                        ("f", ScalarValue::from(5i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(-1000)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("!!!!!")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(6i16)),
                        ("f", ScalarValue::from(7i64)),
                    ]),
                ),
            ]),
        ];
        let array = ScalarValue::iter_to_array(scalars).unwrap();

        let expected = Arc::new(StructArray::from(vec![
            (
                Arc::clone(&field_a),
                Arc::new(Int32Array::from(vec![23, 7, -1000])) as ArrayRef,
            ),
            (
                Arc::clone(&field_b),
                Arc::new(BooleanArray::from(vec![false, true, true])) as ArrayRef,
            ),
            (
                Arc::clone(&field_c),
                Arc::new(StringArray::from(vec!["Hello", "World", "!!!!!"])) as ArrayRef,
            ),
            (
                Arc::clone(&field_d),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::clone(&field_e),
                        Arc::new(Int16Array::from(vec![2, 4, 6])) as ArrayRef,
                    ),
                    (
                        Arc::clone(&field_f),
                        Arc::new(Int64Array::from(vec![3, 5, 7])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);
    }

    #[test]
    fn round_trip() {
        // Each array type should be able to round tripped through a scalar
        let cases: Vec<ArrayRef> = vec![
            // int
            Arc::new(Int8Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Int16Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(UInt16Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(UInt32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(UInt64Array::from(vec![Some(1), None, Some(3)])),
            // bool
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            // float
            Arc::new(Float32Array::from(vec![Some(1.0), None, Some(3.0)])),
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
            // string array
            Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")])),
            Arc::new(LargeStringArray::from(vec![Some("foo"), None, Some("bar")])),
            Arc::new(StringViewArray::from(vec![Some("foo"), None, Some("bar")])),
            // string dictionary
            {
                let mut builder = StringDictionaryBuilder::<Int32Type>::new();
                builder.append("foo").unwrap();
                builder.append_null();
                builder.append("bar").unwrap();
                Arc::new(builder.finish())
            },
            // binary array
            Arc::new(BinaryArray::from_iter(vec![
                Some(b"foo"),
                None,
                Some(b"bar"),
            ])),
            Arc::new(LargeBinaryArray::from_iter(vec![
                Some(b"foo"),
                None,
                Some(b"bar"),
            ])),
            Arc::new(BinaryViewArray::from_iter(vec![
                Some(b"foo"),
                None,
                Some(b"bar"),
            ])),
            // timestamp
            Arc::new(TimestampSecondArray::from(vec![Some(1), None, Some(3)])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1),
                None,
                Some(3),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1),
                None,
                Some(3),
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(1), None, Some(3)])),
            // timestamp with timezone
            Arc::new(
                TimestampSecondArray::from(vec![Some(1), None, Some(3)])
                    .with_timezone_opt(Some("UTC")),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(1), None, Some(3)])
                    .with_timezone_opt(Some("UTC")),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1), None, Some(3)])
                    .with_timezone_opt(Some("UTC")),
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1), None, Some(3)])
                    .with_timezone_opt(Some("UTC")),
            ),
            // date
            Arc::new(Date32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Date64Array::from(vec![Some(1), None, Some(3)])),
            // time
            Arc::new(Time32SecondArray::from(vec![Some(1), None, Some(3)])),
            Arc::new(Time32MillisecondArray::from(vec![Some(1), None, Some(3)])),
            Arc::new(Time64MicrosecondArray::from(vec![Some(1), None, Some(3)])),
            Arc::new(Time64NanosecondArray::from(vec![Some(1), None, Some(3)])),
            // null array
            Arc::new(NullArray::new(3)),
            // dense union
            {
                let mut builder = UnionBuilder::new_dense();
                builder.append::<Int32Type>("a", 1).unwrap();
                builder.append::<Float64Type>("b", 3.4).unwrap();
                Arc::new(builder.build().unwrap())
            },
            // sparse union
            {
                let mut builder = UnionBuilder::new_sparse();
                builder.append::<Int32Type>("a", 1).unwrap();
                builder.append::<Float64Type>("b", 3.4).unwrap();
                Arc::new(builder.build().unwrap())
            },
            // list array
            {
                let values_builder = StringBuilder::new();
                let mut builder = ListBuilder::new(values_builder);
                // [A, B]
                builder.values().append_value("A");
                builder.values().append_value("B");
                builder.append(true);
                // [ ] (empty list)
                builder.append(true);
                // Null
                builder.values().append_value("?"); // irrelevant
                builder.append(false);
                Arc::new(builder.finish())
            },
            // large list array
            {
                let values_builder = StringBuilder::new();
                let mut builder = LargeListBuilder::new(values_builder);
                // [A, B]
                builder.values().append_value("A");
                builder.values().append_value("B");
                builder.append(true);
                // [ ] (empty list)
                builder.append(true);
                // Null
                builder.append(false);
                Arc::new(builder.finish())
            },
            // fixed size list array
            {
                let values_builder = Int32Builder::new();
                let mut builder = FixedSizeListBuilder::new(values_builder, 3);

                //  [[0, 1, 2], null, [3, null, 5]
                builder.values().append_value(0);
                builder.values().append_value(1);
                builder.values().append_value(2);
                builder.append(true);
                builder.values().append_null();
                builder.values().append_null();
                builder.values().append_null();
                builder.append(false);
                builder.values().append_value(3);
                builder.values().append_null();
                builder.values().append_value(5);
                builder.append(true);
                Arc::new(builder.finish())
            },
            // map
            {
                let string_builder = StringBuilder::new();
                let int_builder = Int32Builder::with_capacity(4);

                let mut builder = MapBuilder::new(None, string_builder, int_builder);
                // {"joe": 1}
                builder.keys().append_value("joe");
                builder.values().append_value(1);
                builder.append(true).unwrap();
                // {}
                builder.append(true).unwrap();
                // null
                builder.append(false).unwrap();

                Arc::new(builder.finish())
            },
        ];

        for arr in cases {
            round_trip_through_scalar(arr);
        }
    }

    /// for each row in `arr`:
    /// 1. convert to a `ScalarValue`
    /// 2. Convert `ScalarValue` back to an `ArrayRef`
    /// 3. Compare the original array (sliced) and new array for equality
    fn round_trip_through_scalar(arr: ArrayRef) {
        for i in 0..arr.len() {
            // convert Scalar --> Array
            let scalar = ScalarValue::try_from_array(&arr, i).unwrap();
            let array = scalar.to_array_of_size(1).unwrap();
            assert_eq!(array.len(), 1);
            assert_eq!(array.data_type(), arr.data_type());
            assert_eq!(array.as_ref(), arr.slice(i, 1).as_ref());
        }
    }

    #[test]
    fn test_scalar_union_sparse() {
        let field_a = Arc::new(Field::new("A", DataType::Int32, true));
        let field_b = Arc::new(Field::new("B", DataType::Boolean, true));
        let field_c = Arc::new(Field::new("C", DataType::Utf8, true));
        let fields = UnionFields::from_iter([(0, field_a), (1, field_b), (2, field_c)]);

        let mut values_a = vec![None; 6];
        values_a[0] = Some(42);
        let mut values_b = vec![None; 6];
        values_b[1] = Some(true);
        let mut values_c = vec![None; 6];
        values_c[2] = Some("foo");
        let children: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(values_a)),
            Arc::new(BooleanArray::from(values_b)),
            Arc::new(StringArray::from(values_c)),
        ];

        let type_ids = ScalarBuffer::from(vec![0, 1, 2, 0, 1, 2]);
        let array: ArrayRef = Arc::new(
            UnionArray::try_new(fields.clone(), type_ids, None, children)
                .expect("UnionArray"),
        );

        let expected = [
            (0, ScalarValue::from(42)),
            (1, ScalarValue::from(true)),
            (2, ScalarValue::from("foo")),
            (0, ScalarValue::Int32(None)),
            (1, ScalarValue::Boolean(None)),
            (2, ScalarValue::Utf8(None)),
        ];

        for (i, (ti, value)) in expected.into_iter().enumerate() {
            let is_null = value.is_null();
            let value = Some((ti, Box::new(value)));
            let expected = ScalarValue::Union(value, fields.clone(), UnionMode::Sparse);
            let actual = ScalarValue::try_from_array(&array, i).expect("try_from_array");

            assert_eq!(
                actual, expected,
                "[{i}] {actual} was not equal to {expected}"
            );

            assert!(
                expected.eq_array(&array, i).expect("eq_array"),
                "[{i}] {expected}.eq_array was false"
            );

            if is_null {
                assert!(actual.is_null(), "[{i}] {actual} was not null")
            }
        }
    }

    #[test]
    fn test_scalar_union_dense() {
        let field_a = Arc::new(Field::new("A", DataType::Int32, true));
        let field_b = Arc::new(Field::new("B", DataType::Boolean, true));
        let field_c = Arc::new(Field::new("C", DataType::Utf8, true));
        let fields = UnionFields::from_iter([(0, field_a), (1, field_b), (2, field_c)]);
        let children: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(42), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringArray::from(vec![Some("foo"), None])),
        ];

        let type_ids = ScalarBuffer::from(vec![0, 1, 2, 0, 1, 2]);
        let offsets = ScalarBuffer::from(vec![0, 0, 0, 1, 1, 1]);
        let array: ArrayRef = Arc::new(
            UnionArray::try_new(fields.clone(), type_ids, Some(offsets), children)
                .expect("UnionArray"),
        );

        let expected = [
            (0, ScalarValue::from(42)),
            (1, ScalarValue::from(true)),
            (2, ScalarValue::from("foo")),
            (0, ScalarValue::Int32(None)),
            (1, ScalarValue::Boolean(None)),
            (2, ScalarValue::Utf8(None)),
        ];

        for (i, (ti, value)) in expected.into_iter().enumerate() {
            let is_null = value.is_null();
            let value = Some((ti, Box::new(value)));
            let expected = ScalarValue::Union(value, fields.clone(), UnionMode::Dense);
            let actual = ScalarValue::try_from_array(&array, i).expect("try_from_array");

            assert_eq!(
                actual, expected,
                "[{i}] {actual} was not equal to {expected}"
            );

            assert!(
                expected.eq_array(&array, i).expect("eq_array"),
                "[{i}] {expected}.eq_array was false"
            );

            if is_null {
                assert!(actual.is_null(), "[{i}] {actual} was not null")
            }
        }
    }

    #[test]
    fn test_lists_in_struct() {
        let field_a = Arc::new(Field::new("A", DataType::Utf8, false));
        let field_primitive_list = Arc::new(Field::new(
            "primitive_list",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            false,
        ));

        // Define primitive list scalars
        let l0 =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let l1 =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(4),
                    Some(5),
                ])]),
            ));
        let l2 = ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<
            Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(6)])])));

        // Define struct scalars
        let s0 = ScalarValue::from(vec![
            ("A", ScalarValue::from("First")),
            ("primitive_list", l0),
        ]);

        let s1 = ScalarValue::from(vec![
            ("A", ScalarValue::from("Second")),
            ("primitive_list", l1),
        ]);

        let s2 = ScalarValue::from(vec![
            ("A", ScalarValue::from("Third")),
            ("primitive_list", l2),
        ]);

        // iter_to_array for struct scalars
        let array =
            ScalarValue::iter_to_array(vec![s0.clone(), s1.clone(), s2.clone()]).unwrap();

        let array = as_struct_array(&array).unwrap();
        let expected = StructArray::from(vec![
            (
                Arc::clone(&field_a),
                Arc::new(StringArray::from(vec!["First", "Second", "Third"])) as ArrayRef,
            ),
            (
                Arc::clone(&field_primitive_list),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), Some(2), Some(3)]),
                    Some(vec![Some(4), Some(5)]),
                    Some(vec![Some(6)]),
                ])),
            ),
        ]);

        assert_eq!(array, &expected);

        // Define list-of-structs scalars

        let nl0_array = ScalarValue::iter_to_array(vec![s0, s1.clone()]).unwrap();
        let nl0 = SingleRowListArrayBuilder::new(nl0_array).build_list_scalar();

        let nl1_array = ScalarValue::iter_to_array(vec![s2]).unwrap();
        let nl1 = SingleRowListArrayBuilder::new(nl1_array).build_list_scalar();

        let nl2_array = ScalarValue::iter_to_array(vec![s1]).unwrap();
        let nl2 = SingleRowListArrayBuilder::new(nl2_array).build_list_scalar();

        // iter_to_array for list-of-struct
        let array = ScalarValue::iter_to_array(vec![nl0, nl1, nl2]).unwrap();
        let array = array.as_list::<i32>();

        // Construct expected array with array builders
        let field_a_builder = StringBuilder::with_capacity(4, 1024);
        let primitive_value_builder = Int32Array::builder(8);
        let field_primitive_list_builder = ListBuilder::new(primitive_value_builder);

        let element_builder = StructBuilder::new(
            vec![field_a, field_primitive_list],
            vec![
                Box::new(field_a_builder),
                Box::new(field_primitive_list_builder),
            ],
        );

        let mut list_builder = ListBuilder::new(element_builder);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("First");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(1);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(2);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(3);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Third");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(6);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        let expected = list_builder.finish();

        assert_eq!(array, &expected);
    }

    fn build_2d_list(data: Vec<Option<i32>>) -> ListArray {
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(data)]);
        ListArray::new(
            Arc::new(Field::new_list_field(
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            )),
            OffsetBuffer::<i32>::from_lengths([1]),
            Arc::new(a1),
            None,
        )
    }

    #[test]
    fn test_nested_lists() {
        // Define inner list scalars
        let arr1 = build_2d_list(vec![Some(1), Some(2), Some(3)]);
        let arr2 = build_2d_list(vec![Some(4), Some(5)]);
        let arr3 = build_2d_list(vec![Some(6)]);

        let array = ScalarValue::iter_to_array(vec![
            ScalarValue::List(Arc::new(arr1)),
            ScalarValue::List(Arc::new(arr2)),
            ScalarValue::List(Arc::new(arr3)),
        ])
        .unwrap();
        let array = array.as_list::<i32>();

        // Construct expected array with array builders
        let inner_builder = Int32Array::builder(6);
        let middle_builder = ListBuilder::new(inner_builder);
        let mut outer_builder = ListBuilder::new(middle_builder);

        outer_builder.values().values().append_value(1);
        outer_builder.values().values().append_value(2);
        outer_builder.values().values().append_value(3);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(4);
        outer_builder.values().values().append_value(5);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(6);
        outer_builder.values().append(true);
        outer_builder.append(true);

        let expected = outer_builder.finish();

        assert_eq!(array, &expected);
    }

    #[test]
    fn scalar_timestamp_ns_utc_timezone() {
        let scalar = ScalarValue::TimestampNanosecond(
            Some(1599566400000000000),
            Some("UTC".into()),
        );

        assert_eq!(
            scalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let array = scalar.to_array().expect("Failed to convert to array");
        assert_eq!(array.len(), 1);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let new_scalar = ScalarValue::try_from_array(&array, 0).unwrap();
        assert_eq!(
            new_scalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn cast_round_trip() {
        check_scalar_cast(ScalarValue::Int8(Some(5)), DataType::Int16);
        check_scalar_cast(ScalarValue::Int8(None), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(Some(5.5)), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(None), DataType::Int16);

        check_scalar_cast(
            ScalarValue::from("foo"),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );

        check_scalar_cast(
            ScalarValue::Utf8(None),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );

        check_scalar_cast(ScalarValue::Utf8(None), DataType::Utf8View);
        check_scalar_cast(ScalarValue::from("foo"), DataType::Utf8View);
        check_scalar_cast(
            ScalarValue::from("larger than 12 bytes string"),
            DataType::Utf8View,
        );
        check_scalar_cast(
            {
                let element_field =
                    Arc::new(Field::new("element", DataType::Int32, true));

                let mut builder =
                    ListBuilder::new(Int32Builder::new()).with_field(element_field);
                builder.append_value([Some(1)]);
                builder.append(true);

                ScalarValue::List(Arc::new(builder.finish()))
            },
            DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
        );
        check_scalar_cast(
            {
                let element_field =
                    Arc::new(Field::new("element", DataType::Int32, true));

                let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 1)
                    .with_field(element_field);
                builder.values().append_value(1);
                builder.append(true);

                ScalarValue::FixedSizeList(Arc::new(builder.finish()))
            },
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Int64, true)),
                1,
            ),
        );
        check_scalar_cast(
            {
                let element_field =
                    Arc::new(Field::new("element", DataType::Int32, true));

                let mut builder =
                    LargeListBuilder::new(Int32Builder::new()).with_field(element_field);
                builder.append_value([Some(1)]);
                builder.append(true);

                ScalarValue::LargeList(Arc::new(builder.finish()))
            },
            DataType::LargeList(Arc::new(Field::new("element", DataType::Int64, true))),
        );
    }

    // mimics how casting work on scalar values by `casting` `scalar` to `desired_type`
    fn check_scalar_cast(scalar: ScalarValue, desired_type: DataType) {
        // convert from scalar --> Array to call cast
        let scalar_array = scalar.to_array().expect("Failed to convert to array");
        // cast the actual value
        let cast_array = kernels::cast::cast(&scalar_array, &desired_type).unwrap();

        // turn it back to a scalar
        let cast_scalar = ScalarValue::try_from_array(&cast_array, 0).unwrap();
        assert_eq!(cast_scalar.data_type(), desired_type);

        // Some time later the "cast" scalar is turned back into an array:
        let array = cast_scalar
            .to_array_of_size(10)
            .expect("Failed to convert to array of size");

        // The datatype should be "Dictionary" but is actually Utf8!!!
        assert_eq!(array.data_type(), &desired_type)
    }

    #[test]
    fn test_scalar_negative() -> Result<()> {
        // positive test
        let value = ScalarValue::Int32(Some(12));
        assert_eq!(ScalarValue::Int32(Some(-12)), value.arithmetic_negate()?);
        let value = ScalarValue::Int32(None);
        assert_eq!(ScalarValue::Int32(None), value.arithmetic_negate()?);

        // negative test
        let value = ScalarValue::UInt8(Some(12));
        assert!(value.arithmetic_negate().is_err());
        let value = ScalarValue::Boolean(None);
        assert!(value.arithmetic_negate().is_err());
        Ok(())
    }

    #[test]
    #[allow(arithmetic_overflow)] // we want to test them
    fn test_scalar_negative_overflows() -> Result<()> {
        macro_rules! test_overflow_on_value {
            ($($val:expr),* $(,)?) => {$(
                {
                    let value: ScalarValue = $val;
                    let err = value.arithmetic_negate().expect_err("Should receive overflow error on negating {value:?}");
                    let root_err = err.find_root();
                    match  root_err{
                        DataFusionError::ArrowError(err, _) if matches!(err.as_ref(), ArrowError::ArithmeticOverflow(_)) => {}
                        _ => return Err(err),
                    };
                }
            )*};
        }
        test_overflow_on_value!(
            // the integers
            i8::MIN.into(),
            i16::MIN.into(),
            i32::MIN.into(),
            i64::MIN.into(),
            // for decimals, only value needs to be tested
            ScalarValue::try_new_decimal128(i128::MIN, 10, 5)?,
            ScalarValue::Decimal256(Some(i256::MIN), 20, 5),
            // interval, check all possible values
            ScalarValue::IntervalYearMonth(Some(i32::MIN)),
            ScalarValue::new_interval_dt(i32::MIN, 999),
            ScalarValue::new_interval_dt(1, i32::MIN),
            ScalarValue::new_interval_mdn(i32::MIN, 15, 123_456),
            ScalarValue::new_interval_mdn(12, i32::MIN, 123_456),
            ScalarValue::new_interval_mdn(12, 15, i64::MIN),
            // tz doesn't matter when negating
            ScalarValue::TimestampSecond(Some(i64::MIN), None),
            ScalarValue::TimestampMillisecond(Some(i64::MIN), None),
            ScalarValue::TimestampMicrosecond(Some(i64::MIN), None),
            ScalarValue::TimestampNanosecond(Some(i64::MIN), None),
        );

        let float_cases = [
            (
                ScalarValue::Float16(Some(f16::MIN)),
                ScalarValue::Float16(Some(f16::MAX)),
            ),
            (
                ScalarValue::Float16(Some(f16::MAX)),
                ScalarValue::Float16(Some(f16::MIN)),
            ),
            (f32::MIN.into(), f32::MAX.into()),
            (f32::MAX.into(), f32::MIN.into()),
            (f64::MIN.into(), f64::MAX.into()),
            (f64::MAX.into(), f64::MIN.into()),
        ];
        // skip float 16 because they aren't supported
        for (test, expected) in float_cases.into_iter().skip(2) {
            assert_eq!(test.arithmetic_negate()?, expected);
        }
        Ok(())
    }

    #[test]
    fn f16_test_overflow() {
        // TODO: if negate supports f16, add these cases to `test_scalar_negative_overflows` test case
        let cases = [
            (
                ScalarValue::Float16(Some(f16::MIN)),
                ScalarValue::Float16(Some(f16::MAX)),
            ),
            (
                ScalarValue::Float16(Some(f16::MAX)),
                ScalarValue::Float16(Some(f16::MIN)),
            ),
        ];

        for (test, expected) in cases {
            assert_eq!(test.arithmetic_negate().unwrap(), expected);
        }
    }

    macro_rules! expect_operation_error {
        ($TEST_NAME:ident, $FUNCTION:ident, $EXPECTED_ERROR:expr) => {
            #[test]
            fn $TEST_NAME() {
                let lhs = ScalarValue::UInt64(Some(12));
                let rhs = ScalarValue::Int32(Some(-3));
                match lhs.$FUNCTION(&rhs) {
                    Ok(_result) => {
                        panic!(
                            "Expected binary operation error between lhs: '{:?}', rhs: {:?}",
                            lhs, rhs
                        );
                    }
                    Err(e) => {
                        let error_message = e.to_string();
                        assert!(
                            error_message.contains($EXPECTED_ERROR),
                            "Expected error '{}' not found in actual error '{}'",
                            $EXPECTED_ERROR,
                            error_message
                        );
                    }
                }
            }
        };
    }

    expect_operation_error!(
        expect_add_error,
        add,
        "Invalid arithmetic operation: UInt64 + Int32"
    );
    expect_operation_error!(
        expect_sub_error,
        sub,
        "Invalid arithmetic operation: UInt64 - Int32"
    );

    macro_rules! decimal_op_test_cases {
    ($OPERATION:ident, [$([$L_VALUE:expr, $L_PRECISION:expr, $L_SCALE:expr, $R_VALUE:expr, $R_PRECISION:expr, $R_SCALE:expr, $O_VALUE:expr, $O_PRECISION:expr, $O_SCALE:expr]),+]) => {
            $(

                let left = ScalarValue::Decimal128($L_VALUE, $L_PRECISION, $L_SCALE);
                let right = ScalarValue::Decimal128($R_VALUE, $R_PRECISION, $R_SCALE);
                let result = left.$OPERATION(&right).unwrap();
                assert_eq!(ScalarValue::Decimal128($O_VALUE, $O_PRECISION, $O_SCALE), result);

            )+
        };
    }

    #[test]
    fn decimal_operations() {
        decimal_op_test_cases!(
            add,
            [
                [Some(123), 10, 2, Some(124), 10, 2, Some(123 + 124), 11, 2],
                // test sum decimal with diff scale
                [
                    Some(123),
                    10,
                    3,
                    Some(124),
                    10,
                    2,
                    Some(123 + 124 * 10_i128.pow(1)),
                    12,
                    3
                ],
                // diff precision and scale for decimal data type
                [
                    Some(123),
                    10,
                    2,
                    Some(124),
                    11,
                    3,
                    Some(123 * 10_i128.pow(3 - 2) + 124),
                    12,
                    3
                ]
            ]
        );
    }

    #[test]
    fn decimal_operations_with_nulls() {
        decimal_op_test_cases!(
            add,
            [
                // Case: (None, Some, 0)
                [None, 10, 2, Some(123), 10, 2, None, 11, 2],
                // Case: (Some, None, 0)
                [Some(123), 10, 2, None, 10, 2, None, 11, 2],
                // Case: (Some, None, _) + Side=False
                [Some(123), 8, 2, None, 10, 3, None, 11, 3],
                // Case: (None, Some, _) + Side=False
                [None, 8, 2, Some(123), 10, 3, None, 11, 3],
                // Case: (Some, None, _) + Side=True
                [Some(123), 8, 4, None, 10, 3, None, 12, 4],
                // Case: (None, Some, _) + Side=True
                [None, 10, 3, Some(123), 8, 4, None, 12, 4]
            ]
        );
    }

    #[test]
    fn test_scalar_distance() {
        let cases = [
            // scalar (lhs), scalar (rhs), expected distance
            // ---------------------------------------------
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2)), 1),
            (ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(1)), 1),
            (
                ScalarValue::Int16(Some(-5)),
                ScalarValue::Int16(Some(5)),
                10,
            ),
            (
                ScalarValue::Int16(Some(5)),
                ScalarValue::Int16(Some(-5)),
                10,
            ),
            (ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(0)), 0),
            (
                ScalarValue::Int32(Some(-5)),
                ScalarValue::Int32(Some(-10)),
                5,
            ),
            (
                ScalarValue::Int64(Some(-10)),
                ScalarValue::Int64(Some(-5)),
                5,
            ),
            (ScalarValue::UInt8(Some(1)), ScalarValue::UInt8(Some(2)), 1),
            (ScalarValue::UInt8(Some(0)), ScalarValue::UInt8(Some(0)), 0),
            (
                ScalarValue::UInt16(Some(5)),
                ScalarValue::UInt16(Some(10)),
                5,
            ),
            (
                ScalarValue::UInt32(Some(10)),
                ScalarValue::UInt32(Some(5)),
                5,
            ),
            (
                ScalarValue::UInt64(Some(5)),
                ScalarValue::UInt64(Some(10)),
                5,
            ),
            (
                ScalarValue::Float16(Some(f16::from_f32(1.1))),
                ScalarValue::Float16(Some(f16::from_f32(1.9))),
                1,
            ),
            (
                ScalarValue::Float16(Some(f16::from_f32(-5.3))),
                ScalarValue::Float16(Some(f16::from_f32(-9.2))),
                4,
            ),
            (
                ScalarValue::Float16(Some(f16::from_f32(-5.3))),
                ScalarValue::Float16(Some(f16::from_f32(-9.7))),
                4,
            ),
            (
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
                1,
            ),
            (
                ScalarValue::Float32(Some(2.0)),
                ScalarValue::Float32(Some(1.0)),
                1,
            ),
            (
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(0.0)),
                0,
            ),
            (
                ScalarValue::Float64(Some(-5.0)),
                ScalarValue::Float64(Some(-10.0)),
                5,
            ),
            (
                ScalarValue::Float64(Some(-10.0)),
                ScalarValue::Float64(Some(-5.0)),
                5,
            ),
            // Floats are currently special cased to f64/f32 and the result is rounded
            // rather than ceiled/floored. In the future we might want to take a mode
            // which specified the rounding behavior.
            (
                ScalarValue::Float32(Some(1.2)),
                ScalarValue::Float32(Some(1.3)),
                0,
            ),
            (
                ScalarValue::Float32(Some(1.1)),
                ScalarValue::Float32(Some(1.9)),
                1,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.2)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.7)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.9)),
                5,
            ),
            (
                ScalarValue::Decimal128(Some(10), 1, 0),
                ScalarValue::Decimal128(Some(5), 1, 0),
                5,
            ),
            (
                ScalarValue::Decimal128(Some(5), 1, 0),
                ScalarValue::Decimal128(Some(10), 1, 0),
                5,
            ),
            (
                ScalarValue::Decimal256(Some(10.into()), 1, 0),
                ScalarValue::Decimal256(Some(5.into()), 1, 0),
                5,
            ),
            (
                ScalarValue::Decimal256(Some(5.into()), 1, 0),
                ScalarValue::Decimal256(Some(10.into()), 1, 0),
                5,
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let distance = lhs.distance(rhs).unwrap();
            assert_eq!(distance, *expected);
        }
    }

    #[test]
    fn test_distance_none() {
        let cases = [
            (
                ScalarValue::Decimal128(Some(i128::MAX), DECIMAL128_MAX_PRECISION, 0),
                ScalarValue::Decimal128(Some(-i128::MAX), DECIMAL128_MAX_PRECISION, 0),
            ),
            (
                ScalarValue::Decimal256(Some(i256::MAX), DECIMAL256_MAX_PRECISION, 0),
                ScalarValue::Decimal256(Some(-i256::MAX), DECIMAL256_MAX_PRECISION, 0),
            ),
        ];
        for (lhs, rhs) in cases.iter() {
            let distance = lhs.distance(rhs);
            assert!(distance.is_none(), "{lhs} vs {rhs}");
        }
    }

    #[test]
    fn test_scalar_distance_invalid() {
        let cases = [
            // scalar (lhs), scalar (rhs)
            // --------------------------
            // Same type but with nulls
            (ScalarValue::Int8(None), ScalarValue::Int8(None)),
            (ScalarValue::Int8(None), ScalarValue::Int8(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(None)),
            // Different type
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Float32(Some(1.0))),
            (
                ScalarValue::Float16(Some(f16::from_f32(1.0))),
                ScalarValue::Float32(Some(1.0)),
            ),
            (
                ScalarValue::Float16(Some(f16::from_f32(1.0))),
                ScalarValue::Int32(Some(1)),
            ),
            (
                ScalarValue::Float64(Some(1.1)),
                ScalarValue::Float32(Some(2.2)),
            ),
            (
                ScalarValue::UInt64(Some(777)),
                ScalarValue::Int32(Some(111)),
            ),
            // Different types with nulls
            (ScalarValue::Int8(None), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(None)),
            // Unsupported types
            (ScalarValue::from("foo"), ScalarValue::from("bar")),
            (
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
            ),
            (ScalarValue::Date32(Some(0)), ScalarValue::Date32(Some(1))),
            (ScalarValue::Date64(Some(0)), ScalarValue::Date64(Some(1))),
            (
                ScalarValue::Decimal128(Some(123), 5, 5),
                ScalarValue::Decimal128(Some(120), 5, 3),
            ),
            (
                ScalarValue::Decimal128(Some(123), 5, 5),
                ScalarValue::Decimal128(Some(120), 3, 5),
            ),
            (
                ScalarValue::Decimal256(Some(123.into()), 5, 5),
                ScalarValue::Decimal256(Some(120.into()), 3, 5),
            ),
            // Distance 2 * 2^50 is larger than usize
            (
                ScalarValue::Decimal256(
                    Some(i256::from_parts(0, 2_i64.pow(50).into())),
                    1,
                    0,
                ),
                ScalarValue::Decimal256(
                    Some(i256::from_parts(0, (-(2_i64).pow(50)).into())),
                    1,
                    0,
                ),
            ),
            // Distance overflow
            (
                ScalarValue::Decimal256(Some(i256::from_parts(0, i128::MAX)), 1, 0),
                ScalarValue::Decimal256(Some(i256::from_parts(0, -i128::MAX)), 1, 0),
            ),
        ];
        for (lhs, rhs) in cases {
            let distance = lhs.distance(&rhs);
            assert!(distance.is_none());
        }
    }

    #[test]
    fn test_scalar_interval_negate() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(-1, -12),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(-1, -999),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(-12, -15, -123_456),
            ),
        ];
        for (expr, expected) in cases.iter() {
            let result = expr.arithmetic_negate().unwrap();
            assert_eq!(*expected, result, "-expr:{expr:?}");
        }
    }

    #[test]
    fn test_scalar_interval_add() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(2, 24),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(2, 1998),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(24, 30, 246_912),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.add(rhs).unwrap();
            let result_commute = rhs.add(lhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} + rhs:{rhs:?}");
            assert_eq!(*expected, result_commute, "lhs:{rhs:?} + rhs:{lhs:?}");
        }
    }

    #[test]
    fn test_scalar_interval_sub() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(0, 0),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(0, 0),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(0, 0, 0),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.sub(rhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} - rhs:{rhs:?}");
        }
    }

    #[test]
    fn timestamp_op_random_tests() {
        // timestamp1 + (or -) interval = timestamp2
        // timestamp2 - timestamp1 (or timestamp1 - timestamp2) = interval ?
        let sample_size = 1000;
        let timestamps1 = get_random_timestamps(sample_size);
        let intervals = get_random_intervals(sample_size);
        // ts(sec) + interval(ns) = ts(sec); however,
        // ts(sec) - ts(sec) cannot be = interval(ns). Therefore,
        // timestamps are more precise than intervals in tests.
        for (idx, ts1) in timestamps1.iter().enumerate() {
            if idx % 2 == 0 {
                let timestamp2 = ts1.add(intervals[idx].clone()).unwrap();
                let back = timestamp2.sub(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            } else {
                let timestamp2 = ts1.sub(intervals[idx].clone()).unwrap();
                let back = timestamp2.add(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            };
        }
    }

    #[test]
    fn test_struct_nulls() {
        let fields_b = Fields::from(vec![
            Field::new("ba", DataType::UInt64, true),
            Field::new("bb", DataType::UInt64, true),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Struct(fields_b.clone()), true),
        ]);

        let struct_value = vec![
            (
                Arc::clone(&fields[0]),
                Arc::new(UInt64Array::from(vec![Some(1)])) as ArrayRef,
            ),
            (
                Arc::clone(&fields[1]),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::clone(&fields_b[0]),
                        Arc::new(UInt64Array::from(vec![Some(2)])) as ArrayRef,
                    ),
                    (
                        Arc::clone(&fields_b[1]),
                        Arc::new(UInt64Array::from(vec![Some(3)])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ];

        let struct_value_with_nulls = vec![
            (
                Arc::clone(&fields[0]),
                Arc::new(UInt64Array::from(vec![Some(1)])) as ArrayRef,
            ),
            (
                Arc::clone(&fields[1]),
                Arc::new(StructArray::from((
                    vec![
                        (
                            Arc::clone(&fields_b[0]),
                            Arc::new(UInt64Array::from(vec![Some(2)])) as ArrayRef,
                        ),
                        (
                            Arc::clone(&fields_b[1]),
                            Arc::new(UInt64Array::from(vec![Some(3)])) as ArrayRef,
                        ),
                    ],
                    Buffer::from(&[0]),
                ))) as ArrayRef,
            ),
        ];

        let scalars = vec![
            // all null
            ScalarValue::Struct(Arc::new(StructArray::from((
                struct_value.clone(),
                Buffer::from(&[0]),
            )))),
            // field 1 valid, field 2 null
            ScalarValue::Struct(Arc::new(StructArray::from((
                struct_value_with_nulls.clone(),
                Buffer::from(&[1]),
            )))),
            // all valid
            ScalarValue::Struct(Arc::new(StructArray::from((
                struct_value.clone(),
                Buffer::from(&[1]),
            )))),
        ];

        let check_array = |array| {
            let is_null = is_null(&array).unwrap();
            assert_eq!(is_null, BooleanArray::from(vec![true, false, false]));

            let formatted = pretty_format_columns("col", &[array]).unwrap().to_string();
            let formatted = formatted.split('\n').collect::<Vec<_>>();
            let expected = vec![
                "+---------------------------+",
                "| col                       |",
                "+---------------------------+",
                "|                           |",
                "| {a: 1, b: }               |",
                "| {a: 1, b: {ba: 2, bb: 3}} |",
                "+---------------------------+",
            ];
            assert_eq!(
                formatted, expected,
                "Actual:\n{formatted:#?}\n\nExpected:\n{expected:#?}"
            );
        };

        // test `ScalarValue::iter_to_array`
        let array = ScalarValue::iter_to_array(scalars.clone()).unwrap();
        check_array(array);

        // test `ScalarValue::to_array` / `ScalarValue::to_array_of_size`
        let arrays = scalars
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()
            .expect("Failed to convert to array");
        let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        let array = arrow::compute::concat(&arrays).unwrap();
        check_array(array);
    }

    #[test]
    fn test_struct_display() {
        let field_a = Field::new("a", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Utf8, true);

        let s = ScalarStructBuilder::new()
            .with_scalar(field_a, ScalarValue::from(1i32))
            .with_scalar(field_b, ScalarValue::Utf8(None))
            .build()
            .unwrap();

        assert_eq!(s.to_string(), "{a:1,b:}");
        assert_eq!(format!("{s:?}"), r#"Struct({a:1,b:})"#);

        let ScalarValue::Struct(arr) = s else {
            panic!("Expected struct");
        };

        //verify compared to arrow display
        let batch = RecordBatch::try_from_iter(vec![("s", arr as _)]).unwrap();
        assert_snapshot!(batches_to_string(&[batch]), @r"
        +-------------+
        | s           |
        +-------------+
        | {a: 1, b: } |
        +-------------+
        ");
    }

    #[test]
    fn test_null_bug() {
        let field_a = Field::new("a", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Int32, true);
        let fields = Fields::from(vec![field_a, field_b]);

        let array_a = Arc::new(Int32Array::from_iter_values([1]));
        let array_b = Arc::new(Int32Array::from_iter_values([2]));
        let arrays: Vec<ArrayRef> = vec![array_a, array_b];

        let mut not_nulls = NullBufferBuilder::new(1);

        not_nulls.append_non_null();

        let ar = StructArray::new(fields, arrays, not_nulls.finish());
        let s = ScalarValue::Struct(Arc::new(ar));

        assert_eq!(s.to_string(), "{a:1,b:2}");
        assert_eq!(format!("{s:?}"), r#"Struct({a:1,b:2})"#);

        let ScalarValue::Struct(arr) = s else {
            panic!("Expected struct");
        };

        //verify compared to arrow display
        let batch = RecordBatch::try_from_iter(vec![("s", arr as _)]).unwrap();
        assert_snapshot!(batches_to_string(&[batch]), @r"
        +--------------+
        | s            |
        +--------------+
        | {a: 1, b: 2} |
        +--------------+
        ");
    }

    #[test]
    fn test_display_date64_large_values() {
        assert_eq!(
            format!("{}", ScalarValue::Date64(Some(790179464505))),
            "1995-01-15"
        );
        // This used to panic, see https://github.com/apache/arrow-rs/issues/7728
        assert_eq!(
            format!("{}", ScalarValue::Date64(Some(-790179464505600000))),
            ""
        );
    }

    #[test]
    fn test_struct_display_null() {
        let fields = vec![Field::new("a", DataType::Int32, false)];
        let s = ScalarStructBuilder::new_null(fields);
        assert_eq!(s.to_string(), "NULL");

        let ScalarValue::Struct(arr) = s else {
            panic!("Expected struct");
        };

        //verify compared to arrow display
        let batch = RecordBatch::try_from_iter(vec![("s", arr as _)]).unwrap();

        assert_snapshot!(batches_to_string(&[batch]), @r"
        +---+
        | s |
        +---+
        |   |
        +---+
        ");
    }

    #[test]
    fn test_map_display_and_debug() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);
        let mut builder = MapBuilder::new(None, string_builder, int_builder);
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        let map_value = ScalarValue::Map(Arc::new(builder.finish()));

        assert_eq!(map_value.to_string(), "[{joe:1},{blogs:2,foo:4},{},NULL]");
        assert_eq!(
            format!("{map_value:?}"),
            r#"Map([{"joe":"1"},{"blogs":"2","foo":"4"},{},NULL])"#
        );

        let ScalarValue::Map(arr) = map_value else {
            panic!("Expected map");
        };

        //verify compared to arrow display
        let batch = RecordBatch::try_from_iter(vec![("m", arr as _)]).unwrap();
        assert_snapshot!(batches_to_string(&[batch]), @r"
        +--------------------+
        | m                  |
        +--------------------+
        | {joe: 1}           |
        | {blogs: 2, foo: 4} |
        | {}                 |
        |                    |
        +--------------------+
        ");
    }

    #[test]
    fn test_binary_display() {
        let no_binary_value = ScalarValue::Binary(None);
        assert_eq!(format!("{no_binary_value}"), "NULL");
        let single_binary_value = ScalarValue::Binary(Some(vec![42u8]));
        assert_eq!(format!("{single_binary_value}"), "2A");
        let small_binary_value = ScalarValue::Binary(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value}"), "010203");
        let large_binary_value =
            ScalarValue::Binary(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(format!("{large_binary_value}"), "0102030405060708090A...");

        let no_binary_value = ScalarValue::BinaryView(None);
        assert_eq!(format!("{no_binary_value}"), "NULL");
        let small_binary_value = ScalarValue::BinaryView(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value}"), "010203");
        let large_binary_value =
            ScalarValue::BinaryView(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(format!("{large_binary_value}"), "0102030405060708090A...");

        let no_binary_value = ScalarValue::LargeBinary(None);
        assert_eq!(format!("{no_binary_value}"), "NULL");
        let small_binary_value = ScalarValue::LargeBinary(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value}"), "010203");
        let large_binary_value =
            ScalarValue::LargeBinary(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(format!("{large_binary_value}"), "0102030405060708090A...");

        let no_binary_value = ScalarValue::FixedSizeBinary(3, None);
        assert_eq!(format!("{no_binary_value}"), "NULL");
        let small_binary_value = ScalarValue::FixedSizeBinary(3, Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value}"), "010203");
        let large_binary_value = ScalarValue::FixedSizeBinary(
            11,
            Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        );
        assert_eq!(format!("{large_binary_value}"), "0102030405060708090A...");
    }

    #[test]
    fn test_binary_debug() {
        let no_binary_value = ScalarValue::Binary(None);
        assert_eq!(format!("{no_binary_value:?}"), "Binary(NULL)");
        let single_binary_value = ScalarValue::Binary(Some(vec![42u8]));
        assert_eq!(format!("{single_binary_value:?}"), "Binary(\"42\")");
        let small_binary_value = ScalarValue::Binary(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value:?}"), "Binary(\"1,2,3\")");
        let large_binary_value =
            ScalarValue::Binary(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(
            format!("{large_binary_value:?}"),
            "Binary(\"1,2,3,4,5,6,7,8,9,10,11\")"
        );

        let no_binary_value = ScalarValue::BinaryView(None);
        assert_eq!(format!("{no_binary_value:?}"), "BinaryView(NULL)");
        let small_binary_value = ScalarValue::BinaryView(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value:?}"), "BinaryView(\"1,2,3\")");
        let large_binary_value =
            ScalarValue::BinaryView(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(
            format!("{large_binary_value:?}"),
            "BinaryView(\"1,2,3,4,5,6,7,8,9,10,11\")"
        );

        let no_binary_value = ScalarValue::LargeBinary(None);
        assert_eq!(format!("{no_binary_value:?}"), "LargeBinary(NULL)");
        let small_binary_value = ScalarValue::LargeBinary(Some(vec![1u8, 2, 3]));
        assert_eq!(format!("{small_binary_value:?}"), "LargeBinary(\"1,2,3\")");
        let large_binary_value =
            ScalarValue::LargeBinary(Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]));
        assert_eq!(
            format!("{large_binary_value:?}"),
            "LargeBinary(\"1,2,3,4,5,6,7,8,9,10,11\")"
        );

        let no_binary_value = ScalarValue::FixedSizeBinary(3, None);
        assert_eq!(format!("{no_binary_value:?}"), "FixedSizeBinary(3, NULL)");
        let small_binary_value = ScalarValue::FixedSizeBinary(3, Some(vec![1u8, 2, 3]));
        assert_eq!(
            format!("{small_binary_value:?}"),
            "FixedSizeBinary(3, \"1,2,3\")"
        );
        let large_binary_value = ScalarValue::FixedSizeBinary(
            11,
            Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        );
        assert_eq!(
            format!("{large_binary_value:?}"),
            "FixedSizeBinary(11, \"1,2,3,4,5,6,7,8,9,10,11\")"
        );
    }

    #[test]
    fn test_build_timestamp_millisecond_list() {
        let values = vec![ScalarValue::TimestampMillisecond(Some(1), None)];
        let arr = ScalarValue::new_list_nullable(
            &values,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        assert_eq!(1, arr.len());
    }

    #[test]
    fn test_newlist_timestamp_zone() {
        let s: &'static str = "UTC";
        let values = vec![ScalarValue::TimestampMillisecond(Some(1), Some(s.into()))];
        let arr = ScalarValue::new_list_nullable(
            &values,
            &DataType::Timestamp(TimeUnit::Millisecond, Some(s.into())),
        );
        assert_eq!(1, arr.len());
        assert_eq!(
            arr.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(
                DataType::Timestamp(TimeUnit::Millisecond, Some(s.into())),
                true,
            )))
        );
    }

    fn get_random_timestamps(sample_size: u64) -> Vec<ScalarValue> {
        let vector_size = sample_size;
        let mut timestamp = vec![];
        let mut rng = rand::rng();
        for i in 0..vector_size {
            let year = rng.random_range(1995..=2050);
            let month = rng.random_range(1..=12);
            let day = rng.random_range(1..=28); // to exclude invalid dates
            let hour = rng.random_range(0..=23);
            let minute = rng.random_range(0..=59);
            let second = rng.random_range(0..=59);
            if i % 4 == 0 {
                timestamp.push(ScalarValue::TimestampSecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_opt(hour, minute, second)
                            .unwrap()
                            .and_utc()
                            .timestamp(),
                    ),
                    None,
                ))
            } else if i % 4 == 1 {
                let millisec = rng.random_range(0..=999);
                timestamp.push(ScalarValue::TimestampMillisecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_milli_opt(hour, minute, second, millisec)
                            .unwrap()
                            .and_utc()
                            .timestamp_millis(),
                    ),
                    None,
                ))
            } else if i % 4 == 2 {
                let microsec = rng.random_range(0..=999_999);
                timestamp.push(ScalarValue::TimestampMicrosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_micro_opt(hour, minute, second, microsec)
                            .unwrap()
                            .and_utc()
                            .timestamp_micros(),
                    ),
                    None,
                ))
            } else if i % 4 == 3 {
                let nanosec = rng.random_range(0..=999_999_999);
                timestamp.push(ScalarValue::TimestampNanosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_nano_opt(hour, minute, second, nanosec)
                            .unwrap()
                            .and_utc()
                            .timestamp_nanos_opt()
                            .unwrap(),
                    ),
                    None,
                ))
            }
        }
        timestamp
    }

    fn get_random_intervals(sample_size: u64) -> Vec<ScalarValue> {
        const MILLISECS_IN_ONE_DAY: i64 = 86_400_000;
        const NANOSECS_IN_ONE_DAY: i64 = 86_400_000_000_000;

        let vector_size = sample_size;
        let mut intervals = vec![];
        let mut rng = rand::rng();
        const SECS_IN_ONE_DAY: i32 = 86_400;
        const MICROSECS_IN_ONE_DAY: i64 = 86_400_000_000;
        for i in 0..vector_size {
            if i % 4 == 0 {
                let days = rng.random_range(0..5000);
                // to not break second precision
                let millis = rng.random_range(0..SECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_dt(days, millis));
            } else if i % 4 == 1 {
                let days = rng.random_range(0..5000);
                let millisec = rng.random_range(0..(MILLISECS_IN_ONE_DAY as i32));
                intervals.push(ScalarValue::new_interval_dt(days, millisec));
            } else if i % 4 == 2 {
                let days = rng.random_range(0..5000);
                // to not break microsec precision
                let nanosec = rng.random_range(0..MICROSECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            } else {
                let days = rng.random_range(0..5000);
                let nanosec = rng.random_range(0..NANOSECS_IN_ONE_DAY);
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            }
        }
        intervals
    }

    fn union_fields() -> UnionFields {
        [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn sparse_scalar_union_is_null() {
        let sparse_scalar = ScalarValue::Union(
            Some((0_i8, Box::new(ScalarValue::Int32(None)))),
            union_fields(),
            UnionMode::Sparse,
        );
        assert!(sparse_scalar.is_null());
    }

    #[test]
    fn dense_scalar_union_is_null() {
        let dense_scalar = ScalarValue::Union(
            Some((0_i8, Box::new(ScalarValue::Int32(None)))),
            union_fields(),
            UnionMode::Dense,
        );
        assert!(dense_scalar.is_null());
    }

    #[test]
    fn null_dictionary_scalar_produces_null_dictionary_array() {
        let dictionary_scalar = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Null),
        );
        assert!(dictionary_scalar.is_null());
        let dictionary_array = dictionary_scalar.to_array().unwrap();
        assert!(dictionary_array.is_null(0));
    }

    #[test]
    fn test_scalar_value_try_new_null() {
        let scalars = vec![
            ScalarValue::try_new_null(&DataType::Boolean).unwrap(),
            ScalarValue::try_new_null(&DataType::Int8).unwrap(),
            ScalarValue::try_new_null(&DataType::Int16).unwrap(),
            ScalarValue::try_new_null(&DataType::Int32).unwrap(),
            ScalarValue::try_new_null(&DataType::Int64).unwrap(),
            ScalarValue::try_new_null(&DataType::UInt8).unwrap(),
            ScalarValue::try_new_null(&DataType::UInt16).unwrap(),
            ScalarValue::try_new_null(&DataType::UInt32).unwrap(),
            ScalarValue::try_new_null(&DataType::UInt64).unwrap(),
            ScalarValue::try_new_null(&DataType::Float16).unwrap(),
            ScalarValue::try_new_null(&DataType::Float32).unwrap(),
            ScalarValue::try_new_null(&DataType::Float64).unwrap(),
            ScalarValue::try_new_null(&DataType::Decimal128(42, 42)).unwrap(),
            ScalarValue::try_new_null(&DataType::Decimal256(42, 42)).unwrap(),
            ScalarValue::try_new_null(&DataType::Utf8).unwrap(),
            ScalarValue::try_new_null(&DataType::LargeUtf8).unwrap(),
            ScalarValue::try_new_null(&DataType::Utf8View).unwrap(),
            ScalarValue::try_new_null(&DataType::Binary).unwrap(),
            ScalarValue::try_new_null(&DataType::BinaryView).unwrap(),
            ScalarValue::try_new_null(&DataType::FixedSizeBinary(42)).unwrap(),
            ScalarValue::try_new_null(&DataType::LargeBinary).unwrap(),
            ScalarValue::try_new_null(&DataType::Date32).unwrap(),
            ScalarValue::try_new_null(&DataType::Date64).unwrap(),
            ScalarValue::try_new_null(&DataType::Time32(TimeUnit::Second)).unwrap(),
            ScalarValue::try_new_null(&DataType::Time32(TimeUnit::Millisecond)).unwrap(),
            ScalarValue::try_new_null(&DataType::Time64(TimeUnit::Microsecond)).unwrap(),
            ScalarValue::try_new_null(&DataType::Time64(TimeUnit::Nanosecond)).unwrap(),
            ScalarValue::try_new_null(&DataType::Timestamp(TimeUnit::Second, None))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Timestamp(TimeUnit::Millisecond, None))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Timestamp(TimeUnit::Microsecond, None))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Timestamp(TimeUnit::Nanosecond, None))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Interval(IntervalUnit::DayTime))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Duration(TimeUnit::Second)).unwrap(),
            ScalarValue::try_new_null(&DataType::Duration(TimeUnit::Microsecond))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::Duration(TimeUnit::Nanosecond)).unwrap(),
            ScalarValue::try_new_null(&DataType::Null).unwrap(),
        ];
        assert!(scalars.iter().all(|s| s.is_null()));

        let field_ref = Arc::new(Field::new("foo", DataType::Int32, true));
        let map_field_ref = Arc::new(Field::new(
            "foo",
            DataType::Struct(Fields::from(vec![
                Field::new("bar", DataType::Utf8, true),
                Field::new("baz", DataType::Int32, true),
            ])),
            true,
        ));
        let scalars = vec![
            ScalarValue::try_new_null(&DataType::List(Arc::clone(&field_ref))).unwrap(),
            ScalarValue::try_new_null(&DataType::LargeList(Arc::clone(&field_ref)))
                .unwrap(),
            ScalarValue::try_new_null(&DataType::FixedSizeList(
                Arc::clone(&field_ref),
                42,
            ))
            .unwrap(),
            ScalarValue::try_new_null(&DataType::Struct(
                vec![Arc::clone(&field_ref)].into(),
            ))
            .unwrap(),
            ScalarValue::try_new_null(&DataType::Map(map_field_ref, false)).unwrap(),
            ScalarValue::try_new_null(&DataType::Union(
                UnionFields::new(vec![42], vec![field_ref]),
                UnionMode::Dense,
            ))
            .unwrap(),
        ];
        assert!(scalars.iter().all(|s| s.is_null()));
    }

    // `err.to_string()` depends on backtrace being present (may have backtrace appended)
    // `err.strip_backtrace()` also depends on backtrace being present (may have "This was likely caused by ..." stripped)
    fn assert_starts_with(actual: impl AsRef<str>, expected_prefix: impl AsRef<str>) {
        let actual = actual.as_ref();
        let expected_prefix = expected_prefix.as_ref();
        assert!(
            actual.starts_with(expected_prefix),
            "Expected '{actual}' to start with '{expected_prefix}'"
        );
    }

    #[test]
    fn test_new_default() {
        // Test numeric types
        assert_eq!(
            ScalarValue::new_default(&DataType::Int32).unwrap(),
            ScalarValue::Int32(Some(0))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::Float64).unwrap(),
            ScalarValue::Float64(Some(0.0))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::Boolean).unwrap(),
            ScalarValue::Boolean(Some(false))
        );

        // Test string types
        assert_eq!(
            ScalarValue::new_default(&DataType::Utf8).unwrap(),
            ScalarValue::Utf8(Some("".to_string()))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::LargeUtf8).unwrap(),
            ScalarValue::LargeUtf8(Some("".to_string()))
        );

        // Test binary types
        assert_eq!(
            ScalarValue::new_default(&DataType::Binary).unwrap(),
            ScalarValue::Binary(Some(vec![]))
        );

        // Test fixed size binary
        assert_eq!(
            ScalarValue::new_default(&DataType::FixedSizeBinary(5)).unwrap(),
            ScalarValue::FixedSizeBinary(5, Some(vec![0, 0, 0, 0, 0]))
        );

        // Test temporal types
        assert_eq!(
            ScalarValue::new_default(&DataType::Date32).unwrap(),
            ScalarValue::Date32(Some(0))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::Time32(TimeUnit::Second)).unwrap(),
            ScalarValue::Time32Second(Some(0))
        );

        // Test decimal types
        assert_eq!(
            ScalarValue::new_default(&DataType::Decimal128(10, 2)).unwrap(),
            ScalarValue::Decimal128(Some(0), 10, 2)
        );

        // Test list type
        let list_field = Field::new_list_field(DataType::Int32, true);
        let list_result =
            ScalarValue::new_default(&DataType::List(Arc::new(list_field.clone())))
                .unwrap();
        match list_result {
            ScalarValue::List(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr.value_length(0), 0); // empty list
            }
            _ => panic!("Expected List"),
        }

        // Test struct type
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let struct_result =
            ScalarValue::new_default(&DataType::Struct(struct_fields.clone())).unwrap();
        match struct_result {
            ScalarValue::Struct(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr.column(0).as_primitive::<Int32Type>().value(0), 0);
                assert_eq!(arr.column(1).as_string::<i32>().value(0), "");
            }
            _ => panic!("Expected Struct"),
        }

        // Test union type
        let union_fields = UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("f64", DataType::Float64, false),
            ],
        );
        let union_result = ScalarValue::new_default(&DataType::Union(
            union_fields.clone(),
            UnionMode::Sparse,
        ))
        .unwrap();
        match union_result {
            ScalarValue::Union(Some((type_id, value)), _, _) => {
                assert_eq!(type_id, 0);
                assert_eq!(*value, ScalarValue::Int32(Some(0)));
            }
            _ => panic!("Expected Union"),
        }
    }

    #[test]
    fn test_scalar_min() {
        // Test integer types
        assert_eq!(
            ScalarValue::min(&DataType::Int8),
            Some(ScalarValue::Int8(Some(i8::MIN)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::Int32),
            Some(ScalarValue::Int32(Some(i32::MIN)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::UInt8),
            Some(ScalarValue::UInt8(Some(0)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::UInt64),
            Some(ScalarValue::UInt64(Some(0)))
        );

        // Test float types
        assert_eq!(
            ScalarValue::min(&DataType::Float32),
            Some(ScalarValue::Float32(Some(f32::NEG_INFINITY)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::Float64),
            Some(ScalarValue::Float64(Some(f64::NEG_INFINITY)))
        );

        // Test decimal types
        let decimal_min = ScalarValue::min(&DataType::Decimal128(5, 2)).unwrap();
        match decimal_min {
            ScalarValue::Decimal128(Some(val), 5, 2) => {
                assert_eq!(val, -99999); // -999.99 with scale 2
            }
            _ => panic!("Expected Decimal128"),
        }

        // Test temporal types
        assert_eq!(
            ScalarValue::min(&DataType::Date32),
            Some(ScalarValue::Date32(Some(i32::MIN)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::Time32(TimeUnit::Second)),
            Some(ScalarValue::Time32Second(Some(0)))
        );
        assert_eq!(
            ScalarValue::min(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Some(ScalarValue::TimestampNanosecond(Some(i64::MIN), None))
        );

        // Test duration types
        assert_eq!(
            ScalarValue::min(&DataType::Duration(TimeUnit::Second)),
            Some(ScalarValue::DurationSecond(Some(i64::MIN)))
        );

        // Test unsupported types
        assert_eq!(ScalarValue::min(&DataType::Utf8), None);
        assert_eq!(ScalarValue::min(&DataType::Binary), None);
        assert_eq!(
            ScalarValue::min(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            None
        );
    }

    #[test]
    fn test_scalar_max() {
        // Test integer types
        assert_eq!(
            ScalarValue::max(&DataType::Int8),
            Some(ScalarValue::Int8(Some(i8::MAX)))
        );
        assert_eq!(
            ScalarValue::max(&DataType::Int32),
            Some(ScalarValue::Int32(Some(i32::MAX)))
        );
        assert_eq!(
            ScalarValue::max(&DataType::UInt8),
            Some(ScalarValue::UInt8(Some(u8::MAX)))
        );
        assert_eq!(
            ScalarValue::max(&DataType::UInt64),
            Some(ScalarValue::UInt64(Some(u64::MAX)))
        );

        // Test float types
        assert_eq!(
            ScalarValue::max(&DataType::Float32),
            Some(ScalarValue::Float32(Some(f32::INFINITY)))
        );
        assert_eq!(
            ScalarValue::max(&DataType::Float64),
            Some(ScalarValue::Float64(Some(f64::INFINITY)))
        );

        // Test decimal types
        let decimal_max = ScalarValue::max(&DataType::Decimal128(5, 2)).unwrap();
        match decimal_max {
            ScalarValue::Decimal128(Some(val), 5, 2) => {
                assert_eq!(val, 99999); // 999.99 with scale 2
            }
            _ => panic!("Expected Decimal128"),
        }

        // Test temporal types
        assert_eq!(
            ScalarValue::max(&DataType::Date32),
            Some(ScalarValue::Date32(Some(i32::MAX)))
        );
        assert_eq!(
            ScalarValue::max(&DataType::Time32(TimeUnit::Second)),
            Some(ScalarValue::Time32Second(Some(86_399))) // 23:59:59
        );
        assert_eq!(
            ScalarValue::max(&DataType::Time64(TimeUnit::Microsecond)),
            Some(ScalarValue::Time64Microsecond(Some(86_399_999_999))) // 23:59:59.999999
        );
        assert_eq!(
            ScalarValue::max(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Some(ScalarValue::TimestampNanosecond(Some(i64::MAX), None))
        );

        // Test duration types
        assert_eq!(
            ScalarValue::max(&DataType::Duration(TimeUnit::Millisecond)),
            Some(ScalarValue::DurationMillisecond(Some(i64::MAX)))
        );

        // Test unsupported types
        assert_eq!(ScalarValue::max(&DataType::Utf8), None);
        assert_eq!(ScalarValue::max(&DataType::Binary), None);
        assert_eq!(
            ScalarValue::max(&DataType::Struct(Fields::from(vec![Field::new(
                "field",
                DataType::Int32,
                true
            )]))),
            None
        );
    }

    #[test]
    fn test_min_max_float16() {
        // Test Float16 min and max
        let min_f16 = ScalarValue::min(&DataType::Float16).unwrap();
        match min_f16 {
            ScalarValue::Float16(Some(val)) => {
                assert_eq!(val, f16::NEG_INFINITY);
            }
            _ => panic!("Expected Float16"),
        }

        let max_f16 = ScalarValue::max(&DataType::Float16).unwrap();
        match max_f16 {
            ScalarValue::Float16(Some(val)) => {
                assert_eq!(val, f16::INFINITY);
            }
            _ => panic!("Expected Float16"),
        }
    }

    #[test]
    fn test_new_default_interval() {
        // Test all interval types
        assert_eq!(
            ScalarValue::new_default(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap(),
            ScalarValue::IntervalYearMonth(Some(0))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::Interval(IntervalUnit::DayTime)).unwrap(),
            ScalarValue::IntervalDayTime(Some(IntervalDayTime::ZERO))
        );
        assert_eq!(
            ScalarValue::new_default(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap(),
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::ZERO))
        );
    }

    #[test]
    fn test_min_max_with_timezone() {
        let tz = Some(Arc::from("UTC"));

        // Test timestamp with timezone
        let min_ts =
            ScalarValue::min(&DataType::Timestamp(TimeUnit::Second, tz.clone())).unwrap();
        match min_ts {
            ScalarValue::TimestampSecond(Some(val), Some(tz_str)) => {
                assert_eq!(val, i64::MIN);
                assert_eq!(tz_str.as_ref(), "UTC");
            }
            _ => panic!("Expected TimestampSecond with timezone"),
        }

        let max_ts =
            ScalarValue::max(&DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))
                .unwrap();
        match max_ts {
            ScalarValue::TimestampMillisecond(Some(val), Some(tz_str)) => {
                assert_eq!(val, i64::MAX);
                assert_eq!(tz_str.as_ref(), "UTC");
            }
            _ => panic!("Expected TimestampMillisecond with timezone"),
        }
    }
}
