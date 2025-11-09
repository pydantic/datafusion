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

//! Utilities for pretty-formatting arrays

use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};

/// Formats an array as a comma-separated list enclosed in brackets.
///
/// This function uses Arrow's `ArrayFormatter` to display array values in a human-readable
/// format. Null values are displayed as "null".
///
/// # Example
///
/// ```
/// use arrow::array::{Int32Array, ArrayRef};
/// use datafusion_common::utils::pretty::pretty_format_array;
/// use std::sync::Arc;
///
/// let array = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
/// let formatted = pretty_format_array(array.as_ref());
/// assert_eq!(formatted, "[1, 2, null, 4]");
/// ```
pub fn pretty_format_array(array: &dyn Array) -> String {
    let options = FormatOptions::default().with_null("null");
    match ArrayFormatter::try_new(array, &options) {
        Ok(formatter) => {
            let values: Vec<String> = (0..array.len())
                .map(|i| formatter.value(i).to_string())
                .collect();
            format!("[{}]", values.join(", "))
        }
        Err(_) => "[<formatting error>]".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_pretty_format_array_int32() {
        let array =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let formatted = pretty_format_array(array.as_ref());
        assert_eq!(formatted, "[1, 2, null, 4]");
    }

    #[test]
    fn test_pretty_format_array_strings() {
        let array =
            Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")])) as ArrayRef;
        let formatted = pretty_format_array(array.as_ref());
        assert_eq!(formatted, "[foo, null, bar]");
    }

    #[test]
    fn test_pretty_format_array_empty() {
        let array = Arc::new(Int32Array::from(vec![] as Vec<Option<i32>>)) as ArrayRef;
        let formatted = pretty_format_array(array.as_ref());
        assert_eq!(formatted, "[]");
    }

    #[test]
    fn test_pretty_format_array_floats() {
        let array = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(2.7),
            None,
            Some(3.14),
        ])) as ArrayRef;
        let formatted = pretty_format_array(array.as_ref());
        assert_eq!(formatted, "[1.5, 2.7, null, 3.14]");
    }

    #[test]
    fn test_pretty_format_array_all_nulls() {
        let array = Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef;
        let formatted = pretty_format_array(array.as_ref());
        assert_eq!(formatted, "[null, null, null]");
    }
}
