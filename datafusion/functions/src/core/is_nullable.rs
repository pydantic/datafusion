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

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::{ColumnarValue, Documentation, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Returns true if the expression's field is nullable, false otherwise. This reflects the schema-level nullability, not whether a specific runtime value is NULL.",
    syntax_example = "is_nullable(expression)",
    sql_example = r#"```sql
> select is_nullable(name), is_nullable(ts) from table_with_metadata limit 1;
+----------------------------+------------------------+
| is_nullable(table_with_metadata.name) | is_nullable(table_with_metadata.ts) |
+----------------------------+------------------------+
| true                       | false                  |
+----------------------------+------------------------+
```
"#,
    argument(
        name = "expression",
        description = "Expression to evaluate. The expression can be a constant, column, or function, and any combination of operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsNullableFunc {
    signature: Signature,
}

impl Default for IsNullableFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNullableFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsNullableFunc {
    fn name(&self) -> &str {
        "is_nullable"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [_arg] = take_function_args(self.name(), args.args)?;
        let nullable = args.arg_fields[0].is_nullable();
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(nullable))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
