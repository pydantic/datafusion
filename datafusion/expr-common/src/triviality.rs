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

//! Triviality classification for expressions and function arguments.

/// Classification of argument triviality for scalar functions.
///
/// This enum is used by [`ScalarUDFImpl::triviality_with_args`] to allow
/// functions to make context-dependent decisions about whether they are
/// trivial based on the nature of their arguments.
///
/// For example, `get_field(struct_col, 'field_name')` is trivial (static field
/// lookup), but `get_field(struct_col, key_col)` is not (dynamic per-row lookup).
///
/// [`ScalarUDFImpl::triviality`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.ScalarUDFImpl.html#tymethod.triviality
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgTriviality {
    /// Argument is a literal constant value or an expression that can be
    /// evaluated to a constant at planning time.
    Literal,
    /// Argument is a simple column reference.
    Column,
    /// Argument is a complex expression that declares itself trivial.
    /// For example, if `get_field(struct_col, 'field_name')` is implemented as a
    /// trivial expression, then it would return this variant.
    /// Then `other_trivial_function(get_field(...), 42)` could also be classified as
    /// a trivial expression using the knowledge that `get_field(...)` is trivial.
    TrivialExpr,
    /// Argument is a complex expression that declares itself non-trivial.
    /// For example, `min(col1 + col2)` is non-trivial because it requires per-row computation.
    NonTrivial,
}
