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
/// [`ScalarUDFImpl::triviality_with_args`]: crate::ScalarUDFImpl::triviality_with_args
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgTriviality {
    /// Argument is a literal constant value or an expression that can be
    /// evaluated to a constant at planning time.
    Literal,
    /// Argument is a simple column reference.
    Column,
    /// Argument is a complex expressions that declares itself trivial.
    /// For example, if `get_field(struct_col, 'field_name')` is implemented as a
    /// trivial expression, then it would return this variant.
    /// Then `other_trivial_function(get_field(...), 42)` could also be classified as
    /// a trivial expression using the knowledge that `get_field(...)` is trivial.
    TrivialExpr,
    /// Argument is a complex expression that declares itself non-trivial.
    /// For example, `min(col1 + col2)` is non-trivial because it requires per-row computation.
    NonTrivial,
}

impl ArgTriviality {
    /// Returns true if this triviality classification indicates a trivial
    /// (cheap to evaluate) expression.
    ///
    /// Note that only `ArgTriviality::TrivialExpr` is considered trivial here.
    /// Literal and `Column` are not considered trivial because they
    /// depend on context (e.g. a literal constant may be expensive to compute
    /// if it has to be broadcast to many rows).
    /// 
    /// For example, operations like `get_field(struct_col, 'field_name')` are
    /// trivial because they can be evaluated in O(1) time per batch of data,
    /// whereas `min(col1 + col2)` is non-trivial because it requires O(n) time
    /// per batch of data.
    pub fn is_trivial(&self) -> bool {
        matches!(self, ArgTriviality::TrivialExpr)
    }
}
