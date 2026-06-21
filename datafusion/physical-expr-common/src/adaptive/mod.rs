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

//! Shared substrate for adaptive (measurement-driven) filtering.
//!
//! Adaptive filter policies observe how predicates behave at runtime and
//! re-decide accordingly — the parquet scan adapts filter *placement*
//! (row-level vs. post-scan vs. dropped), and an adaptive `FilterExec` could
//! adapt conjunct evaluation *order*. The first shared ingredient is
//! per-predicate online **selectivity + cost** measurement with confidence
//! intervals — [`SelectivityStats`].
//!
//! What stays with each consumer is *policy*: the per-batch effectiveness
//! metric it feeds in and what it does with the measurements. This module
//! intentionally contains no placement or ordering logic.

pub mod stats;

pub use stats::{FilterId, SelectivityStats};
