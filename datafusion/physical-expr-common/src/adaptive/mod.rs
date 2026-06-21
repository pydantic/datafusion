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
//! adapt conjunct evaluation *order*. Both need the same ingredients:
//!
//! - per-predicate online **selectivity + cost** measurement with confidence
//!   intervals — [`SelectivityStats`];
//! - a concurrent **registry** keyed by a caller-local [`FilterId`], with
//!   per-predicate skip flags so an optional predicate can be made a no-op
//!   mid-stream — [`AdaptiveStatsRegistry`];
//! - shared **champion/challenger coordination** over an opaque arrangement
//!   type, so a proposed change (a new conjunct order, a new filter
//!   placement) is adopted only on a measured, statistically separated A/B
//!   win shared across all of a consumer's streams —
//!   [`AdaptiveArbiter`].
//!
//! What stays with each consumer is *policy*: the per-batch effectiveness
//! metric it feeds in, the proposal it computes over the snapshots, what an
//! arrangement *is* and how to execute one. This module intentionally
//! contains no placement or ordering logic.

pub mod arbiter;
pub mod registry;
pub mod stats;

pub use arbiter::{AdaptiveArbiter, TRIAL_PAIRS, TrialUpdate};
pub use registry::AdaptiveStatsRegistry;
pub use stats::{FilterId, SelectivityStats};
