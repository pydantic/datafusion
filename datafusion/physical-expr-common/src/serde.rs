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

//! Format-agnostic serialization for [`PhysicalExpr`].
//!
//! This module provides the building blocks that let any [`serde`]-compatible
//! format (JSON for debugging, protobuf via a serde adapter, etc.) serialize
//! an `Arc<dyn PhysicalExpr>` without being coupled to a specific wire format.
//!
//! # Encoding
//!
//! Each expression is encoded as a `{tag, data}` envelope. The `tag` is the
//! string returned from [`PhysicalExpr::serde_tag`] and is used by the
//! deserialization side to dispatch back to a concrete type. The `data` is
//! the body produced by [`PhysicalExpr::erased_serialize`].
//!
//! `Arc<dyn PhysicalExpr>` is serializable too: serde's `rc` feature provides
//! a blanket `Serialize` impl for `Arc<T>` whenever `T: Serialize`.
//!
//! # Opt-in
//!
//! Both `serde_tag` and `erased_serialize` have default implementations on the
//! `PhysicalExpr` trait. Expressions that don't override them are not
//! serializable — the [`Serialize`] impl below will fail at runtime with a
//! descriptive error.
//!
//! Wire stability across DataFusion versions is **not** a goal of this layer.
//! Use the proto crate for stable cross-version wire formats.

use serde::Serialize;
use serde::ser::{Error as _, SerializeStruct, Serializer};

use crate::physical_expr::PhysicalExpr;

impl Serialize for dyn PhysicalExpr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let tag = self.serde_tag();
        if tag.is_empty() {
            return Err(S::Error::custom(format!(
                "PhysicalExpr serialization not implemented for {self}"
            )));
        }

        let body = self.erased_serialize();
        let mut state = serializer.serialize_struct("PhysicalExpr", 2)?;
        state.serialize_field("tag", tag)?;
        // `&dyn erased_serde::Serialize` implements `serde::Serialize` via the
        // `serialize_trait_object!(Serialize)` invocation inside erased_serde.
        state.serialize_field("data", &*body)?;
        state.end()
    }
}

/// Sentinel returned by the default [`PhysicalExpr::erased_serialize`] impl.
///
/// Serializing this value fails with a descriptive error. Used so that the
/// trait method can have a default implementation without needing
/// specialization or a separate "is serializable" branch outside the tag
/// check.
#[doc(hidden)]
pub struct NotSerializable(pub String);

impl Serialize for NotSerializable {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom(&self.0))
    }
}
