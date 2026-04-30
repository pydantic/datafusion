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
//! # Deserialization
//!
//! Decoding goes through a [`PhysicalExprRegistry`]: implementers register a
//! constructor for each tag, and the registry's `deserialize_*` methods
//! dispatch on the tag to call the right constructor. Implementers opt in
//! by implementing [`PhysicalExprDeserialize`].
//!
//! The deserialization path is currently JSON-only — the trait method takes a
//! [`serde_json::Value`] for the body. A future revision will replace that
//! with a streaming, format-agnostic API; the current shape is enough to
//! cover JSON-based debugging and tests.
//!
//! Wire stability across DataFusion versions is **not** a goal of this layer.
//! Use the proto crate for stable cross-version wire formats.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{Result, exec_datafusion_err};
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

/// Trait implemented by expressions that opt in to deserialization.
///
/// Each implementer pairs a stable string tag (`TAG`) with a constructor
/// (`deserialize`) that rebuilds the expression from a JSON body. The tag
/// must be globally unique and must agree with what the type returns from
/// [`PhysicalExpr::serde_tag`] — the convention is to use `TAG` for both:
///
/// ```ignore
/// impl PhysicalExpr for MyExpr {
///     fn serde_tag(&self) -> &'static str { Self::TAG }
///     // ...
/// }
/// ```
///
/// Trait-object children (`Arc<dyn PhysicalExpr>`) are deserialized by
/// recursing back through the registry — see [`PhysicalExprRegistry::deserialize_value`].
pub trait PhysicalExprDeserialize: PhysicalExpr + Sized {
    /// Stable identifier for this expression in serialized form.
    const TAG: &'static str;

    /// Rebuild `Self` from the body of an envelope.
    ///
    /// `data` is the value of the `"data"` field of the `{tag, data}`
    /// envelope. For expressions with no trait-object children, the typical
    /// implementation is `serde_json::from_value(data).map_err(...)`.
    /// Expressions with children should deserialize the children's bodies as
    /// `serde_json::Value` and recurse via `ctx.registry().deserialize_value`.
    fn deserialize(ctx: &DeserializeContext<'_>, data: serde_json::Value)
    -> Result<Self>;
}

/// Context passed to [`PhysicalExprDeserialize::deserialize`]. Carries the
/// registry so implementers can recursively deserialize child expressions.
pub struct DeserializeContext<'reg> {
    registry: &'reg PhysicalExprRegistry,
}

impl<'reg> DeserializeContext<'reg> {
    pub fn registry(&self) -> &'reg PhysicalExprRegistry {
        self.registry
    }
}

type Constructor =
    fn(&DeserializeContext<'_>, serde_json::Value) -> Result<Arc<dyn PhysicalExpr>>;

/// Registry mapping serialization tags to constructors.
///
/// Built up with [`PhysicalExprRegistry::empty`] and
/// [`PhysicalExprRegistry::with`] in builder style. The `physical-expr` crate
/// provides a `PhysicalExprRegistry::new()` that returns a registry
/// pre-populated with all of DataFusion's built-in expressions.
pub struct PhysicalExprRegistry {
    constructors: HashMap<&'static str, Constructor>,
}

impl PhysicalExprRegistry {
    /// Returns an empty registry. Use [`Self::with`] to add constructors.
    pub fn empty() -> Self {
        Self {
            constructors: HashMap::new(),
        }
    }

    /// Register the constructor for `T`.
    ///
    /// Panics if a constructor was already registered under `T::TAG` — see
    /// [`Self::contains_tag`] if you need to check ahead of time.
    pub fn with<T: PhysicalExprDeserialize + 'static>(mut self) -> Self {
        let tag = T::TAG;
        if tag.is_empty() {
            panic!(
                "PhysicalExprDeserialize::TAG must not be empty (got empty for type registered with PhysicalExprRegistry::with)"
            );
        }
        let prev = self.constructors.insert(tag, |ctx, data| {
            T::deserialize(ctx, data).map(|v| Arc::new(v) as Arc<dyn PhysicalExpr>)
        });
        if prev.is_some() {
            panic!("PhysicalExprRegistry: duplicate registration for tag {tag:?}");
        }
        self
    }

    /// Returns true if a constructor is registered for `tag`.
    pub fn contains_tag(&self, tag: &str) -> bool {
        self.constructors.contains_key(tag)
    }

    /// Decode a JSON-serialized expression.
    ///
    /// `s` must be a `{tag, data}` envelope produced by serializing an
    /// `Arc<dyn PhysicalExpr>` (or `dyn PhysicalExpr`) through this crate's
    /// [`Serialize`] impl.
    pub fn deserialize_json(&self, s: &str) -> Result<Arc<dyn PhysicalExpr>> {
        let value: serde_json::Value = serde_json::from_str(s).map_err(|e| {
            exec_datafusion_err!("failed to parse PhysicalExpr JSON: {e}")
        })?;
        self.deserialize_value(value)
    }

    /// Decode an already-parsed JSON value as an expression. Used both at the
    /// top level and recursively for child expressions.
    pub fn deserialize_value(
        &self,
        value: serde_json::Value,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let mut obj = match value {
            serde_json::Value::Object(o) => o,
            other => {
                return Err(exec_datafusion_err!(
                    "expected PhysicalExpr envelope object, got {other}"
                ));
            }
        };

        let tag = match obj.remove("tag") {
            Some(serde_json::Value::String(s)) => s,
            Some(other) => {
                return Err(exec_datafusion_err!(
                    "PhysicalExpr envelope `tag` must be a string, got {other}"
                ));
            }
            None => {
                return Err(exec_datafusion_err!(
                    "PhysicalExpr envelope missing `tag` field"
                ));
            }
        };
        let data = obj.remove("data").ok_or_else(|| {
            exec_datafusion_err!("PhysicalExpr envelope missing `data` field")
        })?;

        let constructor = *self.constructors.get(tag.as_str()).ok_or_else(|| {
            exec_datafusion_err!(
                "no PhysicalExpr registered under tag {:?}; registered tags: {:?}",
                tag,
                self.constructors.keys().copied().collect::<Vec<_>>()
            )
        })?;

        let ctx = DeserializeContext { registry: self };
        constructor(&ctx, data)
    }
}

impl Default for PhysicalExprRegistry {
    fn default() -> Self {
        Self::empty()
    }
}
