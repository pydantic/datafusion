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

//! [`PhysicalExprRegistry::new`] — a registry pre-populated with all of
//! DataFusion's built-in physical expressions.
//!
//! See [`datafusion_physical_expr_common::serde`] for the underlying
//! serialization/deserialization machinery and the
//! [`PhysicalExprDeserialize`] trait.

pub use datafusion_physical_expr_common::serde::{
    DeserializeContext, PhysicalExprDeserialize, PhysicalExprRegistry,
};

use crate::expressions::Column;

/// Returns a [`PhysicalExprRegistry`] with all of DataFusion's built-in
/// physical expressions registered.
///
/// Equivalent to `PhysicalExprRegistry::empty().with::<Column>()...`. Use
/// [`PhysicalExprRegistry::with`] to layer additional custom expressions on
/// top.
pub fn default_registry() -> PhysicalExprRegistry {
    PhysicalExprRegistry::empty().with::<Column>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    #[test]
    fn column_json_round_trip() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 3));

        let json = serde_json::to_string(&expr).unwrap();
        // {tag, data} envelope, with Column body { name, index }
        assert!(json.contains("\"tag\":\"Column\""));
        assert!(json.contains("\"name\":\"a\""));
        assert!(json.contains("\"index\":3"));

        let registry = default_registry();
        let back = registry.deserialize_json(&json).unwrap();
        assert!(expr.dyn_eq(back.as_ref()));
    }

    #[test]
    fn unknown_tag_errors() {
        let registry = PhysicalExprRegistry::empty();
        let json = r#"{"tag":"Column","data":{"name":"a","index":0}}"#;
        let err = registry.deserialize_json(json).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no PhysicalExpr registered under tag"),
            "{msg}"
        );
    }
}
