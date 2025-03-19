use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExpr;

/// A source of dynamic runtime filters.
///
/// During query execution, operators implementing this trait can provide
/// filter expressions that other operators can use to dynamically prune data.
pub trait DynamicFilterSource: Send + Sync + std::fmt::Debug + 'static {
    /// Returns a list of filter expressions that can be used for dynamic pruning.
    fn current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>>;
}
