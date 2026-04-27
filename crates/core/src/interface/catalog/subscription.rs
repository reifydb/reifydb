// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

/// Implicit column names for subscriptions
pub const IMPLICIT_COLUMN_OP: &str = "_op";

/// Type-erased interface for inspecting subscription data.
///
/// Implemented by the subscription subsystem and registered in IoC as
/// `Arc<dyn SubscriptionInspector>`. Used by the `subscription::inspect`
/// generator procedure in the routine crate without depending on the
/// subscription subsystem crate.
pub trait SubscriptionInspector: Send + Sync {
	/// Drain all available rows from a subscription's buffer,
	/// merged into a single Columns result.
	fn inspect(&self, id: SubscriptionId) -> Option<Columns>;

	/// Return the IDs of all currently active subscriptions.
	fn active_subscriptions(&self) -> Vec<SubscriptionId>;

	/// Return the number of columns in a subscription's schema.
	fn column_count(&self, id: &SubscriptionId) -> Option<usize>;
}

/// Convenience type alias for IoC registration.
pub type SubscriptionInspectorRef = Arc<dyn SubscriptionInspector>;
