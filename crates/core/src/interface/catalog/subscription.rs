// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion,
	encoded::named::EncodedValuesNamedLayout,
	interface::catalog::{
		id::{NamespaceId, SubscriptionColumnId, SubscriptionId},
		key::PrimaryKeyDef,
	},
};

/// Implicit column names for subscriptions
pub const IMPLICIT_COLUMN_OP: &str = "_op";

/// A column definition for a subscription.
/// Simpler than regular ColumnDef - only has id, name, and type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionColumnDef {
	pub id: SubscriptionColumnId,
	pub name: String,
	pub ty: Type,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionDef {
	pub id: SubscriptionId,
	// Note: Subscriptions do NOT have names - identified only by ID
	pub columns: Vec<SubscriptionColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
	pub acknowledged_version: CommitVersion,
}

impl SubscriptionDef {
	/// Returns the implicit columns that are automatically added to all subscriptions
	pub fn implicit_columns() -> Vec<SubscriptionColumnDef> {
		vec![SubscriptionColumnDef {
			id: SubscriptionColumnId(u64::MAX - 2), // Use high IDs for implicit columns
			name: IMPLICIT_COLUMN_OP.to_string(),
			ty: Type::Uint1, // 0=INSERT, 1=UPDATE, 2=DELETE
		}]
	}

	/// Returns all columns including user-defined and implicit columns
	pub fn all_columns(&self) -> Vec<SubscriptionColumnDef> {
		let mut all = self.columns.clone();
		all.extend(Self::implicit_columns());
		all
	}
}

impl From<&SubscriptionDef> for EncodedValuesNamedLayout {
	fn from(value: &SubscriptionDef) -> Self {
		// Use all columns (user + implicit) for layout
		EncodedValuesNamedLayout::new(value.all_columns().iter().map(|col| (col.name.clone(), col.ty)))
	}
}

/// Returns the flow name for a subscription using the deterministic naming convention.
/// Subscription flows are always created in the system namespace.
pub fn subscription_flow_name(id: SubscriptionId) -> String {
	format!("subscription_{}", id.0)
}

/// Returns the namespace ID where subscription flows are created (system namespace).
pub const fn subscription_flow_namespace() -> NamespaceId {
	NamespaceId(1)
}
