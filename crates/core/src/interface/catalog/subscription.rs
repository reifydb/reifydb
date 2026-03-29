// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion,
	encoded::shape::{RowShape, RowShapeField},
	interface::catalog::{
		id::{NamespaceId, SubscriptionColumnId, SubscriptionId},
		key::PrimaryKey,
	},
};

/// Implicit column names for subscriptions
pub const IMPLICIT_COLUMN_OP: &str = "_op";

/// A column definition for a subscription.
/// Simpler than regular Column - only has id, name, and type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionColumn {
	pub id: SubscriptionColumnId,
	pub name: String,
	pub ty: Type,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
	pub id: SubscriptionId,
	// Note: Subscriptions do NOT have names - identified only by ID
	pub columns: Vec<SubscriptionColumn>,
	pub primary_key: Option<PrimaryKey>,
	pub acknowledged_version: CommitVersion,
}

impl Subscription {
	/// Returns the implicit columns that are automatically added to all subscriptions
	pub fn implicit_columns() -> Vec<SubscriptionColumn> {
		vec![SubscriptionColumn {
			id: SubscriptionColumnId(u64::MAX - 2), // Use high IDs for implicit columns
			name: IMPLICIT_COLUMN_OP.to_string(),
			ty: Type::Uint1, // 1=INSERT, 2=UPDATE, 3=DELETE
		}]
	}

	/// Returns all columns including user-defined and implicit columns
	pub fn all_columns(&self) -> Vec<SubscriptionColumn> {
		let mut all = self.columns.clone();
		all.extend(Self::implicit_columns());
		all
	}
}

impl From<&Subscription> for RowShape {
	fn from(value: &Subscription) -> Self {
		// Use only user-defined columns for shape (implicit columns like _op removed)
		let fields = value
			.columns
			.iter()
			.map(|col| RowShapeField::unconstrained(col.name.clone(), col.ty.clone()))
			.collect();
		RowShape::new(fields)
	}
}

/// Returns the flow name for a subscription using the deterministic naming convention.
/// Subscription flows are always created in the system namespace.
pub fn subscription_flow_name(id: SubscriptionId) -> String {
	format!("subscription_{}", id.0)
}

/// Returns the namespace ID where subscription flows are created (system namespace).
pub const fn subscription_flow_namespace() -> NamespaceId {
	NamespaceId::SYSTEM
}
