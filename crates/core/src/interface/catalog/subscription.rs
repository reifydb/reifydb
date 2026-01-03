// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::Type;
use serde::{Deserialize, Serialize};

use crate::{
	CommitVersion,
	interface::{PrimaryKeyDef, SubscriptionColumnId, SubscriptionId},
	value::encoded::EncodedValuesNamedLayout,
};

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

impl From<&SubscriptionDef> for EncodedValuesNamedLayout {
	fn from(value: &SubscriptionDef) -> Self {
		EncodedValuesNamedLayout::new(value.columns.iter().map(|col| (col.name.clone(), col.ty)))
	}
}
