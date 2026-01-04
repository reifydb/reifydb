// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::Type;
use serde::{Deserialize, Serialize};

use crate::{
	CommitVersion,
	interface::{PrimaryKeyDef, SubscriptionColumnId, SubscriptionId},
	value::encoded::EncodedValuesNamedLayout,
};

/// Implicit column names for subscriptions
pub const IMPLICIT_COLUMN_OP: &str = "_op";
pub const IMPLICIT_COLUMN_VERSION: &str = "_version";
pub const IMPLICIT_COLUMN_SEQUENCE: &str = "_sequence";

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
		vec![
			SubscriptionColumnDef {
				id: SubscriptionColumnId(u64::MAX - 2), // Use high IDs for implicit columns
				name: IMPLICIT_COLUMN_OP.to_string(),
				ty: Type::Uint1, // 0=INSERT, 1=UPDATE, 2=DELETE
			},
			SubscriptionColumnDef {
				id: SubscriptionColumnId(u64::MAX - 1),
				name: IMPLICIT_COLUMN_VERSION.to_string(),
				ty: Type::Uint8, // CommitVersion as u64
			},
			SubscriptionColumnDef {
				id: SubscriptionColumnId(u64::MAX),
				name: IMPLICIT_COLUMN_SEQUENCE.to_string(),
				ty: Type::Uint2, // u16
			},
		]
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
