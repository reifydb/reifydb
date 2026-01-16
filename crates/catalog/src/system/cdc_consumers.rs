// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::cdc_consumers::*, vtable::CDC_CONSUMERS};

/// Returns the static definition for the system.cdc_consumers virtual table
/// This table exposes information about all CDC consumers and their checkpoints
pub fn cdc_consumers() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: CDC_CONSUMERS,
			namespace: NamespaceId(1), // system namespace
			name: "cdc_consumers".to_string(),
			columns: vec![
				ColumnDef {
					id: CONSUMER_ID,
					name: "consumer_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CHECKPOINT,
					name: "checkpoint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
