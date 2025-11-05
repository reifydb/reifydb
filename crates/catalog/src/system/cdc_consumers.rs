// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::cdc_consumers::*, table_virtual::CDC_CONSUMERS};

/// Returns the static definition for the system.cdc_consumers virtual table
/// This table exposes information about all CDC consumers and their checkpoints
pub fn cdc_consumers() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
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
				},
				ColumnDef {
					id: CHECKPOINT,
					name: "checkpoint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
