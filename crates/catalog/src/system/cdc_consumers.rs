// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::cdc_consumers::*, vtable::CDC_CONSUMERS};

/// Returns the static definition for the system.cdc_consumers virtual table
/// This table exposes information about all CDC consumers and their checkpoints
pub fn cdc_consumers() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: CDC_CONSUMERS,
			namespace: NamespaceId::SYSTEM,
			name: "cdc_consumers".to_string(),
			columns: vec![
				Column {
					id: CONSUMER_ID,
					name: "consumer_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CHECKPOINT,
					name: "checkpoint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
