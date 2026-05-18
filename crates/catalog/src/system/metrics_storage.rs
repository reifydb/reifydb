// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::{ColumnId, NamespaceId},
	vtable::{VTable, VTableId},
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

pub fn metrics_storage_vtable(id: VTableId, local_name: &str) -> Arc<VTable> {
	Arc::new(VTable {
		id,
		namespace: NamespaceId::SYSTEM_METRICS_STORAGE,
		name: local_name.to_string(),
		columns: vec![
			col(1, 0, "id", Type::Uint8),
			col(2, 1, "namespace_id", Type::Uint8),
			col(3, 2, "tier", Type::Utf8),
			col(4, 3, "current_key_bytes", Type::Uint8),
			col(5, 4, "current_value_bytes", Type::Uint8),
			col(6, 5, "current_total_bytes", Type::Uint8),
			col(7, 6, "current_count", Type::Uint8),
			col(8, 7, "historical_key_bytes", Type::Uint8),
			col(9, 8, "historical_value_bytes", Type::Uint8),
			col(10, 9, "historical_total_bytes", Type::Uint8),
			col(11, 10, "historical_count", Type::Uint8),
			col(12, 11, "total_bytes", Type::Uint8),
		],
	})
}

fn col(id: u64, index: u8, name: &str, ty: Type) -> Column {
	Column {
		id: ColumnId(id),
		name: name.to_string(),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		index: ColumnIndex(index),
		auto_increment: false,
		dictionary_id: None,
	}
}
