// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::{ColumnId, NamespaceId},
	vtable::{VTable, VTableId},
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

pub fn metrics_cdc_vtable(id: VTableId, namespace: NamespaceId) -> Arc<VTable> {
	Arc::new(VTable {
		id,
		namespace,
		name: "current".to_string(),
		columns: vec![
			col(1, 0, "id", ValueType::Uint8),
			col(2, 1, "namespace_id", ValueType::Uint8),
			col(3, 2, "key_bytes", ValueType::Uint8),
			col(4, 3, "value_bytes", ValueType::Uint8),
			col(5, 4, "total_bytes", ValueType::Uint8),
			col(6, 5, "count", ValueType::Uint8),
		],
	})
}

fn col(id: u64, index: u8, name: &str, ty: ValueType) -> Column {
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
