// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::flow_nodes::*, table_virtual::FLOW_NODES};

/// Returns the static definition for the system.flow_nodes virtual table
/// This table exposes information about all flow nodes in the database
pub fn flow_nodes() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: FLOW_NODES,
			namespace: NamespaceId(1), // system namespace
			name: "flow_nodes".to_string(),
			columns: vec![
				ColumnDef {
					id: ID,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: FLOW_ID,
					name: "flow_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
				ColumnDef {
					id: NODE_TYPE,
					name: "node_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1), /* 0-255 for node
					                                                         * type discriminator */
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
				},
				ColumnDef {
					id: DATA,
					name: "data".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Blob), // Serialized node data
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
