// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::flow_lags::*, vtable::FLOW_LAGS};

/// Returns the static definition for the system.flow_lags virtual table.
///
/// This table exposes per-source lag information for each flow,
/// showing how far behind each flow is for each of its subscribed sources.
pub fn flow_lags() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: FLOW_LAGS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_lags".to_string(),
			columns: vec![
				ColumnDef {
					id: FLOW_ID,
					name: "flow_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: PRIMITIVE_ID,
					name: "primitive_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: LAG,
					name: "lag".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
