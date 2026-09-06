// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, NamespaceId, RelationshipId, TableId},
		relationship::{Relationship, RelationshipCardinality, RelationshipJunction},
	},
	key::catalog::RelationshipKey,
	return_internal_error,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::relationship::shape::relationship as relationship_shape};

impl CatalogStore {
	pub(crate) fn list_relationships(rx: &mut Transaction<'_>) -> Result<Vec<Relationship>> {
		let mut entries = Vec::new();
		{
			let stream = rx.range(RelationshipKey::full_scan(), RangeScope::All, 1024)?;
			for entry in stream {
				entries.push(entry?);
			}
		}

		let mut result = Vec::with_capacity(entries.len());
		for entry in entries {
			result.push(decode_relationship_row(EncodedCatalogRow::view(&entry.bytes))?);
		}
		Ok(result)
	}
}

pub(crate) fn decode_relationship_row(bytes: &EncodedCatalogRow) -> Result<Relationship> {
	let id = RelationshipId(relationship_shape::get_id(bytes));
	let namespace = NamespaceId(relationship_shape::get_namespace_id(bytes));
	let name = relationship_shape::get_name(bytes).to_string();
	let source_table = TableId(relationship_shape::get_source_table_id(bytes));
	let source_column = ColumnId(relationship_shape::get_source_column_id(bytes));
	let target_table = TableId(relationship_shape::get_target_table_id(bytes));
	let target_column = ColumnId(relationship_shape::get_target_column_id(bytes));

	let junction_table_raw = relationship_shape::get_junction_table_id(bytes);
	let junction = if junction_table_raw == 0 {
		None
	} else {
		let source_column = ColumnId(relationship_shape::get_junction_source_column_id(bytes));
		let target_column = ColumnId(relationship_shape::get_junction_target_column_id(bytes));
		Some(RelationshipJunction {
			table: TableId(junction_table_raw),
			source_column,
			target_column,
		})
	};

	let cardinality_code = relationship_shape::get_cardinality(bytes);
	let cardinality = match RelationshipCardinality::from_code(cardinality_code) {
		Some(c) => c,
		None => return_internal_error!(format!("invalid relationship cardinality code: {}", cardinality_code)),
	};

	Ok(Relationship {
		id,
		namespace,
		name,
		source_table,
		source_column,
		target_table,
		target_column,
		junction,
		cardinality,
	})
}
