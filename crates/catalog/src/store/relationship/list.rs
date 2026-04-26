// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		id::{ColumnId, NamespaceId, RelationshipId, TableId},
		relationship::{Relationship, RelationshipCardinality, RelationshipJunction},
	},
	key::relationship::RelationshipKey,
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::relationship::shape::relationship as relationship_shape};

impl CatalogStore {
	pub(crate) fn list_relationships(rx: &mut Transaction<'_>) -> Result<Vec<Relationship>> {
		let mut entries = Vec::new();
		{
			let stream = rx.range(RelationshipKey::full_scan(), 1024)?;
			for entry in stream {
				entries.push(entry?);
			}
		}

		let mut result = Vec::with_capacity(entries.len());
		for entry in entries {
			result.push(decode_relationship_row(&entry.row)?);
		}
		Ok(result)
	}
}

pub(crate) fn decode_relationship_row(row: &EncodedRow) -> Result<Relationship> {
	let id = RelationshipId(relationship_shape::SHAPE.get_u64(row, relationship_shape::ID));
	let namespace = NamespaceId(relationship_shape::SHAPE.get_u64(row, relationship_shape::NAMESPACE_ID));
	let name = relationship_shape::SHAPE.get_utf8(row, relationship_shape::NAME).to_string();
	let source_table = TableId(relationship_shape::SHAPE.get_u64(row, relationship_shape::SOURCE_TABLE_ID));
	let source_column = ColumnId(relationship_shape::SHAPE.get_u64(row, relationship_shape::SOURCE_COLUMN_ID));
	let target_table = TableId(relationship_shape::SHAPE.get_u64(row, relationship_shape::TARGET_TABLE_ID));
	let target_column = ColumnId(relationship_shape::SHAPE.get_u64(row, relationship_shape::TARGET_COLUMN_ID));

	let junction_table_raw = relationship_shape::SHAPE.get_u64(row, relationship_shape::JUNCTION_TABLE_ID);
	let junction = if junction_table_raw == 0 {
		None
	} else {
		let source_column =
			ColumnId(relationship_shape::SHAPE.get_u64(row, relationship_shape::JUNCTION_SOURCE_COLUMN_ID));
		let target_column =
			ColumnId(relationship_shape::SHAPE.get_u64(row, relationship_shape::JUNCTION_TARGET_COLUMN_ID));
		Some(RelationshipJunction {
			table: TableId(junction_table_raw),
			source_column,
			target_column,
		})
	};

	let cardinality_code = relationship_shape::SHAPE.get_u8(row, relationship_shape::CARDINALITY);
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
