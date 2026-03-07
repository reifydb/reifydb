// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{sumtype::SumTypeKind, vtable::VTableDef},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes variant and field information for enum sumtypes
pub struct EnumVariants {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl EnumVariants {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_enum_variants_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for EnumVariants {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let sumtypes: Vec<_> = CatalogStore::list_all_sumtypes(txn)?
			.into_iter()
			.filter(|st| st.kind == SumTypeKind::Enum)
			.collect();

		let mut ids = ColumnData::uint8_with_capacity(0);
		let mut variant_tags = ColumnData::uint1_with_capacity(0);
		let mut variant_names = ColumnData::utf8_with_capacity(0);
		let mut field_counts = ColumnData::uint1_with_capacity(0);
		let mut field_indices = ColumnData::uint1_with_capacity(0);
		let mut field_names = ColumnData::utf8_with_capacity(0);
		let mut field_types = ColumnData::uint1_with_capacity(0);

		for st in &sumtypes {
			for variant in &st.variants {
				if variant.fields.is_empty() {
					ids.push(st.id.0);
					variant_tags.push(variant.tag);
					variant_names.push(variant.name.as_str());
					field_counts.push(0u8);
					field_indices.push(0u8);
					field_names.push("");
					field_types.push(0u8);
				} else {
					for (idx, field) in variant.fields.iter().enumerate() {
						let ffi = field.field_type.to_ffi();
						ids.push(st.id.0);
						variant_tags.push(variant.tag);
						variant_names.push(variant.name.as_str());
						field_counts.push(variant.fields.len() as u8);
						field_indices.push(idx as u8);
						field_names.push(field.name.as_str());
						field_types.push(ffi.base_type);
					}
				}
			}
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("variant_tag"),
				data: variant_tags,
			},
			Column {
				name: Fragment::internal("variant_name"),
				data: variant_names,
			},
			Column {
				name: Fragment::internal("field_count"),
				data: field_counts,
			},
			Column {
				name: Fragment::internal("field_index"),
				data: field_indices,
			},
			Column {
				name: Fragment::internal("field_name"),
				data: field_names,
			},
			Column {
				name: Fragment::internal("field_type"),
				data: field_types,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
