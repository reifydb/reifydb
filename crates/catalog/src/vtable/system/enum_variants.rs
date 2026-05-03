// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{sumtype::SumTypeKind, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemEnumVariants {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemEnumVariants {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemEnumVariants {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_enum_variants_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemEnumVariants {
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

		let mut ids = ColumnBuffer::uint8_with_capacity(0);
		let mut variant_tags = ColumnBuffer::uint1_with_capacity(0);
		let mut variant_names = ColumnBuffer::utf8_with_capacity(0);
		let mut field_counts = ColumnBuffer::uint1_with_capacity(0);
		let mut field_indices = ColumnBuffer::uint1_with_capacity(0);
		let mut field_names = ColumnBuffer::utf8_with_capacity(0);
		let mut field_types = ColumnBuffer::uint1_with_capacity(0);

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
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("variant_tag"), variant_tags),
			ColumnWithName::new(Fragment::internal("variant_name"), variant_names),
			ColumnWithName::new(Fragment::internal("field_count"), field_counts),
			ColumnWithName::new(Fragment::internal("field_index"), field_indices),
			ColumnWithName::new(Fragment::internal("field_name"), field_names),
			ColumnWithName::new(Fragment::internal("field_type"), field_types),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
