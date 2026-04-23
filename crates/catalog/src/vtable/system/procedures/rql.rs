// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		procedure::{Procedure, RqlTrigger},
		vtable::VTable,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes RQL (user-defined) procedures.
pub struct SystemProceduresRql {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemProceduresRql {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemProceduresRql {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_rql_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProceduresRql {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let procs: Vec<_> = CatalogStore::list_procedures_all(txn)?
			.into_iter()
			.filter(|p| matches!(p, Procedure::Rql { .. }))
			.collect();

		let mut ids = ColumnBuffer::uint8_with_capacity(procs.len());
		let mut namespace_ids = ColumnBuffer::uint8_with_capacity(procs.len());
		let mut names = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut return_types = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut bodies = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut trigger_kinds = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut event_sumtypes = ColumnBuffer::uint8_with_capacity(procs.len());
		let mut event_indexes = ColumnBuffer::uint2_with_capacity(procs.len());

		for p in procs {
			let Procedure::Rql {
				id,
				namespace,
				name,
				return_type,
				body,
				trigger,
				..
			} = p
			else {
				continue;
			};
			ids.push(*id);
			namespace_ids.push(namespace.0);
			names.push(name.as_str());
			return_types.push_value(match return_type {
				Some(rt) => Value::Utf8(to_string(&rt).expect("TypeConstraint serializes")),
				None => Value::none_of(Type::Utf8),
			});
			bodies.push(body.as_str());
			match trigger {
				RqlTrigger::Call => {
					trigger_kinds.push("call");
					event_sumtypes.push_value(Value::none_of(Type::Uint8));
					event_indexes.push_value(Value::none_of(Type::Uint2));
				}
				RqlTrigger::Event {
					variant,
				} => {
					trigger_kinds.push("event");
					event_sumtypes.push_value(Value::Uint8(variant.sumtype_id.0));
					event_indexes.push_value(Value::Uint2(variant.variant_tag as u16));
				}
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespace_ids),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("return_type"), return_types),
			ColumnWithName::new(Fragment::internal("body"), bodies),
			ColumnWithName::new(Fragment::internal("trigger_kind"), trigger_kinds),
			ColumnWithName::new(Fragment::internal("event_variant_sumtype_id"), event_sumtypes),
			ColumnWithName::new(Fragment::internal("event_variant_index"), event_indexes),
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
