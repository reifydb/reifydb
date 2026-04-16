// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		procedure::{Procedure, RqlTrigger},
		vtable::VTable,
	},
	value::column::{Column, columns::Columns, data::ColumnData},
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

		let mut ids = ColumnData::uint8_with_capacity(procs.len());
		let mut namespace_ids = ColumnData::uint8_with_capacity(procs.len());
		let mut names = ColumnData::utf8_with_capacity(procs.len());
		let mut return_types = ColumnData::utf8_with_capacity(procs.len());
		let mut bodies = ColumnData::utf8_with_capacity(procs.len());
		let mut trigger_kinds = ColumnData::utf8_with_capacity(procs.len());
		let mut event_sumtypes = ColumnData::uint8_with_capacity(procs.len());
		let mut event_indexes = ColumnData::uint2_with_capacity(procs.len());

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
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespace_ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("return_type"),
				data: return_types,
			},
			Column {
				name: Fragment::internal("body"),
				data: bodies,
			},
			Column {
				name: Fragment::internal("trigger_kind"),
				data: trigger_kinds,
			},
			Column {
				name: Fragment::internal("event_variant_sumtype_id"),
				data: event_sumtypes,
			},
			Column {
				name: Fragment::internal("event_variant_index"),
				data: event_indexes,
			},
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
