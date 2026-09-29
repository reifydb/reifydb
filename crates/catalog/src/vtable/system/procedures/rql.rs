// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		procedure::{Procedure, RqlTrigger},
		vtable::VTable,
	},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, value_type::ValueType};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut namespace_ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut return_types = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut bodies = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut trigger_kinds = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut event_sumtypes = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut event_indexes = ColumnBuilder::with_capacity(ValueType::Uint2, procs.len());

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
				None => Value::none_of(ValueType::Utf8),
			});
			bodies.push(body.as_str());
			match trigger {
				RqlTrigger::Call => {
					trigger_kinds.push("call");
					event_sumtypes.push_value(Value::none_of(ValueType::Uint8));
					event_indexes.push_value(Value::none_of(ValueType::Uint2));
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
			ids.finish("id"),
			namespace_ids.finish("namespace_id"),
			names.finish("name"),
			return_types.finish("return_type"),
			bodies.finish("body"),
			trigger_kinds.finish("trigger_kind"),
			event_sumtypes.finish("event_variant_sumtype_id"),
			event_indexes.finish("event_variant_index"),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
