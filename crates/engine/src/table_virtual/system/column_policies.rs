// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system column policy information
pub struct ColumnPolicies<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> ColumnPolicies<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_column_policies_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for ColumnPolicies<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a, T>,
		_ctx: TableVirtualContext<'a>,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
	) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut policy_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut policy_types = Vec::new();
		let mut policy_values = Vec::new();

		let policies = CatalogStore::list_column_policies_all(txn)?;
		for policy in policies {
			policy_ids.push(policy.id.0);
			column_ids.push(policy.column.0);
			let (ty, val) = policy.policy.to_u8();
			policy_types.push(ty);
			policy_values.push(val);
		}

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ColumnData::uint8(policy_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "column_id".to_string(),
				data: ColumnData::uint8(column_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "type".to_string(),
				data: ColumnData::uint1(policy_types),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "value".to_string(),
				data: ColumnData::uint1(policy_values),
			}),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
