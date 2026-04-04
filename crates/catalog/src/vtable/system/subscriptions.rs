// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{subscription::SubscriptionInspectorRef, vtable::VTable},
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes all currently active subscriptions.
///
/// Each row shows the subscription ID and the number of columns in that subscription.
/// This uses dynamic IoC resolution to avoid a hard dependency on the subscription subsystem.
pub struct SystemSubscriptions {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	ioc: IocContainer,
}

impl SystemSubscriptions {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_subscriptions_table().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl BaseVTable for SystemSubscriptions {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let subscriptions = match self.ioc.resolve::<SubscriptionInspectorRef>() {
			Ok(inspector) => {
				let ids = inspector.active_subscriptions();
				ids.into_iter()
					.filter_map(|id| inspector.column_count(&id).map(|count| (id, count)))
					.collect::<Vec<_>>()
			}
			Err(_) => vec![],
		};

		let mut id_col = ColumnData::uint8_with_capacity(subscriptions.len());
		let mut column_count_col = ColumnData::uint8_with_capacity(subscriptions.len());

		for (id, count) in subscriptions {
			id_col.push(id.0);
			column_count_col.push(count as u64);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: id_col,
			},
			Column {
				name: Fragment::internal("column_count"),
				data: column_count_col,
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
