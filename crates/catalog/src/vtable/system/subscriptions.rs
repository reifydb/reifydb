// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{subscription::SubscriptionInspectorRef, vtable::VTable},
	util::ioc::IocContainer,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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

		let subscriptions = match self.ioc.try_resolve::<SubscriptionInspectorRef>() {
			Some(inspector) => inspector.active_subscriptions(),
			None => vec![],
		};

		let mut id_col = ColumnBuilder::with_capacity(ValueType::Uint8, subscriptions.len());

		for id in subscriptions {
			id_col.push(id.0);
		}

		let columns = vec![id_col.finish("id")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
