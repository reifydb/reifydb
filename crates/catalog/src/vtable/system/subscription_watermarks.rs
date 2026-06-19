// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, subscription::SubscriptionWatermarkSampler},
	util::ioc::IocContainer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemSubscriptionWatermarks {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	ioc: IocContainer,
}

impl SystemSubscriptionWatermarks {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_subscription_watermarks_table().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl BaseVTable for SystemSubscriptionWatermarks {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rows = match self.ioc.resolve::<SubscriptionWatermarkSampler>() {
			Ok(source) => source.all(),
			Err(_) => vec![],
		};

		let mut subscription_ids = ColumnBuffer::uint8_with_capacity(rows.len());
		let mut shape_ids = ColumnBuffer::uint8_with_capacity(rows.len());
		let mut lags = ColumnBuffer::uint8_with_capacity(rows.len());

		for row in rows {
			subscription_ids.push(row.subscription_id.0);
			shape_ids.push(row.shape_id.as_u64());
			lags.push(row.lag);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("subscription_id"), subscription_ids),
			ColumnWithName::new(Fragment::internal("shape_id"), shape_ids),
			ColumnWithName::new(Fragment::internal("lag"), lags),
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
