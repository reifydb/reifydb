// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::VecDeque;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
#[cfg(not(target_arch = "wasm32"))]
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

#[allow(dead_code)]
pub(crate) struct RemoteFetchNode {
	address: String,
	remote_rql: String,
	batches: VecDeque<Columns>,
	headers: Option<ColumnHeaders>,
}

impl RemoteFetchNode {
	pub fn new(address: String, remote_rql: String) -> Self {
		Self {
			address,
			remote_rql,
			batches: VecDeque::new(),
			headers: None,
		}
	}
}

impl QueryNode for RemoteFetchNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		#[cfg(not(target_arch = "wasm32"))]
		{
			if let Some(ref registry) = _ctx.services.remote_registry {
				let frames =
					registry.forward_query(&self.address, &self.remote_rql, _ctx.params.clone())?;

				for frame in frames {
					let cols: Columns = frame.into();
					if self.headers.is_none() {
						self.headers = Some(ColumnHeaders {
							columns: cols
								.columns
								.iter()
								.map(|c| Fragment::internal(c.name.text()))
								.collect(),
						});
					}
					self.batches.push_back(cols);
				}
			}
		}
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		Ok(self.batches.pop_front())
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
