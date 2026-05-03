// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use std::collections::HashMap;
use std::collections::VecDeque;
#[cfg(not(reifydb_single_threaded))]
use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::fragment::Fragment;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::{params::Params, value::Value};

#[cfg(not(reifydb_single_threaded))]
use crate::vm::stack::Variable;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

#[allow(dead_code)]
pub(crate) struct RemoteFetchNode {
	address: String,
	token: Option<String>,
	remote_rql: String,
	variable_names: Vec<String>,
	batches: VecDeque<Columns>,
	headers: Option<ColumnHeaders>,
}

impl RemoteFetchNode {
	pub fn new(address: String, token: Option<String>, remote_rql: String, variable_names: Vec<String>) -> Self {
		Self {
			address,
			token,
			remote_rql,
			variable_names,
			batches: VecDeque::new(),
			headers: None,
		}
	}
}

impl QueryNode for RemoteFetchNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		#[cfg(not(reifydb_single_threaded))]
		{
			if let Some(ref registry) = _ctx.services.remote_registry {
				let mut named_params: HashMap<String, Value> = HashMap::new();
				for var_name in &self.variable_names {
					if let Some(Variable::Columns {
						columns,
					}) = _ctx.symbols.get(var_name) && columns.is_scalar()
					{
						named_params.insert(var_name.clone(), columns.scalar_value().clone());
					}
				}

				let params = if named_params.is_empty() {
					_ctx.params.clone()
				} else {
					if let Params::Named(ref existing) = _ctx.params {
						let mut merged = named_params;
						for (k, v) in existing.iter() {
							merged.insert(k.clone(), v.clone());
						}
						Params::Named(Arc::new(merged))
					} else {
						Params::Named(Arc::new(named_params))
					}
				};

				let frames = registry.forward_query(
					&self.address,
					&self.remote_rql,
					params,
					self.token.as_deref(),
				)?;

				for frame in frames {
					let cols: Columns = frame.into();
					if self.headers.is_none() {
						self.headers = Some(ColumnHeaders {
							columns: cols
								.names
								.iter()
								.map(|n| Fragment::internal(n.text()))
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
