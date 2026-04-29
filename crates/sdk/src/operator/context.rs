// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_abi::context::context::ContextFFI;
use reifydb_core::{encoded::key::EncodedKey, interface::catalog::flow::FlowNodeId};
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, row_number::RowNumber},
};

use crate::{
	catalog::Catalog,
	error::Result,
	operator::{builder::ColumnsBuilder, diff::DiffStart},
	rql::raw_rql,
	state::{State, row::RowNumberProvider},
	store::Store,
};

pub struct OperatorContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl OperatorContext {
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn operator_id(&self) -> FlowNodeId {
		unsafe { FlowNodeId((*self.ctx).operator_id) }
	}

	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}

	pub fn store(&mut self) -> Store<'_> {
		Store::new(self)
	}

	pub fn catalog(&mut self) -> Catalog<'_> {
		Catalog::new(self)
	}

	pub fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		let provider = RowNumberProvider::new(self.operator_id());
		provider.get_or_create_row_number(self, key)
	}

	pub fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<RowNumber>> {
		let provider = RowNumberProvider::new(self.operator_id());
		Ok(provider.get_or_create_row_numbers_batch(self, keys.iter())?.into_iter().map(|(rn, _)| rn).collect())
	}

	pub fn rql(&self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		raw_rql(self, rql, params)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::new(self)
	}

	pub fn diff(&mut self) -> DiffStart<'_> {
		DiffStart::new(self)
	}
}
