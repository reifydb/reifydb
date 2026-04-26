// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct DeltaMergeNode {
	inputs: Vec<Box<dyn QueryNode>>,
	cursor: usize,
}

impl DeltaMergeNode {
	pub fn new(inputs: Vec<Box<dyn QueryNode>>) -> Self {
		Self {
			inputs,
			cursor: 0,
		}
	}
}

impl QueryNode for DeltaMergeNode {
	#[instrument(name = "volcano::merge::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		for input in &mut self.inputs {
			input.initialize(rx, ctx)?;
		}
		Ok(())
	}

	#[instrument(name = "volcano::merge::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		while self.cursor < self.inputs.len() {
			match self.inputs[self.cursor].next(rx, ctx)? {
				Some(columns) => return Ok(Some(columns)),
				None => self.cursor += 1,
			}
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.inputs.first().and_then(|input| input.headers())
	}

	fn set_scan_limit(&mut self, limit: usize) {
		if let Some(input) = self.inputs.get_mut(self.cursor) {
			input.set_scan_limit(limit);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::VecDeque;

	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, headers::ColumnHeaders};
	use reifydb_type::{fragment::Fragment, params::Params, value::identity::IdentityId};

	use super::*;
	use crate::{
		test_harness::create_test_admin_transaction,
		vm::{services::Services, stack::SymbolTable},
	};

	struct StubNode {
		batches: VecDeque<Columns>,
		headers: Option<ColumnHeaders>,
		init_count: usize,
		scan_limit: Option<usize>,
	}

	impl StubNode {
		fn new(batches: Vec<Columns>, headers: Option<ColumnHeaders>) -> Self {
			Self {
				batches: batches.into(),
				headers,
				init_count: 0,
				scan_limit: None,
			}
		}
	}

	impl QueryNode for StubNode {
		fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
			self.init_count += 1;
			Ok(())
		}

		fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
			Ok(self.batches.pop_front())
		}

		fn headers(&self) -> Option<ColumnHeaders> {
			self.headers.clone()
		}

		fn set_scan_limit(&mut self, limit: usize) {
			self.scan_limit = Some(limit);
		}
	}

	fn batch(name: &str, vals: Vec<i32>) -> Columns {
		Columns::new(vec![ColumnWithName {
			name: Fragment::internal(name),
			data: ColumnBuffer::int4(vals),
		}])
	}

	fn first_int4(columns: &Columns) -> Vec<i32> {
		let buf = &columns.columns[0];
		(0..buf.len())
			.map(|i| match buf.get_value(i) {
				reifydb_type::value::Value::Int4(v) => v,
				other => panic!("expected Int4, got {other:?}"),
			})
			.collect()
	}

	fn make_ctx() -> QueryContext {
		QueryContext {
			services: Services::testing(),
			source: None,
			batch_size: 1024,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
		}
	}

	fn header(names: &[&str]) -> Option<ColumnHeaders> {
		Some(ColumnHeaders {
			columns: names.iter().map(|n| Fragment::internal(*n)).collect(),
		})
	}

	#[test]
	fn concatenates_two_inputs_in_order() {
		let mut admin = create_test_admin_transaction();
		let mut tx: Transaction<'_> = (&mut admin).into();
		let mut ctx = make_ctx();

		let h = header(&["v"]);
		let a = StubNode::new(vec![batch("v", vec![1, 2]), batch("v", vec![3, 4])], h.clone());
		let b = StubNode::new(vec![batch("v", vec![5, 6]), batch("v", vec![7, 8])], h);
		let mut node = DeltaMergeNode::new(vec![Box::new(a), Box::new(b)]);
		node.initialize(&mut tx, &ctx).unwrap();

		let mut values = Vec::new();
		while let Some(b) = node.next(&mut tx, &mut ctx).unwrap() {
			values.extend(first_int4(&b));
		}
		assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8]);
	}

	#[test]
	fn empty_input_followed_by_nonempty_yields_nonempty() {
		let mut admin = create_test_admin_transaction();
		let mut tx: Transaction<'_> = (&mut admin).into();
		let mut ctx = make_ctx();

		let h = header(&["v"]);
		let a = StubNode::new(vec![], h.clone());
		let b = StubNode::new(vec![batch("v", vec![10, 20])], h);
		let mut node = DeltaMergeNode::new(vec![Box::new(a), Box::new(b)]);
		node.initialize(&mut tx, &ctx).unwrap();

		let mut values = Vec::new();
		while let Some(b) = node.next(&mut tx, &mut ctx).unwrap() {
			values.extend(first_int4(&b));
		}
		assert_eq!(values, vec![10, 20]);
	}

	#[test]
	fn all_empty_inputs_return_none() {
		let mut admin = create_test_admin_transaction();
		let mut tx: Transaction<'_> = (&mut admin).into();
		let mut ctx = make_ctx();

		let h = header(&["v"]);
		let a = StubNode::new(vec![], h.clone());
		let b = StubNode::new(vec![], h);
		let mut node = DeltaMergeNode::new(vec![Box::new(a), Box::new(b)]);
		node.initialize(&mut tx, &ctx).unwrap();
		assert!(node.next(&mut tx, &mut ctx).unwrap().is_none());
	}

	#[test]
	fn no_inputs_returns_none() {
		let mut admin = create_test_admin_transaction();
		let mut tx: Transaction<'_> = (&mut admin).into();
		let mut ctx = make_ctx();

		let mut node = DeltaMergeNode::new(vec![]);
		node.initialize(&mut tx, &ctx).unwrap();
		assert!(node.next(&mut tx, &mut ctx).unwrap().is_none());
		assert!(node.headers().is_none());
	}

	#[test]
	fn headers_match_first_input() {
		let h0 = header(&["a", "b"]);
		let h1 = header(&["x"]);
		let a = StubNode::new(vec![], h0.clone());
		let b = StubNode::new(vec![], h1);
		let node = DeltaMergeNode::new(vec![Box::new(a), Box::new(b)]);
		assert_eq!(node.headers().map(|h| h.columns), h0.map(|h| h.columns));
	}
}
