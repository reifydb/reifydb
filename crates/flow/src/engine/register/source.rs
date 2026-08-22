// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	flow::OperatorId,
	id::{RingBufferId, SeriesId, TableId, ViewId},
	object::ObjectId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	engine::FlowEngineInner,
	operator::scan::{
		ringbuffer::SourceRingBufferOperator, series::SourceSeriesOperator, table::SourceTableOperator,
		view::SourceViewOperator,
	},
};

impl FlowEngineInner {
	#[inline]
	pub(super) fn add_source_table(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		table: TableId,
	) -> Result<()> {
		let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

		self.add_source(flow.id, operator_id, ObjectId::table(table.id));
		self.operators.insert(operator_id, Box::new(SourceTableOperator::new(operator_id, table)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_ringbuffer(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		ringbuffer: RingBufferId,
	) -> Result<()> {
		let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
		self.add_source(flow.id, operator_id, ObjectId::ringbuffer(rb.id));
		self.operators.insert(operator_id, Box::new(SourceRingBufferOperator::new(operator_id, rb)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_series(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		series: SeriesId,
	) -> Result<()> {
		let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
		self.add_source(flow.id, operator_id, ObjectId::series(s.id));
		self.operators.insert(operator_id, Box::new(SourceSeriesOperator::new(operator_id)));
		Ok(())
	}

	#[inline]
	pub(super) fn add_source_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, operator_id, ObjectId::view(view.id()));
		self.operators.insert(operator_id, Box::new(SourceViewOperator::new(operator_id, view)));
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::interface::catalog::flow::FlowId;
	use reifydb_rql::flow::operator::{FlowNode, OperatorDef};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::{
		operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
		transaction::substrate::FlowSubstrate,
	};

	#[test]
	fn a_view_source_registers_only_under_its_own_view_object_id() {
		// A second key never fires, and a repeat of the same key dispatches the change to the operator twice.
		let engine = TestEngine::new();
		engine.admin("CREATE NAMESPACE app");
		engine.admin("CREATE TABLE app::t { id: int4 }");
		engine.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(
				engine.inner().dictionary_allocators(),
				engine.inner().operator_state(),
			),
			OperatorSampleRegistry::new(),
		);

		let mut admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = Transaction::Admin(&mut admin);
		let namespace = engine.catalog().find_namespace_by_name(&mut txn, "app").unwrap().unwrap();
		let view = engine.catalog().find_view_by_name(&mut txn, namespace.id(), "v").unwrap().unwrap();

		let operator = OperatorId(7);
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			operator,
			OperatorDef::SourceView {
				view: view.id(),
			},
		));
		let flow = builder.build();

		inner.add_source_view(&mut txn, &flow, operator, view.id()).unwrap();

		assert_eq!(
			inner.sources.keys().copied().collect::<Vec<_>>(),
			vec![ObjectId::view(view.id())],
			"a view source must occupy exactly one routing key"
		);
		assert_eq!(
			inner.sources[&ObjectId::view(view.id())],
			vec![(FlowId(1), operator)],
			"and that key must carry exactly one registration for the operator"
		);
	}
}
