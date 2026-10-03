// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, iter, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, shape::RowShape},
};
use reifydb_core::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change, store::MultiVersionRow},
	key::any::TaggedKey,
};
use reifydb_flow_async::{
	operator::sink::DurableSink,
	timer::{Timer, TimerDue},
	transaction::{ChangeCoordinate, FlowTransaction, substrate::FlowSubstrate},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	accumulator::ChangeAccumulator,
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	single::SingleTransaction,
};
use reifydb_value::{Result, value::datetime::DateTime};

pub struct TestFlowTransaction {
	version: CommitVersion,
	clock: Clock,
	catalog: Catalog,
	substrate: FlowSubstrate,
	pending: Pending,
	accumulator: ChangeAccumulator,
	armed: Vec<TimerDue>,
	change_coordinate: Option<ChangeCoordinate>,
	flow_watermark: Option<DateTime>,
	source_watermarks: HashMap<OperatorId, i64>,
	row_shapes: HashMap<OperatorId, HashMap<EncodedKey, RowShape>>,
}

impl TestFlowTransaction {
	pub fn new(version: CommitVersion, clock: Clock) -> Self {
		let dictionary = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(
			SingleTransaction::testing(),
		)));
		Self {
			version,
			clock,
			catalog: Catalog::testing(),
			substrate: FlowSubstrate::with_dictionary(dictionary, OperatorStore::testing_memory()),
			pending: Pending::new(),
			accumulator: ChangeAccumulator::new(),
			armed: Vec::new(),
			change_coordinate: None,
			flow_watermark: None,
			source_watermarks: HashMap::new(),
			row_shapes: HashMap::new(),
		}
	}

	pub fn set_version(&mut self, version: CommitVersion) {
		self.version = version;
	}
}

impl FlowTransaction for TestFlowTransaction {
	fn version(&self) -> CommitVersion {
		self.version
	}

	fn clock(&self) -> &Clock {
		&self.clock
	}

	fn catalog(&self) -> &Catalog {
		&self.catalog
	}

	fn query(&self) -> MultiReadTransaction {
		panic!("not supported by the test transaction: query")
	}

	fn substrate(&self) -> &FlowSubstrate {
		&self.substrate
	}

	fn pending(&self) -> &Pending {
		&self.pending
	}

	fn pending_mut(&mut self) -> &mut Pending {
		&mut self.pending
	}

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator {
		&mut self.accumulator
	}

	fn armed_mut(&mut self) -> &mut Vec<TimerDue> {
		&mut self.armed
	}

	fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		self.change_coordinate
	}

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		self.change_coordinate = Some(coordinate);
	}

	fn flow_watermark(&self) -> Option<DateTime> {
		self.flow_watermark
	}

	fn set_flow_watermark(&mut self, watermark: DateTime) {
		self.flow_watermark = Some(watermark);
	}

	fn source_watermark_cache(&mut self) -> &mut HashMap<OperatorId, i64> {
		&mut self.source_watermarks
	}

	fn row_shape_cache(&mut self, operator: OperatorId) -> &mut HashMap<EncodedKey, RowShape> {
		self.row_shapes.entry(operator).or_default()
	}

	fn run_durable_sink(&mut self, _sink: &mut dyn DurableSink, _change: Change) -> Result<Change> {
		panic!("not supported by the test transaction: run_durable_sink")
	}

	fn run_durable_sink_timer(&mut self, _sink: &mut dyn DurableSink, _timer: Timer) -> Result<Option<Change>> {
		panic!("not supported by the test transaction: run_durable_sink_timer")
	}

	fn storage_get(&mut self, _key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		Ok(None)
	}

	fn storage_contains(&mut self, _key: &EncodedKey) -> Result<bool> {
		Ok(false)
	}

	fn storage_range(
		&mut self,
		_range: EncodedKeyRange,
		_scope: RangeScope,
		_batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		Box::new(iter::empty())
	}

	fn fetch_state_external(
		&mut self,
		_keys: Vec<EncodedKey>,
		_items: &mut Vec<MultiVersionRow<TaggedKey>>,
	) -> Result<()> {
		Ok(())
	}
}
