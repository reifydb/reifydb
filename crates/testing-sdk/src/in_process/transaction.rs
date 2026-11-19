// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, iter, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, shape::RowShape},
};
use reifydb_core::{
	actors::pending::Pending,
	commit::tracker::WriteTracker,
	common::CommitVersion,
	interface::{
		catalog::{dictionary::Dictionary, flow::OperatorId},
		change::Change,
		store::MultiVersionRow,
	},
	key::{any::TaggedKey, bound::TaggedKeyBoundRange, operator::state::GroupStateKey},
};
use reifydb_flow::txn::{Find, FlowBatch, FlowGet, Intern, Resolve};
use reifydb_flow_async::{
	operator::{TimedOutput, sink::DurableSink},
	timer::{Timer, TimerDue},
	transaction::{FlowTransaction, dictionary::DictionaryExtension, pending_get, substrate::FlowSubstrate},
};
use reifydb_runtime::context::{RuntimeContext, clock::Clock, spawner::Spawner, transaction::TransactionContext};
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	single::SingleTransaction,
};
use reifydb_value::{
	error::ValueError,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub struct TestFlowTransaction {
	ctx: Arc<TransactionContext>,
	version: CommitVersion,
	clock: Clock,
	catalog: Catalog,
	substrate: FlowSubstrate,
	pending: Pending,
	armed: Vec<TimerDue>,
	batch_time: Option<DateTime>,
	flow_watermark: Option<DateTime>,
	source_watermarks: HashMap<OperatorId, i64>,
	row_shapes: HashMap<OperatorId, HashMap<EncodedKey, RowShape>>,
}

impl TestFlowTransaction {
	pub fn new(version: CommitVersion, clock: Clock) -> Self {
		let ctx = Arc::new(TransactionContext::system(
			RuntimeContext::builder(Spawner::inline()).clock(clock.clone()).build(),
		));
		let dictionary = DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(
			ctx.clone(),
			SingleTransaction::testing(),
		)));
		Self {
			ctx,
			version,
			clock,
			catalog: Catalog::testing(),
			substrate: FlowSubstrate::with_dictionary(
				dictionary,
				OperatorStore::testing_memory(),
				WriteTracker::detached(),
			),
			pending: Pending::new(),
			armed: Vec::new(),
			batch_time: None,
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

	fn context(&self) -> &Arc<TransactionContext> {
		&self.ctx
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

	fn armed_mut(&mut self) -> &mut Vec<TimerDue> {
		&mut self.armed
	}

	fn batch_time_if_set(&self) -> Option<DateTime> {
		self.batch_time
	}

	fn set_batch_time(&mut self, at: Option<DateTime>) {
		self.batch_time = at;
	}

	fn flow_watermark(&self) -> Option<DateTime> {
		self.flow_watermark
	}

	fn set_flow_watermark(&mut self, flow_watermark: DateTime) {
		self.flow_watermark = Some(flow_watermark);
	}

	fn source_watermark_cache(&mut self) -> &mut HashMap<OperatorId, i64> {
		&mut self.source_watermarks
	}

	fn row_shape_cache(&mut self, operator: OperatorId) -> &mut HashMap<EncodedKey, RowShape> {
		self.row_shapes.entry(operator).or_default()
	}

	fn run_durable_sink(&mut self, _sink: &mut dyn DurableSink, _change: Change) -> Result<Change, ValueError> {
		panic!("not supported by the test transaction: run_durable_sink")
	}

	fn run_durable_sink_timer(
		&mut self,
		_sink: &mut dyn DurableSink,
		_timer: Timer,
	) -> Result<Option<TimedOutput>, ValueError> {
		panic!("not supported by the test transaction: run_durable_sink_timer")
	}

	fn storage_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>, ValueError> {
		Ok(vec![None; keys.len()])
	}

	fn storage_range(
		&mut self,
		_range: TaggedKeyBoundRange,
		_scope: RangeScope,
		_batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>, ValueError>> + Send + '_> {
		Box::new(iter::empty())
	}

	fn fetch_state(
		&mut self,
		_id: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<Vec<Option<EncodedBytes>>, ValueError> {
		Ok(vec![None; keys.len()])
	}
}

impl FlowGet for TestFlowTransaction {
	fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>, ValueError> {
		pending_get(self, keys)
	}
}

impl FlowBatch for TestFlowTransaction {
	fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<(), ValueError> {
		self.pending_mut().insert(keys, rows);
		Ok(())
	}

	fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<(), ValueError> {
		self.pending_mut().remove(keys);
		Ok(())
	}
}

impl Resolve for TestFlowTransaction {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>, ValueError> {
		Ok(self.find_dictionary(id))
	}

	fn resolve(
		&mut self,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>, ValueError> {
		self.get_from_dictionary(dictionary, ids)
	}
}

impl Find for TestFlowTransaction {
	fn find(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>, ValueError> {
		self.find_in_dictionary(dictionary, values)
	}
}

impl Intern for TestFlowTransaction {
	fn intern(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<DictionaryEntryId>, ValueError> {
		Ok(self.dictionary_allocators()
			.intern_batch(dictionary, values)?
			.into_iter()
			.map(|outcome| outcome.id)
			.collect())
	}
}
