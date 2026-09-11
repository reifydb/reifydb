// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::{key::encoded::EncodedKeyRange, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
	metrics::collect::MetricsCollector,
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	error::{OperatorError, Result},
	persistent::{Apply, Checkpoint, Enumerate, Fetch, Measure, Page, Persistent, memory::MemoryPersistent},
	types::{Applied, FlushBatch, OperatorBatch, OperatorStateCensus},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
	Land,
	Partial(usize),
	Lost,
	Err(OperatorError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
	Clean,
	Absent,
	Short(usize),
	Err(OperatorError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continue {
	Yes,
	Crash,
}

pub trait PersistentHooks: Send + Sync {
	fn on_get(&self, _operator: OperatorId, _key: &GroupStateKey) -> ReadOutcome {
		ReadOutcome::Clean
	}

	fn on_page(&self, _operator: OperatorId, _len: usize) -> ReadOutcome {
		ReadOutcome::Clean
	}

	fn on_apply(&self, _batch: &FlushBatch) -> ApplyOutcome {
		ApplyOutcome::Land
	}

	fn on_checkpoint(&self, _flow: FlowId) -> ApplyOutcome {
		ApplyOutcome::Land
	}

	fn on_checkpoint_remove(&self, _flow: FlowId) -> ApplyOutcome {
		ApplyOutcome::Land
	}

	fn on_drop(&self, _operator: OperatorId) -> ApplyOutcome {
		ApplyOutcome::Land
	}

	fn during_apply(&self, _batch: &FlushBatch) {}

	fn on_call(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl PersistentHooks for NoFaults {}

struct Inner {
	durable: MemoryPersistent,
	hooks: Arc<dyn PersistentHooks>,
	calls: AtomicU64,
}

#[derive(Clone)]
pub struct TestingPersistent(Arc<Inner>);

impl TestingPersistent {
	pub fn new(hooks: Arc<dyn PersistentHooks>) -> Self {
		Self::over(MemoryPersistent::new(), hooks)
	}

	pub fn over(durable: MemoryPersistent, hooks: Arc<dyn PersistentHooks>) -> Self {
		Self(Arc::new(Inner {
			durable,
			hooks,
			calls: AtomicU64::new(0),
		}))
	}

	pub fn durable(&self) -> &MemoryPersistent {
		&self.0.durable
	}

	pub fn calls(&self) -> u64 {
		self.0.calls.load(Ordering::SeqCst)
	}

	fn call(&self) {
		let call = self.0.calls.fetch_add(1, Ordering::SeqCst) + 1;
		if self.0.hooks.on_call(call) == Continue::Crash {
			panic!("simulated crash at call {call}");
		}
	}
}

fn shorten(mut batch: OperatorBatch, outcome: ReadOutcome) -> Result<OperatorBatch> {
	match outcome {
		ReadOutcome::Clean => Ok(batch),
		ReadOutcome::Absent => Ok(OperatorBatch::empty()),
		ReadOutcome::Short(len) => {
			batch.items.truncate(len);
			batch.has_more = true;
			Ok(batch)
		}
		ReadOutcome::Err(error) => Err(error),
	}
}

impl Persistent for TestingPersistent {
	fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		self.0.durable.metrics_collectors()
	}
}

impl Fetch for TestingPersistent {
	fn get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		self.call();
		match self.0.hooks.on_get(operator, key) {
			ReadOutcome::Clean => self.0.durable.get(operator, key),
			ReadOutcome::Absent | ReadOutcome::Short(_) => Ok(None),
			ReadOutcome::Err(error) => Err(error),
		}
	}

	fn get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, EncodedPodRow>> {
		self.call();
		let mut found = HashMap::new();
		for key in keys {
			if let Some(row) = self.get(operator, key)? {
				found.insert(key.clone(), row);
			}
		}
		Ok(found)
	}

	fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool> {
		Ok(self.get(operator, key)?.is_some())
	}
}

impl Page for TestingPersistent {
	fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch> {
		self.call();
		let page = self.0.durable.range_batch(operator, range, batch, occupied)?;
		let outcome = self.0.hooks.on_page(operator, page.items.len());
		shorten(page, outcome)
	}

	fn last_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch> {
		self.call();
		let page = self.0.durable.last_batch(operator, range, batch, occupied)?;
		let outcome = self.0.hooks.on_page(operator, page.items.len());
		shorten(page, outcome)
	}

	fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch: u64, mask: u64) -> Result<OperatorBatch> {
		self.call();
		let page = self.0.durable.group_page(operator, groups, batch, mask)?;
		let outcome = self.0.hooks.on_page(operator, page.items.len());
		shorten(page, outcome)
	}
}

impl Measure for TestingPersistent {
	fn state_sizes(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, ByteSize>> {
		self.call();
		self.0.durable.state_sizes(operator, keys)
	}

	fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		self.call();
		self.0.durable.bytes(operator)
	}
}

impl Enumerate for TestingPersistent {
	fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		self.call();
		self.0.durable.census()
	}

	fn operators(&self) -> Result<Vec<OperatorId>> {
		self.call();
		self.0.durable.operators()
	}

	fn keyspaces(&self, operator: OperatorId) -> Result<Vec<KeyspaceId>> {
		self.call();
		self.0.durable.keyspaces(operator)
	}
}

impl Checkpoint for TestingPersistent {
	fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		self.call();
		self.0.durable.checkpoint_get(flow)
	}

	fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		self.call();
		match self.0.hooks.on_checkpoint(flow) {
			ApplyOutcome::Land => self.0.durable.checkpoint_set(flow, version),
			ApplyOutcome::Partial(_) | ApplyOutcome::Lost => Ok(()),
			ApplyOutcome::Err(error) => Err(error),
		}
	}

	fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		self.call();
		match self.0.hooks.on_checkpoint_remove(flow) {
			ApplyOutcome::Land => self.0.durable.checkpoint_remove(flow),
			ApplyOutcome::Partial(_) | ApplyOutcome::Lost => Ok(()),
			ApplyOutcome::Err(error) => Err(error),
		}
	}

	fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		self.call();
		self.0.durable.checkpoint_floor()
	}

	fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		self.call();
		self.0.durable.checkpoint_list()
	}
}

impl Apply for TestingPersistent {
	fn apply(&self, batch: &FlushBatch) -> Result<Applied> {
		self.call();
		let applied = match self.0.hooks.on_apply(batch) {
			ApplyOutcome::Land => self.0.durable.apply(batch),
			ApplyOutcome::Partial(len) => {
				let torn = FlushBatch {
					writes: batch.writes.iter().take(len).cloned().collect(),
					checkpoints: Default::default(),
					drops: Vec::new(),
					bytes: batch.bytes,
				};
				self.0.durable.apply(&torn).map(|_| Applied {
					rows: batch.writes.len(),
					bytes: batch.bytes,
				})
			}
			ApplyOutcome::Lost => Ok(Applied {
				rows: batch.writes.len(),
				bytes: batch.bytes,
			}),
			ApplyOutcome::Err(error) => Err(error),
		};
		self.0.hooks.during_apply(batch);
		applied
	}

	fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		self.call();
		match self.0.hooks.on_drop(operator) {
			ApplyOutcome::Land => self.0.durable.drop_operator(operator),
			ApplyOutcome::Partial(_) | ApplyOutcome::Lost => Ok(()),
			ApplyOutcome::Err(error) => Err(error),
		}
	}
}
