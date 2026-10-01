// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, marker::PhantomData, ops::Index};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{dictionary::Dictionary, flow::OperatorId},
		change::{Change, Diffs},
	},
	key::operator::{
		keyspace::timer::TimerWheelKey,
		state::{GroupId, KeyspaceId, OperatorStateKey},
	},
	operator_with::ApplyWith,
	row::Row,
	state::{timer::TimerKind, typed::SuffixBytes},
};
use reifydb_flow_async::{
	operator::{BoxedHostOperator, apply::ApplyOperator, host::TxnHostContext},
	timer::{
		Timer,
		wheel::{MAX_TIMERS_PER_SCAN, TimerWheel},
	},
	transaction::FlowTransaction,
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::{
	error::Result,
	flow::operator::{MountedOperator, OperatorMetadata, mount::mount, state::decode_payload},
};
use reifydb_testing_chaos::operator::subject::Subject;
use reifydb_value::{
	Result as ValueResult,
	config::ExtensionParams,
	count::Count,
	value::{Value, datetime::DateTime},
};

use crate::{builders::TestChangeBuilder, in_process::transaction::TestFlowTransaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimedGroups {
	pub groups: Count,
	pub keys: Count,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedTimer {
	pub due: DateTime,
	pub kind: TimerKind,
	pub key: Vec<u8>,
}

pub struct InProcessOperatorHarness<C: MountedOperator + OperatorMetadata + 'static> {
	operator: BoxedHostOperator,
	txn: TestFlowTransaction,
	params: HashMap<String, Value>,
	with: ApplyWith,
	operator_id: OperatorId,
	history: Vec<Change>,
	_phantom: PhantomData<C>,
}

impl<C: MountedOperator + OperatorMetadata + 'static> InProcessOperatorHarness<C> {
	pub fn builder() -> InProcessOperatorHarnessBuilder<C> {
		InProcessOperatorHarnessBuilder::new()
	}

	fn mount_operator(
		operator_id: OperatorId,
		params: &HashMap<String, Value>,
		with: &ApplyWith,
	) -> Result<BoxedHostOperator> {
		let logic = C::create(operator_id, &ExtensionParams::new("operator", params.clone()), with)?;
		let mounted = mount(logic, operator_id, <C as OperatorMetadata>::CAPABILITIES);
		Ok(Box::new(ApplyOperator::new(None, operator_id, mounted, with)))
	}

	pub fn apply(&mut self, input: Change) -> Result<Change> {
		let retention = self.operator.retention();
		let mut host = TxnHostContext::with_retention(&mut self.txn, self.operator_id, retention);
		let output = self.operator.apply(&mut host, input)?;
		self.history.push(output.clone());
		Ok(output)
	}

	pub fn fire_timer(&mut self, due: DateTime, kind: TimerKind, key: &[u8]) -> Result<()> {
		self.on_timer(due, kind, key)?;
		Ok(())
	}

	pub fn on_timer(&mut self, at: DateTime, kind: TimerKind, key: &[u8]) -> Result<Option<Change>> {
		let retention = self.operator.retention();
		let mut host = TxnHostContext::with_retention(&mut self.txn, self.operator_id, retention);
		let output = self.operator.on_timer(
			&mut host,
			Timer {
				due: at,
				kind,
				key: EncodedKey::new(key),
			},
		)?;
		if let Some(change) = &output {
			self.history.push(change.clone());
		}
		Ok(output)
	}

	pub fn armed_timers(&self) -> Vec<ArmedTimer> {
		self.txn.pending()
			.iter_sorted()
			.filter_map(|(key, write)| {
				let PendingWrite::Set(bytes) = write else {
					return None;
				};
				let decoded = OperatorStateKey::decode(key)?;
				if decoded.operator != self.operator_id
					|| !decoded.group.is_root()
					|| decoded.keyspace != KeyspaceId::TIMER_WHEEL
				{
					return None;
				}
				let wheel = TimerWheelKey::from_suffix_bytes(&decoded.suffix)?;
				let key = decode_payload::<Vec<u8>>(&EncodedPodRow::from(bytes.clone()))
					.expect("an armed timer row must carry the key it was armed with");
				Some(ArmedTimer {
					due: wheel.due.0,
					kind: wheel.kind.0,
					key,
				})
			})
			.collect()
	}

	pub fn advance_watermark(&mut self, at: DateTime) -> Result<usize> {
		const MAX_ROUNDS: usize = 8192;

		let watermark = self.txn.flow_watermark().map_or(at, |current| current.max(at));
		self.txn.set_flow_watermark(watermark);
		let mut fired = 0usize;
		for _ in 0..MAX_ROUNDS {
			let due = TimerWheel::take_due(self.operator_id, &mut self.txn, at, MAX_TIMERS_PER_SCAN, None)?;
			if due.timers.is_empty() {
				return Ok(fired);
			}
			for timer in due.timers {
				self.fire_timer(timer.due, timer.kind, timer.key.as_slice())?;
				fired += 1;
			}
		}
		panic!("advance_watermark kept finding due timers after {MAX_ROUNDS} rounds; an operator is \
			 re-arming at or below the watermark it was just woken for, which would spin the real \
			 wheel too")
	}

	pub fn reclaim_groups(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |_| true);
		Self::reclaimed(groups, removed)
	}

	pub fn reclaim_group_data(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |keyspace| keyspace.is_data());
		Self::reclaimed(groups, removed)
	}

	pub fn reclaim_group_identity(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |keyspace| keyspace.is_identity());
		Self::reclaimed(groups, removed)
	}

	fn reclaimed(groups: &[GroupId], removed: usize) -> ReclaimedGroups {
		ReclaimedGroups {
			groups: Count::new(groups.len() as u64),
			keys: Count::new(removed as u64),
		}
	}

	fn erase_group_state(&mut self, groups: &[GroupId], erase: impl Fn(KeyspaceId) -> bool) -> usize {
		let erased: Vec<EncodedKey> =
			self.txn.pending()
				.iter_sorted()
				.filter(|(_, write)| matches!(write, PendingWrite::Set(_)))
				.filter_map(|(key, _)| {
					let decoded = OperatorStateKey::decode(key)?;
					let doomed = decoded.operator == self.operator_id
						&& !decoded.group.is_root()
						&& groups.contains(&decoded.group)
						&& erase(decoded.keyspace);
					doomed.then(|| key.clone())
				})
				.collect();
		let pending = self.txn.pending_mut();
		for key in &erased {
			pending.remove_silent(key.clone());
		}
		erased.len()
	}

	pub fn insert(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().insert(row).build();
		self.apply(change).expect("insert failed");
		self
	}

	pub fn update(&mut self, pre: Row, post: Row) -> &mut Self {
		let change = TestChangeBuilder::new().update(pre, post).build();
		self.apply(change).expect("update failed");
		self
	}

	pub fn remove(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().remove(row).build();
		self.apply(change).expect("remove failed");
		self
	}

	pub fn history_len(&self) -> usize {
		self.history.len()
	}

	pub fn last_change(&self) -> Option<&Change> {
		self.history.last()
	}

	pub fn clear_history(&mut self) {
		self.history.clear();
	}

	pub fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	pub fn set_version(&mut self, version: CommitVersion) {
		self.txn.set_version(version);
	}

	pub fn snapshot_state(&self) -> HashMap<EncodedKey, EncodedBytes> {
		self.txn.pending()
			.iter_sorted()
			.filter_map(|(key, write)| match write {
				PendingWrite::Set(bytes) => Some((key.clone(), bytes.clone())),
				PendingWrite::Remove {
					..
				} => None,
			})
			.collect()
	}

	pub fn group_state(&self) -> HashMap<EncodedKey, EncodedBytes> {
		self.snapshot_state()
			.into_iter()
			.filter(|(key, _)| {
				OperatorStateKey::decode(key).is_none_or(|decoded| {
					decoded.keyspace != KeyspaceId::TIMER_WHEEL
						&& decoded.keyspace != KeyspaceId::TIMER_INDEX
				})
			})
			.collect()
	}

	pub fn restore_state(&mut self, snapshot: HashMap<EncodedKey, EncodedBytes>) {
		let pending = self.txn.pending_mut();
		*pending = Pending::new();
		for (key, value) in snapshot {
			pending.insert(key, value);
		}
	}

	pub fn reset(&mut self) -> Result<()> {
		*self.txn.pending_mut() = Pending::new();
		self.txn.take_armed();
		self.txn.accumulator_mut().clear();
		self.txn.source_watermark_cache().clear();
		self.txn.row_shape_cache(self.operator_id).clear();
		self.txn.set_version(CommitVersion(1));
		self.history.clear();

		self.operator = Self::mount_operator(self.operator_id, &self.params, &self.with)?;
		Ok(())
	}

	pub fn operator_id(&self) -> OperatorId {
		self.operator_id
	}
}

impl<C: MountedOperator + OperatorMetadata + 'static> Subject for InProcessOperatorHarness<C> {
	fn apply(&mut self, change: Change) -> ValueResult<Change> {
		InProcessOperatorHarness::apply(self, change).map_err(Into::into)
	}

	fn tick(&mut self, at_ms: u64) -> ValueResult<Option<Change>> {
		let at = DateTime::from_epoch_millis(
			i64::try_from(at_ms).expect("chaos harness tick time fits in i64 milliseconds"),
		)?;
		let fired_from = self.history.len();
		self.advance_watermark(at)?;
		let diffs: Diffs =
			self.history[fired_from..].iter().flat_map(|change| change.diffs.iter().cloned()).collect();
		if diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(Change::from_flow(self.operator_id, ChangeVersion::from(self.version()), diffs, at)))
	}
}

impl<C: MountedOperator + OperatorMetadata + 'static> Index<usize> for InProcessOperatorHarness<C> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

pub struct InProcessOperatorHarnessBuilder<C: MountedOperator + OperatorMetadata + 'static> {
	params: HashMap<String, Value>,
	with: ApplyWith,
	operator_id: OperatorId,
	version: CommitVersion,
	clock: Clock,
	dictionaries: Vec<(Dictionary, Vec<Value>)>,
	_phantom: PhantomData<C>,
}

impl<C: MountedOperator + OperatorMetadata + 'static> Default for InProcessOperatorHarnessBuilder<C> {
	fn default() -> Self {
		Self::new()
	}
}

impl<C: MountedOperator + OperatorMetadata + 'static> InProcessOperatorHarnessBuilder<C> {
	pub fn new() -> Self {
		Self {
			params: HashMap::new(),
			with: ApplyWith::default(),
			operator_id: OperatorId(1),
			version: CommitVersion(1),
			clock: Clock::Mock(MockClock::new(0)),
			dictionaries: Vec::new(),
			_phantom: PhantomData,
		}
	}

	pub fn with_clock(mut self, clock: Clock) -> Self {
		self.clock = clock;
		self
	}

	pub fn with_params<I, K>(mut self, params: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		self.params = params.into_iter().map(|(k, v)| (k.into(), v)).collect();
		self
	}

	pub fn add_param(mut self, key: impl Into<String>, value: Value) -> Self {
		self.params.insert(key.into(), value);
		self
	}

	pub fn with(mut self, with: ApplyWith) -> Self {
		self.with = with;
		self
	}

	pub fn with_node_id(mut self, operator_id: OperatorId) -> Self {
		self.operator_id = operator_id;
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn with_dictionary(mut self, dictionary: Dictionary, values: Vec<Value>) -> Self {
		self.dictionaries.push((dictionary, values));
		self
	}

	pub fn build(self) -> Result<InProcessOperatorHarness<C>> {
		let txn = TestFlowTransaction::new(self.version, self.clock);
		for (dictionary, values) in &self.dictionaries {
			txn.catalog().cache().set_dictionary(dictionary.id, CommitVersion(1), Some(dictionary.clone()));
			txn.dictionary_allocators().intern_batch(dictionary, values)?;
		}

		let operator =
			InProcessOperatorHarness::<C>::mount_operator(self.operator_id, &self.params, &self.with)?;

		Ok(InProcessOperatorHarness {
			operator,
			txn,
			params: self.params,
			with: self.with,
			operator_id: self.operator_id,
			history: Vec::new(),
			_phantom: PhantomData,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{
		key::encoded::IntoEncodedKey,
		row::{operator::state::decode, pod::EncodedPodRow},
	};
	use reifydb_core::{
		common::{WindowRequirements, WindowSizeDomain},
		interface::flow::OperatorCapability,
		key::operator::state::{UnmanagedKey, unmanaged_key},
	};
	use reifydb_sdk::flow::operator::{
		UnmanagedMount, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{ClassState, GuestContext, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	};
	use reifydb_value::value::row_number::RowNumber;

	use super::*;

	struct StatefulTestOperator;

	impl OperatorMetadata for StatefulTestOperator {
		const NAME: &'static str = "stateful_test_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Stateful test operator that stores values";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl UnmanagedOperator for StatefulTestOperator {
		const UNMANAGED_BECAUSE: &'static str = "test operator";
		const WINDOW: WindowRequirements = WindowRequirements {
			takes_window: false,
			kinds: &[],
			domain: WindowSizeDomain::Time,
			needs_pane: false,
			throttles: false,
		};

		fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> Result<()> {
			for index in 0..change.diff_count() {
				let Some(diff) = change.diff(index) else {
					continue;
				};
				let Some(post) = diff.post() else {
					continue;
				};
				for position in 0..post.row_count() {
					let row = post.row(position).expect("every counted row resolves");
					let (Some(row_number), Some(value)) = (row.row_number(), row.i64("field0")?)
					else {
						continue;
					};
					ctx.state().set(&probe_row_key(row_number), &value)?;
				}
			}
			Ok(())
		}
	}

	fn probe_row_key(row_number: RowNumber) -> UnmanagedKey {
		unmanaged_key(format!("row_{}", row_number.0).into_encoded_key().as_ref())
			.expect("a probe row key must fit the keyspace's id width")
	}

	fn stored_value(harness: &InProcessOperatorHarness<UnmanagedMount<StatefulTestOperator>>, row: u64) -> i64 {
		let inner = probe_row_key(RowNumber(row));
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(inner.as_ref().as_slice()).expect("a probe key decodes");
		let key = OperatorStateKey::encoded(harness.operator_id(), group, keyspace, suffix);
		let bytes =
			harness.snapshot_state().remove(&key).unwrap_or_else(|| panic!("row {row} not found in state"));
		decode(&EncodedPodRow::from(bytes)).expect("a stored probe value decodes")
	}

	#[test]
	fn test_harness_multiple_operations() {
		// Both inserts fold into one diff, so a fixture reading only its first row never stores row 2.
		let mut harness = InProcessOperatorHarnessBuilder::<UnmanagedMount<StatefulTestOperator>>::new()
			.build()
			.expect("Failed to build harness");

		let input1 = TestChangeBuilder::new()
			.insert_row(1, vec![Value::Int8(10i64)])
			.insert_row(2, vec![Value::Int8(20i64)])
			.build();

		harness.apply(input1).expect("First apply failed");

		assert_eq!(harness.snapshot_state().len(), 2);

		let input2 = TestChangeBuilder::new().insert_row(RowNumber(3), vec![Value::Int8(30i64)]).build();

		harness.apply(input2).expect("Second apply failed");

		assert_eq!(stored_value(&harness, 1), 10i64);
		assert_eq!(stored_value(&harness, 2), 20i64);
		assert_eq!(stored_value(&harness, 3), 30i64);

		assert_eq!(harness.snapshot_state().len(), 3);
	}
}
