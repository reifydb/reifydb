// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use postcard::to_extend;
use reifydb_codec::{
	key::{
		decode_u64, decode_u64_asc, encode_u64, encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
	row::operator::{OperatorState, decode},
};
use reifydb_core::{
	common::JoinType,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	metrics::heap::OperatorSample,
	state::store::TimerKind,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_macro::operator_state;
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::{Hash128, xxh3_128},
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::{
	column::JoinedColumnsBuilder,
	snapshot::{PublishedRight, SnapshotLedger},
	state::{JoinSide, JoinState},
	store::group_bytes,
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::{
	context::FlowContext,
	error::{FlowGraphError, FlowStateError},
	operator::{
		HostOperator,
		host::HostContext,
		join::{Emitted, Identity},
	},
	state::{
		reaper::{StoreReaper, drain, enqueue, queue_key, queued},
		seal::{ledger::FiredAt, policy::SealPolicy},
	},
	timer::Timer,
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

const SEAL_BATCH: usize = 256;

const ANCHOR_SUFFIX_LEN: usize = 9;

#[operator_state]
#[derive(Clone)]
pub struct SealAnchor {
	expiry: DateTime,
}

struct GroupAnchors {
	before: Option<DateTime>,
	live: HashMap<(u8, u64), DateTime>,
}

fn group_by_key(keys: &[Option<Hash128>]) -> (Vec<Hash128>, HashMap<Hash128, Vec<usize>>, Vec<usize>) {
	let mut order: Vec<Hash128> = Vec::new();
	let mut groups: HashMap<Hash128, Vec<usize>> = HashMap::new();
	let mut undefined: Vec<usize> = Vec::new();
	for (row_idx, key) in keys.iter().enumerate() {
		match key {
			Some(key_hash) => {
				groups.entry(*key_hash)
					.or_insert_with(|| {
						order.push(*key_hash);
						Vec::new()
					})
					.push(row_idx);
			}
			None => undefined.push(row_idx),
		}
	}
	(order, groups, undefined)
}

#[cfg(test)]
mod group_by_key_tests {
	use super::*;

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	#[test]
	fn groups_duplicate_keys_and_preserves_first_occurrence_order() {
		// Latest reuses the left row's number, so a first-occurrence order is what keeps output
		// identity stable; every input index must land in exactly one group.
		let keys = vec![Some(h(0xA)), Some(h(0xB)), Some(h(0xA)), Some(h(0xC)), Some(h(0xB))];
		let (order, groups, undefined) = group_by_key(&keys);

		assert_eq!(order, vec![h(0xA), h(0xB), h(0xC)], "keys must appear in first-occurrence order");
		assert_eq!(groups[&h(0xA)], vec![0, 2], "indices within a group keep input order");
		assert_eq!(groups[&h(0xB)], vec![1, 4]);
		assert_eq!(groups[&h(0xC)], vec![3]);
		assert!(undefined.is_empty());

		let regrouped: usize = order.iter().map(|k| groups[k].len()).sum();
		assert_eq!(regrouped, 5, "every defined row is grouped exactly once");
	}

	#[test]
	fn routes_none_keys_to_undefined_without_grouping_them() {
		// None-key rows take the per-row undefined path (they never probe); they must not create a
		// group keyed by some sentinel hash, or an undefined row would be mis-joined.
		let keys = vec![None, Some(h(0xA)), None, Some(h(0xA))];
		let (order, groups, undefined) = group_by_key(&keys);

		assert_eq!(order, vec![h(0xA)]);
		assert_eq!(groups[&h(0xA)], vec![1, 3]);
		assert_eq!(undefined, vec![0, 2], "none-key rows are collected in input order, separately");
	}

	#[test]
	fn empty_input_yields_empty_partitions() {
		let (order, groups, undefined) = group_by_key(&[]);
		assert!(order.is_empty());
		assert!(groups.is_empty());
		assert!(undefined.is_empty());
	}
}

pub struct JoinSideConfig {
	pub operator: OperatorId,
	pub exprs: Vec<Expression>,
	pub schema: Columns,
}

pub struct JoinOperator {
	operator: OperatorId,
	strategy: JoinStrategy,
	left_node: OperatorId,
	right_node: OperatorId,
	compiled_left_exprs: Vec<CompiledExpr>,
	compiled_right_exprs: Vec<CompiledExpr>,
	alias: Option<String>,
	right_schema: Columns,
	routines: Routines,
	runtime_context: RuntimeContext,
	pub(crate) snapshot: bool,
	natural: bool,
	pub(crate) latest: bool,
	left_seal: Option<Duration>,
	right_seal: Option<Duration>,
	ctx: Arc<FlowContext>,
}

impl JoinOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		left: JoinSideConfig,
		right: JoinSideConfig,
		operator: OperatorId,
		join_type: JoinType,
		alias: Option<String>,
		routines: Routines,
		runtime_context: RuntimeContext,
		snapshot: bool,
		natural: bool,
		latest: bool,
		left_seal: Option<Duration>,
		right_seal: Option<Duration>,
		ctx: Arc<FlowContext>,
	) -> Self {
		let left_node = left.operator;
		let right_node = right.operator;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type, latest);

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};

		let compiled_left_exprs: Vec<CompiledExpr> = left_exprs
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile left expressions");

		let compiled_right_exprs: Vec<CompiledExpr> = right_exprs
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile right expressions");

		Self {
			operator,
			strategy,
			left_node,
			right_node,
			compiled_left_exprs,
			compiled_right_exprs,
			alias,
			right_schema,
			routines,
			runtime_context,
			snapshot,
			natural,
			latest,
			left_seal: left_seal.filter(|span| !span.is_zero()),
			right_seal: right_seal.filter(|span| !span.is_zero()),
			ctx,
		}
	}

	pub(crate) fn seal_of(&self, side: JoinSide) -> Option<Duration> {
		match side {
			JoinSide::Left => self.left_seal,
			JoinSide::Right => match self.snapshot || self.latest {
				true => None,
				false => self.right_seal,
			},
		}
	}

	fn widest_seal(&self) -> Option<Duration> {
		match (self.seal_of(JoinSide::Left), self.seal_of(JoinSide::Right)) {
			(Some(left), Some(right)) => Some(left.max(right)),
			(left, right) => left.or(right),
		}
	}

	fn anchor_key(group: GroupId, side: JoinSide, row_number: RowNumber) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(ANCHOR_SUFFIX_LEN);
		suffix.push(side.tag());
		suffix.extend_from_slice(&encode_u64_asc(row_number.0));
		OperatorStateKey::inner_encoded(group, Keyspace::CUSTOM, suffix)
	}

	fn anchor_range(group: GroupId) -> EncodedKeyRange {
		keyspace_inner_range(group, Keyspace::CUSTOM)
	}

	fn timer_key(group: GroupId) -> EncodedKey {
		EncodedKey::new(encode_u64(group.0))
	}

	fn timer_group(key: &EncodedKey) -> Result<GroupId> {
		let bytes = <[u8; 8]>::try_from(key.as_slice()).map_err(|_| {
			Error::from(FlowStateError::Decode {
				state: "join seal timer key",
				cause: format!("expected eight group bytes, found {}", key.as_slice().len()),
			})
		})?;
		Ok(GroupId(decode_u64(bytes)))
	}

	fn anchors_in(host: &mut dyn HostContext, group: GroupId) -> Result<Vec<(JoinSide, RowNumber, DateTime)>> {
		let mut out = Vec::new();
		for (key, row) in host.state_range(Self::anchor_range(group))? {
			let suffix = OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
				.map(|(_, _, suffix)| suffix)
				.filter(|suffix| suffix.len() == ANCHOR_SUFFIX_LEN)
				.ok_or_else(|| {
					Error::from(FlowStateError::Decode {
						state: "join seal anchor key",
						cause: "a join seal anchor carries a side tag and a row number"
							.to_string(),
					})
				})?;
			let side = JoinSide::from_tag(suffix[0]).ok_or_else(|| {
				Error::from(FlowStateError::Decode {
					state: "join seal anchor key",
					cause: format!("unknown join side tag {}", suffix[0]),
				})
			})?;
			let row_number = RowNumber(decode_u64_asc(
				<[u8; 8]>::try_from(&suffix[1..]).expect("the suffix length was checked"),
			));
			out.push((side, row_number, decode::<SealAnchor>(&row)?.expiry));
		}
		Ok(out)
	}

	fn resolve_groups(
		host: &mut dyn HostContext,
		cleared: &[(Hash128, RowNumber)],
		armed: &[(Hash128, RowNumber, DateTime)],
	) -> Result<HashMap<Hash128, GroupId>> {
		let mut distinct: Vec<Hash128> = Vec::new();
		let mut seen: HashSet<Hash128> = HashSet::new();
		for hash in cleared.iter().map(|(hash, _)| hash).chain(armed.iter().map(|(hash, _, _)| hash)) {
			if seen.insert(*hash) {
				distinct.push(*hash);
			}
		}
		let keys: Vec<EncodedKey> = distinct.iter().map(group_bytes).collect();
		let mut resolved: HashMap<Hash128, GroupId> = HashMap::new();
		for (hash, group) in distinct.into_iter().zip(host.lookup_groups(&keys)?) {
			if let Some(group) = group {
				resolved.insert(hash, group);
			}
		}
		Ok(resolved)
	}

	fn load_anchors(
		host: &mut dyn HostContext,
		order: &mut Vec<GroupId>,
		loaded: &mut HashMap<GroupId, GroupAnchors>,
		group: GroupId,
	) -> Result<()> {
		if loaded.contains_key(&group) {
			return Ok(());
		}
		let mut live: HashMap<(u8, u64), DateTime> = HashMap::new();
		for (side, row_number, expiry) in Self::anchors_in(host, group)? {
			live.insert((side.tag(), row_number.0), expiry);
		}
		order.push(group);
		loaded.insert(
			group,
			GroupAnchors {
				before: live.values().copied().min(),
				live,
			},
		);
		Ok(())
	}

	fn resync_timers(
		host: &mut dyn HostContext,
		order: &[GroupId],
		loaded: &HashMap<GroupId, GroupAnchors>,
	) -> Result<()> {
		for group in order {
			let anchors = loaded.get(group).expect("every ordered group was loaded");
			match anchors.live.values().copied().min() {
				Some(earliest) => {
					host.arm_timer(earliest, TimerKind::Maintenance, &Self::timer_key(*group))?
				}
				None => {
					if let Some(at) = anchors.before {
						host.disarm_timer(
							at,
							TimerKind::Maintenance,
							&Self::timer_key(*group),
						)?;
					}
				}
			}
		}
		Ok(())
	}

	fn move_anchors(
		&self,
		host: &mut dyn HostContext,
		side: JoinSide,
		cleared: &[(Hash128, RowNumber)],
		armed: &[(Hash128, RowNumber, DateTime)],
	) -> Result<()> {
		let Some(seal) = self.seal_of(side) else {
			return Ok(());
		};
		if cleared.is_empty() && armed.is_empty() {
			return Ok(());
		}
		let policy = SealPolicy::of(seal);
		let now = host.written_at();
		let resolved = Self::resolve_groups(host, cleared, armed)?;
		let mut order: Vec<GroupId> = Vec::new();
		let mut loaded: HashMap<GroupId, GroupAnchors> = HashMap::new();

		for (hash, row_number) in cleared {
			let Some(group) = resolved.get(hash).copied() else {
				continue;
			};
			Self::load_anchors(host, &mut order, &mut loaded, group)?;
			loaded.get_mut(&group)
				.expect("the group was just loaded")
				.live
				.remove(&(side.tag(), row_number.0));
			host.state_remove(&Self::anchor_key(group, side, *row_number))?;
		}

		for (hash, row_number, at) in armed {
			let Some(group) = resolved.get(hash).copied() else {
				continue;
			};
			Self::load_anchors(host, &mut order, &mut loaded, group)?;
			host.state_remove(&queue_key(group))?;
			let expiry = policy.seal_instant(*at).at();
			loaded.get_mut(&group)
				.expect("the group was just loaded")
				.live
				.insert((side.tag(), row_number.0), expiry);
			host.state_set(
				&Self::anchor_key(group, side, *row_number),
				SealAnchor {
					expiry,
				}
				.encode_state(now)?,
			)?;
		}

		Self::resync_timers(host, &order, &loaded)
	}

	fn arm_batch(
		&self,
		host: &mut dyn HostContext,
		side: JoinSide,
		columns: &Columns,
		keys: &[Option<Hash128>],
	) -> Result<()> {
		if self.seal_of(side).is_none() {
			return Ok(());
		}
		let times = columns.time();
		let mut armed = Vec::with_capacity(keys.len());
		for (row_idx, key) in keys.iter().enumerate() {
			let (Some(hash), Some(at)) = (key, times.get(row_idx)) else {
				continue;
			};
			armed.push((*hash, columns.row_numbers()[row_idx], *at));
		}
		self.move_anchors(host, side, &[], &armed)
	}

	fn clear_batch(
		&self,
		host: &mut dyn HostContext,
		side: JoinSide,
		columns: &Columns,
		keys: &[Option<Hash128>],
	) -> Result<()> {
		if self.seal_of(side).is_none() {
			return Ok(());
		}
		let mut cleared = Vec::with_capacity(keys.len());
		for (row_idx, key) in keys.iter().enumerate() {
			let Some(hash) = key else {
				continue;
			};
			cleared.push((*hash, columns.row_numbers()[row_idx]));
		}
		self.move_anchors(host, side, &cleared, &[])
	}

	fn move_row_anchor(
		&self,
		host: &mut dyn HostContext,
		side: JoinSide,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		keys: (Option<Hash128>, Option<Hash128>),
	) -> Result<()> {
		if self.seal_of(side).is_none() {
			return Ok(());
		}
		let mut cleared: Vec<(Hash128, RowNumber)> = Vec::new();
		if let Some(hash) = keys.0 {
			cleared.push((hash, pre.row_numbers()[row_idx]));
		}
		let mut armed: Vec<(Hash128, RowNumber, DateTime)> = Vec::new();
		if let (Some(hash), Some(at)) = (keys.1, post.time().get(row_idx).copied()) {
			armed.push((hash, post.row_numbers()[row_idx], at));
		}
		self.move_anchors(host, side, &cleared, &armed)
	}

	fn free_sealed_row(
		&self,
		host: &mut dyn HostContext,
		state: &JoinState,
		group: GroupId,
		side: JoinSide,
		row_number: RowNumber,
	) -> Result<()> {
		match side {
			JoinSide::Left => {
				if self.snapshot {
					let ledger = self.snapshot_ledger();
					for (right, _) in ledger.published(host, group, row_number)? {
						match right {
							PublishedRight::Unmatched => {
								ledger.release_unmatched(host, group, row_number)?
							}
							PublishedRight::Row(right_number) => {
								ledger.release(host, group, row_number, right_number)?;
							}
						}
					}
				}
				self.cleanup_left_row_joins(host, row_number.0)?;
				state.left.remove_row_in(host, group, row_number)?;
			}
			JoinSide::Right => {
				for left_number in state.left.row_numbers_in(host, group)? {
					host.remove_row_number(
						GroupId::ROOT,
						&Self::make_composite_key(left_number, row_number),
					)?;
				}
				state.right.remove_row_in(host, group, row_number)?;
			}
		}
		host.state_remove(&Self::anchor_key(group, side, row_number))
	}

	fn seal_group(&self, host: &mut dyn HostContext, fired: FiredAt, group: GroupId) -> Result<()> {
		let Some(span) = self.widest_seal() else {
			return Ok(());
		};

		let mut pending: Option<DateTime> = None;
		let mut due: Vec<(JoinSide, RowNumber)> = Vec::new();
		for (side, row_number, expiry) in Self::anchors_in(host, group)? {
			if expiry > fired.at() {
				pending = Some(pending.map_or(expiry, |earliest: DateTime| earliest.min(expiry)));
			} else {
				due.push((side, row_number));
			}
		}

		let state = JoinState::new();
		for (side, row_number) in due.iter().filter(|(side, _)| *side == JoinSide::Left) {
			self.free_sealed_row(host, &state, group, *side, *row_number)?;
		}
		for (side, row_number) in due.iter().filter(|(side, _)| *side == JoinSide::Right) {
			self.free_sealed_row(host, &state, group, *side, *row_number)?;
		}

		if let Some(at) = pending {
			host.arm_timer(at, TimerKind::Maintenance, &Self::timer_key(group))?;
		} else if !state.left.holds_rows(host, group)? && !state.right.holds_rows(host, group)? {
			enqueue(host, group)?;
		}

		drain(host, &mut StoreReaper, SEAL_BATCH)?;
		let retry = fired.at().saturating_add(span);
		for still_queued in queued(host, SEAL_BATCH)? {
			host.arm_timer(retry, TimerKind::Maintenance, &Self::timer_key(still_queued))?;
		}
		Ok(())
	}

	pub(crate) fn snapshot_ledger(&self) -> SnapshotLedger {
		SnapshotLedger::new()
	}

	#[instrument(name = "flow::operator::join::compute_keys", level = "trace", skip_all, fields(rows = columns.row_count()))]
	pub(crate) fn compute_join_keys(
		&self,
		columns: &Columns,
		compiled_exprs: &[CompiledExpr],
	) -> Result<Vec<Option<Hash128>>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let session = EvalContext {
			params: &self.ctx.params,
			symbols: &self.ctx.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.ctx.identity,
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut expr_columns = Vec::with_capacity(compiled_exprs.len());
		for compiled_expr in compiled_exprs.iter() {
			let col: ColumnWithName = if let Some(col_name) = compiled_expr.access_column_name() {
				columns.column(col_name)
					.map(|c| ColumnWithName::new(c.name().clone(), c.data().clone()))
					.unwrap_or_else(|| {
						ColumnWithName::undefined_typed(col_name, ValueType::Boolean, row_count)
					})
			} else {
				compiled_expr.execute(&exec_ctx)?
			};
			expr_columns.push(col);
		}

		let mut hashes = Vec::with_capacity(row_count);
		let mut buf: Vec<u8> = Vec::with_capacity(256);
		for row_idx in 0..row_count {
			buf.clear();
			let mut has_undefined = false;

			for col in &expr_columns {
				let value = col.data().get_value(row_idx);

				if matches!(value, Value::None { .. }) {
					has_undefined = true;
					break;
				}

				buf = to_extend(&value, buf).map_err(|e| {
					Error::from(FlowStateError::Encode {
						state: "value for hash",
						cause: e.to_string(),
					})
				})?;
			}

			if has_undefined {
				hashes.push(None);
			} else {
				hashes.push(Some(xxh3_128(&buf)));
			}
		}

		Ok(hashes)
	}

	pub(crate) fn unmatched_left_columns(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_idx: usize,
		identity: Identity,
	) -> Result<Emitted> {
		let left_row_number = left.row_numbers()[left_idx];

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_row_number.0);
		let composite_key = serializer.finish();

		let (row_numbers, fresh, existing) = self.identities(host, &[composite_key], identity)?;
		if fresh.is_empty() && existing.is_empty() {
			return Ok(Emitted::empty());
		}

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		let built = builder.unmatched_left(row_numbers[0], left, left_idx, &self.right_schema);
		Ok(Self::split(built, &fresh, &existing))
	}

	fn identities(
		&self,
		host: &mut dyn HostContext,
		keys: &[EncodedKey],
		identity: Identity,
	) -> Result<(Vec<RowNumber>, Vec<usize>, Vec<usize>)> {
		match identity {
			Identity::Mint => {
				let minted = host.get_or_create_row_numbers(GroupId::ROOT, keys)?;
				let (fresh, existing) = (0..keys.len()).partition(|index| minted[*index].1);
				Ok((minted.iter().map(|(number, _)| *number).collect(), fresh, existing))
			}
			Identity::Existing | Identity::Consume => {
				let resolved = host.get_row_numbers(GroupId::ROOT, keys)?;
				let existing: Vec<usize> = resolved
					.iter()
					.enumerate()
					.filter_map(|(index, number)| number.map(|_| index))
					.collect();
				if identity == Identity::Consume {
					for index in &existing {
						host.remove_row_number(GroupId::ROOT, &keys[*index])?;
					}
				}
				Ok((
					resolved.into_iter().map(|number| number.unwrap_or(RowNumber(0))).collect(),
					Vec::new(),
					existing,
				))
			}
		}
	}

	fn split(built: Columns, fresh: &[usize], existing: &[usize]) -> Emitted {
		Emitted {
			fresh: JoinedColumnsBuilder::retain_rows(&built, fresh),
			existing: JoinedColumnsBuilder::retain_rows(&built, existing),
		}
	}

	pub(crate) fn unmatched_left_columns_batch(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_indices: &[usize],
		identity: Identity,
	) -> Result<Emitted> {
		if left_indices.is_empty() {
			return Ok(Emitted::empty());
		}

		let composite_keys: Vec<EncodedKey> = left_indices
			.iter()
			.map(|&idx| {
				let left_row_number = left.row_numbers()[idx];
				let mut serializer = KeySerializer::new();
				serializer.extend_u8(b'L');
				serializer.extend_u64(left_row_number.0);
				serializer.finish()
			})
			.collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		let built = builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn cleanup_left_row_joins(&self, host: &mut dyn HostContext, left_number: u64) -> Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		host.remove_row_numbers_by_prefix(GroupId::ROOT, &prefix)
	}

	fn make_composite_key(left_num: RowNumber, right_num: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_num.0);
		serializer.extend_u64(right_num.0);
		serializer.finish()
	}

	pub(crate) fn join_columns_one_to_many(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		identity: Identity,
	) -> Result<Emitted> {
		let right_count = right.row_count();
		if right_count == 0 {
			return Ok(Emitted::empty());
		}

		let left_row_number = left.row_numbers()[left_idx];

		let composite_keys: Vec<EncodedKey> = (0..right_count)
			.map(|right_idx| {
				let right_row_number = right.row_numbers()[right_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_one_to_many(&row_numbers, left, left_idx, right);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn join_columns_many_to_one(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		right: &Columns,
		right_idx: usize,
		identity: Identity,
	) -> Result<Emitted> {
		let left_count = left.row_count();
		if left_count == 0 {
			return Ok(Emitted::empty());
		}

		let right_row_number = right.row_numbers()[right_idx];

		let composite_keys: Vec<EncodedKey> = (0..left_count)
			.map(|left_idx| {
				let left_row_number = left.row_numbers()[left_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_many_to_one(&row_numbers, left, right, right_idx);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn join_columns_cartesian(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
		right_indices: &[usize],
		identity: Identity,
	) -> Result<Emitted> {
		let left_count = left_indices.len();
		let right_count = right_indices.len();
		if left_count == 0 || right_count == 0 {
			return Ok(Emitted::empty());
		}

		let total_results = left_count * right_count;
		let mut composite_keys = Vec::with_capacity(total_results);

		for &left_idx in left_indices {
			let left_row_number = left.row_numbers()[left_idx];
			for &right_idx in right_indices {
				let right_row_number = right.row_numbers()[right_idx];
				composite_keys.push(Self::make_composite_key(left_row_number, right_row_number));
			}
		}

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_cartesian(&row_numbers, left, left_indices, right, right_indices);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn join_left_with_slot(&self, left: &Columns, left_indices: &[usize], slot: &Columns) -> Columns {
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();
		let builder = JoinedColumnsBuilder::new(left, slot, &self.alias, self.natural);
		builder.join_cartesian(&row_numbers, left, left_indices, slot, &[0])
	}

	pub(crate) fn unmatched_left_latest(&self, left: &Columns, left_indices: &[usize]) -> Columns {
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();
		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema)
	}

	fn determine_side_from_origin(&self, origin: &ChangeOrigin) -> Option<JoinSide> {
		match origin {
			ChangeOrigin::Flow(from_node) => {
				if *from_node == self.left_node {
					Some(JoinSide::Left)
				} else if *from_node == self.right_node {
					Some(JoinSide::Right)
				} else {
					None
				}
			}
			_ => None,
		}
	}
}

impl HostOperator for JoinOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::default())
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.operator
		{
			return Ok(Change::from_flow(self.operator, change.version, Vec::new(), DateTime::default()));
		}

		if self.natural && self.compiled_left_exprs.is_empty() {
			return Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at));
		}

		let mut state = JoinState::new();
		let mut result = Vec::with_capacity(change.diffs.len() * 2);

		let version = change.version;
		let parent_origin = change.origin.clone();
		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let side = self.determine_side_from_origin(&diff_origin).ok_or_else(|| {
				Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Join",
					origin: None,
				})
			})?;
			let compiled_exprs = match side {
				JoinSide::Left => &self.compiled_left_exprs,
				JoinSide::Right => &self.compiled_right_exprs,
			};
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_join_insert(host, &post, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_join_remove(host, &pre, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_join_update(
					host,
					&pre,
					&post,
					compiled_exprs,
					side,
					&mut state,
					&mut result,
				)?,
			}
		}

		Ok(Change::from_flow(self.operator, version, result, change.changed_at))
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		if timer.kind == TimerKind::Maintenance {
			self.seal_group(host, FiredAt::of(&timer), Self::timer_group(&timer.key)?)?;
		}
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		self.widest_seal()
	}
}

impl JoinOperator {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_join_insert(
		&self,
		host: &mut dyn HostContext,
		post: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(post, compiled_exprs)?;

		if !self.latest {
			for (row_idx, key) in keys.iter().enumerate() {
				let mut ctx = JoinContext {
					side,
					state,
					operator: self,
				};
				let diffs = match key {
					Some(key_hash) => self.strategy.handle_insert(
						host,
						post,
						&[row_idx],
						key_hash,
						&mut ctx,
					)?,
					None => self.strategy.handle_insert_undefined(host, post, row_idx, &mut ctx)?,
				};
				result.extend(diffs);
			}
			return self.arm_batch(host, side, post, &keys);
		}

		let (order, groups, undefined) = group_by_key(&keys);

		for key_hash in &order {
			let indices = &groups[key_hash];
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_insert(host, post, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_insert_undefined(host, post, row_idx, &mut ctx)?);
		}

		self.arm_batch(host, side, post, &keys)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_join_remove(
		&self,
		host: &mut dyn HostContext,
		pre: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(pre, compiled_exprs)?;

		if !self.latest {
			for (row_idx, key) in keys.iter().enumerate() {
				let mut ctx = JoinContext {
					side,
					state,
					operator: self,
				};
				let diffs = match key {
					Some(key_hash) => self.strategy.handle_remove(
						host,
						pre,
						&[row_idx],
						key_hash,
						&mut ctx,
					)?,
					None => self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?,
				};
				result.extend(diffs);
			}
			return self.clear_batch(host, side, pre, &keys);
		}

		let (order, groups, undefined) = group_by_key(&keys);

		for key_hash in &order {
			let indices = &groups[key_hash];
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_remove(host, pre, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?);
		}

		self.clear_batch(host, side, pre, &keys)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_join_update(
		&self,
		host: &mut dyn HostContext,
		pre: &Columns,
		post: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let pre_keys = self.compute_join_keys(pre, compiled_exprs)?;
		let post_keys = self.compute_join_keys(post, compiled_exprs)?;
		let row_count = post.row_count();

		for row_idx in 0..row_count {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			let diffs = match (pre_keys[row_idx], post_keys[row_idx]) {
				(Some(pre_key), Some(post_key)) => {
					let keys = UpdateKeys {
						pre: &pre_key,
						post: &post_key,
					};
					self.strategy.handle_update(host, pre, post, &[row_idx], keys, &mut ctx)?
				}
				(Some(pre_key), None) => {
					let mut diffs = self.strategy.handle_remove(
						host,
						pre,
						&[row_idx],
						&pre_key,
						&mut ctx,
					)?;
					diffs.extend(self
						.strategy
						.handle_insert_undefined(host, post, row_idx, &mut ctx)?);
					diffs
				}
				(None, Some(post_key)) => {
					let mut diffs =
						self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?;
					diffs.extend(self.strategy.handle_insert(
						host,
						post,
						&[row_idx],
						&post_key,
						&mut ctx,
					)?);
					diffs
				}
				(None, None) => self
					.strategy
					.handle_update_both_undefined(host, pre, post, row_idx, &mut ctx)?,
			};
			result.extend(diffs);
			self.move_row_anchor(host, side, pre, post, row_idx, (pre_keys[row_idx], post_keys[row_idx]))?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod seal_tests {
	use reifydb_core::{
		common::CommitVersion,
		key::operator_state::{group_inner_range, keyspace_inner_range},
		value::column::buffer::ColumnBuffer,
	};
	use reifydb_rql::expression::parse_expression;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, fragment::Fragment};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{
			ChangeCoordinate, FlowTransaction, deferred::DeferredTransaction, group::GroupExtension,
			mock::FlowTxn, row_number::RowNumberExtension, state::StateExtension, timer::TimerExtension,
		},
	};

	fn join(operator: u64, left_seal: Option<Duration>, right_seal: Option<Duration>) -> JoinOperator {
		join_with(operator, false, false, left_seal, right_seal)
	}

	fn join_with(
		operator: u64,
		snapshot: bool,
		latest: bool,
		left_seal: Option<Duration>,
		right_seal: Option<Duration>,
	) -> JoinOperator {
		JoinOperator::new(
			JoinSideConfig {
				operator: OperatorId(operator + 1_000),
				exprs: parse_expression("k").expect("the left key parses"),
				schema: Columns::empty(),
			},
			JoinSideConfig {
				operator: OperatorId(operator + 2_000),
				exprs: parse_expression("k").expect("the right key parses"),
				schema: Columns::empty(),
			},
			OperatorId(operator),
			JoinType::Left,
			None,
			Routines::empty(),
			RuntimeContext::testing(0, 1),
			snapshot,
			false,
			latest,
			left_seal,
			right_seal,
			Arc::new(FlowContext::default()),
		)
	}

	fn seconds(value: i64) -> Duration {
		Duration::from_seconds(value).unwrap()
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(coordinate)),
			version: CommitVersion(coordinate),
		});
		txn
	}

	fn rows(keys: &[i32], numbers: &[u64], at: DateTime) -> Columns {
		let column = ColumnWithName::new(Fragment::internal("k"), ColumnBuffer::int4(keys.to_vec()));
		let mut columns =
			Columns::new(vec![column]).with_row_numbers(numbers.iter().map(|n| RowNumber(*n)).collect());
		columns.system.set_time(vec![at; keys.len()]);
		columns
	}

	fn exprs(op: &JoinOperator, side: JoinSide) -> &[CompiledExpr] {
		match side {
			JoinSide::Left => &op.compiled_left_exprs,
			JoinSide::Right => &op.compiled_right_exprs,
		}
	}

	fn insert(op: &JoinOperator, txn: &mut DeferredTransaction, side: JoinSide, post: &Columns) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		op.apply_join_insert(
			&mut TxnHostContext::new(txn, op.operator),
			post,
			exprs(op, side),
			side,
			&mut state,
			&mut result,
		)
		.unwrap();
		result
	}

	fn remove(op: &JoinOperator, txn: &mut DeferredTransaction, side: JoinSide, pre: &Columns) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		op.apply_join_remove(
			&mut TxnHostContext::new(txn, op.operator),
			pre,
			exprs(op, side),
			side,
			&mut state,
			&mut result,
		)
		.unwrap();
		result
	}

	fn update(
		op: &JoinOperator,
		txn: &mut DeferredTransaction,
		side: JoinSide,
		pre: &Columns,
		post: &Columns,
	) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		op.apply_join_update(
			&mut TxnHostContext::new(txn, op.operator),
			pre,
			post,
			exprs(op, side),
			side,
			&mut state,
			&mut result,
		)
		.unwrap();
		result
	}

	fn hash_of(op: &JoinOperator, side: JoinSide, columns: &Columns, row_idx: usize) -> Hash128 {
		op.compute_join_keys(columns, exprs(op, side)).unwrap()[row_idx].expect("the join key is defined")
	}

	fn group_of(op: &JoinOperator, txn: &mut DeferredTransaction, hash: &Hash128) -> Option<GroupId> {
		txn.lookup_group(op.operator, &group_bytes(hash)).unwrap()
	}

	fn anchor_of(
		op: &JoinOperator,
		txn: &mut DeferredTransaction,
		group: GroupId,
		side: JoinSide,
		row_number: u64,
	) -> Option<DateTime> {
		let key = JoinOperator::anchor_key(group, side, RowNumber(row_number));
		txn.state_get(op.operator, &key).unwrap().map(|row| decode::<SealAnchor>(&row).unwrap().expiry)
	}

	fn armed_timers(op: &JoinOperator, txn: &mut DeferredTransaction) -> usize {
		txn.state_range(op.operator, keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL), None, "test")
			.unwrap()
			.items
			.len()
	}

	fn group_rows(op: &JoinOperator, txn: &mut DeferredTransaction, group: GroupId) -> usize {
		txn.state_range(op.operator, group_inner_range(group), None, "test").unwrap().items.len()
	}

	fn side_rows(op: &JoinOperator, txn: &mut DeferredTransaction, group: GroupId, side: JoinSide) -> usize {
		txn.state_range(op.operator, keyspace_inner_range(group, side.keyspace()), None, "test")
			.unwrap()
			.items
			.len()
	}

	fn ledger_rows(op: &JoinOperator, txn: &mut DeferredTransaction, group: GroupId, keyspace: Keyspace) -> usize {
		txn.state_range(op.operator, keyspace_inner_range(group, keyspace), None, "test").unwrap().items.len()
	}

	fn unmatched_mapping(op: &JoinOperator, txn: &mut DeferredTransaction, left: u64) -> Option<RowNumber> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left);
		txn.get_row_number(op.operator, GroupId::ROOT, &serializer.finish()).unwrap()
	}

	fn fire(op: &mut JoinOperator, txn: &mut DeferredTransaction, at: DateTime, group: GroupId) -> Option<Change> {
		// The engine lifts a due timer off the wheel before dispatch, so skipping the disarm reads as a leak.
		let operator = op.operator;
		let timer = Timer {
			at,
			kind: TimerKind::Maintenance,
			key: JoinOperator::timer_key(group),
		};
		txn.disarm_timer(operator, &timer).unwrap();
		op.on_timer(&mut TxnHostContext::new(txn, operator), timer).unwrap()
	}

	#[test]
	fn an_inserted_row_is_armed_one_seal_past_its_own_event_time() {
		// The anchor must be the row's own event time; a wall-clock seal evicts a backfilled row on arrival.
		let engine = TestEngine::new();
		let op = join(1, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));

		insert(&op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0))
			.expect("storing a row must intern its join key");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the due time is event time + seal + the strict gate step"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and exactly one timer addresses that key's group");
	}

	#[test]
	fn an_update_moves_the_rows_timer_rather_than_adding_a_second() {
		// Without cancelling the old arming the row is addressed twice and the stale one fires while it lives.
		let engine = TestEngine::new();
		let op = join(2, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let pre = rows(&[7], &[42], at_millis(5_000));
		let post = rows(&[7], &[42], at_millis(20_000));
		insert(&op, &mut txn, JoinSide::Left, &pre);

		update(&op, &mut txn, JoinSide::Left, &pre, &post);

		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &post, 0)).expect("the key is interned");
		assert_eq!(armed_timers(&op, &mut txn), 1, "an update re-arms one timer, it does not add one");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(30_001)),
			"and the due time follows the row's own last write"
		);
	}

	#[test]
	fn a_row_still_inside_its_seal_survives_a_maintenance_tick() {
		// The gate is strict: a row whose due time lands one tick past the fire must not seal yet.
		let engine = TestEngine::new();
		let mut op = join(3, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");

		fire(&mut op, &mut txn, at_millis(15_000), group);

		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 1, "the row must keep its state");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"and its anchor must survive untouched"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "its timer is not due and must be armed again");
	}

	#[test]
	fn removing_a_row_takes_its_anchor_and_its_timer_with_it() {
		// A source delete leaves no row to seal, so an anchor left behind fires against state that is gone.
		let engine = TestEngine::new();
		let op = join(4, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");

		remove(&op, &mut txn, JoinSide::Left, &left);

		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			None,
			"the anchor must go with the row"
		);
		assert_eq!(armed_timers(&op, &mut txn), 0, "and the timer that addressed it must be disarmed");
	}

	#[test]
	fn a_sibling_row_under_the_same_key_outlives_its_neighbours_seal() {
		// A join group holds many rows, so sealing must free exactly one and re-arm on the next earliest.
		let engine = TestEngine::new();
		let mut op = join(5, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let early = rows(&[7], &[1], at_millis(5_000));
		let late = rows(&[7], &[2], at_millis(50_000));
		insert(&op, &mut txn, JoinSide::Left, &early);
		insert(&op, &mut txn, JoinSide::Left, &late);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &early, 0)).expect("the key is interned");

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(anchor_of(&op, &mut txn, group, JoinSide::Left, 1), None, "the due row's anchor must go");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 2),
			Some(at_millis(60_001)),
			"its neighbour under the same key must keep its own anchor"
		);
		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 1, "exactly one row was freed");
		assert_eq!(armed_timers(&op, &mut txn), 1, "and the group re-arms on the next earliest anchor");
	}

	#[test]
	fn right_side_traffic_never_extends_a_left_rows_anchor() {
		// A match is not a write to the left row; advancing its anchor would keep a joined row alive forever.
		let engine = TestEngine::new();
		let op = join(6, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");

		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left anchor must still name the left row's own last write"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and a side with no seal must arm nothing of its own");
	}

	#[test]
	fn a_join_without_a_seal_arms_nothing_at_all() {
		// Arming without a seal leaves one timer and one anchor per row that nothing ever collects.
		let engine = TestEngine::new();
		let op = join(7, None, None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));

		insert(&op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0))
			.expect("the row must still intern its key");
		assert_eq!(anchor_of(&op, &mut txn, group, JoinSide::Left, 42), None, "no seal means no anchor");
		assert_eq!(armed_timers(&op, &mut txn), 0, "and no timer");
	}

	#[test]
	fn a_sealed_row_frees_its_state_its_anchor_and_its_output_mapping_without_emitting() {
		// A sealed row is frozen downstream, so it must emit nothing while its mapping is reclaimed.
		let engine = TestEngine::new();
		let mut op = join(8, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");
		assert!(unmatched_mapping(&op, &mut txn, 42).is_some(), "precondition: the row published downstream");

		let emitted = fire(&mut op, &mut txn, at_millis(15_001), group);

		assert!(emitted.is_none(), "sealing must publish no diff at all");
		assert_eq!(anchor_of(&op, &mut txn, group, JoinSide::Left, 42), None, "the anchor must go");
		assert_eq!(
			unmatched_mapping(&op, &mut txn, 42),
			None,
			"and so must the output row number, or one leaks per emitted row forever"
		);
	}

	#[test]
	fn a_key_whose_last_row_sealed_loses_the_group_that_carried_it() {
		// The dictionary entry and the group record only leave through the reaper, so the seal must queue it.
		let engine = TestEngine::new();
		let mut op = join(9, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&op, &mut txn, &hash).expect("the key is interned");

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(group_of(&op, &mut txn, &hash), None, "the dictionary entry must go");
		assert_eq!(group_rows(&op, &mut txn, group), 0, "and the group's range must be left empty");
		assert_eq!(armed_timers(&op, &mut txn), 0, "and the timer that drove the seal must not re-arm");
	}

	#[test]
	fn a_group_is_only_reaped_once_both_sides_hold_nothing() {
		// Both sides share one group, so queueing on the first empty side reaps rows the other still reads.
		let engine = TestEngine::new();
		let mut op = join(10, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&op, &mut txn, &hash).expect("the key is interned");

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 0, "the left row is past its own seal");
		assert_eq!(
			side_rows(&op, &mut txn, group, JoinSide::Right),
			1,
			"an unsealed side keeps its rows and no anchor holds the group open on its behalf"
		);
		assert!(group_of(&op, &mut txn, &hash).is_some(), "so the group that still carries it must survive");
	}

	#[test]
	fn a_row_short_of_its_own_seal_holds_the_groups_timer_open() {
		// The two sides seal on independent spans, so the group re-arms on whichever anchor is next due.
		let engine = TestEngine::new();
		let mut op = join(11, Some(seconds(10)), Some(seconds(3_600)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Left, &left);
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&op, &mut txn, &hash).expect("the key is interned");

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 0, "the left row is past its own seal");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Right, 99),
			Some(at_millis(3_609_001)),
			"the right row keeps the anchor its own longer seal gave it"
		);
		assert_eq!(
			armed_timers(&op, &mut txn),
			1,
			"and the group re-arms on that anchor rather than dropping it"
		);
	}

	#[test]
	fn a_latest_join_seals_its_left_side_and_arms_nothing_on_the_right() {
		// A slot overwritten in place carries no per-row anchor, so a right seal there could never fire.
		let engine = TestEngine::new();
		let op = join_with(12, false, true, Some(seconds(10)), Some(seconds(10)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		insert(&op, &mut txn, JoinSide::Left, &left);

		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left side is an ordinary row set and must seal normally"
		);
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Right, 99),
			None,
			"the right slot must arm nothing"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "so only the left anchor addresses the group");
	}

	#[test]
	fn a_snapshot_join_seals_its_left_side_and_arms_nothing_on_the_right() {
		// A pinned right row must outlive the left rows that published against it, so it must never seal.
		let engine = TestEngine::new();
		let op = join_with(13, true, false, Some(seconds(10)), Some(seconds(10)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		insert(&op, &mut txn, JoinSide::Left, &left);

		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left side is unaffected by the flag and must seal normally"
		);
		assert_eq!(
			anchor_of(&op, &mut txn, group, JoinSide::Right, 99),
			None,
			"the pinned right row must not arm"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "so only the left anchor addresses the group");
	}

	#[test]
	fn a_sealed_left_row_releases_the_snapshot_ledger_it_held() {
		// A pin is refcounted, so a left row that seals without releasing holds its retired copy forever.
		let engine = TestEngine::new();
		let mut op = join_with(14, true, false, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		insert(&op, &mut txn, JoinSide::Left, &left);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &left, 0)).expect("the key is interned");
		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PUBLISHED),
			1,
			"precondition: it published"
		);
		assert_eq!(ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PIN), 1, "precondition: it pinned");

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PUBLISHED),
			0,
			"the published pair must go with the left row that owned it"
		);
		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PIN),
			0,
			"and its last reference must take the pin with it"
		);
		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Right), 1, "while the right row it read survives");
	}

	#[test]
	fn a_siblings_pin_on_the_same_right_row_survives_its_neighbours_seal() {
		// Release must be scoped to the sealing left row, or a live sibling loses the version it published.
		let engine = TestEngine::new();
		let mut op = join_with(15, true, false, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let early = rows(&[7], &[1], at_millis(5_000));
		let late = rows(&[7], &[2], at_millis(50_000));
		insert(&op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		insert(&op, &mut txn, JoinSide::Left, &early);
		insert(&op, &mut txn, JoinSide::Left, &late);
		let group =
			group_of(&op, &mut txn, &hash_of(&op, JoinSide::Left, &early, 0)).expect("the key is interned");
		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PUBLISHED),
			2,
			"precondition: both published"
		);

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PUBLISHED),
			1,
			"exactly the sealed row's pair goes, never its neighbour's"
		);
		assert_eq!(
			ledger_rows(&op, &mut txn, group, Keyspace::JOIN_PIN),
			1,
			"and the pin the sibling still references must stay"
		);
	}
}
