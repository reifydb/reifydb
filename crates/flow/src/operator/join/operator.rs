// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use postcard::to_extend;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::JoinType,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	key::{
		operator::{
			keyspace::join::{JoinExpiryDueKey, JoinRowMappingKey},
			state::GroupId,
		},
		typed::direction::{Asc, Desc},
	},
	metrics::{heap::OperatorSample, instruments::counter::Counter},
	row::JoinPick,
	state::timer::TimerKind,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
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
	snapshot::{Numbering, SnapshotLedger},
	state::{JoinSide, JoinState},
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::{
	context::FlowContext,
	error::{FlowGraphError, FlowStateError},
	operator::{
		HostOperator,
		host::HostContext,
		join::{Emitted, Identity, expiry::JoinExpiryIndex},
		state::{
			reaper::{StoreReaper, drain, drain_group, enqueue, queue_key, queued},
			seal::{ledger::FiredAt, rule::SealRule},
		},
	},
	timer::Timer,
	transaction::join_expiry::{JoinDueEntry, join_expiry_range},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

const JOIN_MAPPING_TAG: u8 = b'L';

const UNMATCHED_RIGHT: u64 = 0;

const SEAL_BATCH: usize = 256;

const QUEUE_SWEEP_EVERY: u64 = 1024;

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
	pub(crate) pick: Option<JoinPick>,
	left_retention: Option<Duration>,
	right_retention: Option<Duration>,
	ctx: Arc<FlowContext>,
	seal_fires: Counter,
	expiry: JoinExpiryIndex,
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
		pick: Option<JoinPick>,
		left_retention: Option<Duration>,
		right_retention: Option<Duration>,
		ctx: Arc<FlowContext>,
	) -> Self {
		let left_node = left.operator;
		let right_node = right.operator;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type, pick.is_some());

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
			pick,
			left_retention: left_retention.filter(|span| !span.is_zero()),
			right_retention: right_retention.filter(|span| !span.is_zero()),
			ctx,
			seal_fires: Counter::new("flow.operator.join.seal_fires_total", "Join seal timer fires"),
			expiry: JoinExpiryIndex::default(),
		}
	}

	pub(crate) fn retention_of(&self, side: JoinSide) -> Option<Duration> {
		match side {
			JoinSide::Left => self.left_retention,
			JoinSide::Right => self.right_retention,
		}
	}

	fn compiled_exprs_of(&self, side: JoinSide) -> &[CompiledExpr] {
		match side {
			JoinSide::Left => &self.compiled_left_exprs,
			JoinSide::Right => &self.compiled_right_exprs,
		}
	}

	fn widest_retention(&self) -> Option<Duration> {
		match (self.retention_of(JoinSide::Left), self.retention_of(JoinSide::Right)) {
			(Some(left), Some(right)) => Some(left.max(right)),
			(left, right) => left.or(right),
		}
	}

	fn timer_key() -> EncodedKey {
		EncodedKey::new(Vec::new())
	}

	fn side_of(tag: u8) -> Result<JoinSide> {
		JoinSide::from_tag(tag).ok_or_else(|| {
			Error::from(FlowStateError::Decode {
				state: "join expiry key",
				cause: format!("unknown join side tag {tag}"),
			})
		})
	}

	fn resolve_groups(
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
		Ok(distinct.into_iter().map(|hash| (hash, GroupId::hashed(hash))).collect())
	}

	fn resync_timer(&mut self, host: &mut dyn HostContext, retry: Option<DateTime>) -> Result<()> {
		let next = match (self.expiry.min(host)?, retry) {
			(Some(earliest), Some(retry)) => Some(earliest.min(retry)),
			(earliest, retry) => earliest.or(retry),
		};
		match next {
			Some(at) => host.arm_timer(at, TimerKind::Maintenance, &Self::timer_key()),
			None => host.disarm_timer_by_key(TimerKind::Maintenance, &Self::timer_key()),
		}
	}

	fn move_join_expiries(
		&mut self,
		host: &mut dyn HostContext,
		side: JoinSide,
		cleared: &[(Hash128, RowNumber)],
		armed: &[(Hash128, RowNumber, DateTime)],
	) -> Result<()> {
		let Some(retention) = self.retention_of(side) else {
			return Ok(());
		};
		if cleared.is_empty() && armed.is_empty() {
			return Ok(());
		}
		let rule = SealRule::of(retention);
		let resolved = Self::resolve_groups(cleared, armed)?;

		for (hash, row_number) in cleared {
			let Some(group) = resolved.get(hash).copied() else {
				continue;
			};
			host.join_expiry_clear(group, side.tag(), *row_number)?;
		}
		if !cleared.is_empty() {
			self.expiry.invalidate();
		}

		for (hash, row_number, at) in armed {
			let Some(group) = resolved.get(hash).copied() else {
				continue;
			};
			let sealed = rule.seal_instant(*at).at();
			host.state_remove(&queue_key(group))?;
			host.join_expiry_arm(group, side.tag(), *row_number, sealed)?;
			self.expiry.armed(sealed);
		}

		self.resync_timer(host, None)
	}

	fn arm_batch(
		&mut self,
		host: &mut dyn HostContext,
		side: JoinSide,
		columns: &Columns,
		keys: &[Option<Hash128>],
	) -> Result<()> {
		if self.retention_of(side).is_none() {
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
		self.move_join_expiries(host, side, &[], &armed)
	}

	fn clear_batch(
		&mut self,
		host: &mut dyn HostContext,
		side: JoinSide,
		columns: &Columns,
		keys: &[Option<Hash128>],
	) -> Result<()> {
		if self.retention_of(side).is_none() {
			return Ok(());
		}
		let mut cleared = Vec::with_capacity(keys.len());
		for (row_idx, key) in keys.iter().enumerate() {
			let Some(hash) = key else {
				continue;
			};
			cleared.push((*hash, columns.row_numbers()[row_idx]));
		}
		self.move_join_expiries(host, side, &cleared, &[])
	}

	fn move_row_join_expiry(
		&mut self,
		host: &mut dyn HostContext,
		side: JoinSide,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		keys: (Option<Hash128>, Option<Hash128>),
	) -> Result<()> {
		if self.retention_of(side).is_none() {
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
		self.move_join_expiries(host, side, &cleared, &armed)
	}

	fn free_expired_left_row(
		&self,
		host: &mut dyn HostContext,
		state: &JoinState,
		entry: &JoinDueEntry,
	) -> Result<()> {
		let group = entry.group;
		let row_number = entry.row_number;
		if self.snapshot {
			self.snapshot_ledger().release_all(host, group, row_number)?;
		}
		self.cleanup_left_row_joins(host, row_number.0)?;
		state.left.remove_row_in(host, group, row_number)?;
		host.join_expiry_free(entry)
	}

	fn free_expired_right_row(
		&self,
		host: &mut dyn HostContext,
		state: &JoinState,
		entry: &JoinDueEntry,
		left_numbers: &[RowNumber],
	) -> Result<()> {
		let group = entry.group;
		let row_number = entry.row_number;
		if self.snapshot
			&& let Some(content) = state.right.get_row_in(host, group, row_number)?
		{
			self.snapshot_ledger().retire(host, group, row_number, &content)?;
		}
		let composites: Vec<JoinRowMappingKey> = left_numbers
			.iter()
			.map(|left_number| Self::make_composite_key(*left_number, row_number))
			.collect();
		host.remove_join_row_numbers(&composites)?;
		state.right.remove_row_in(host, group, row_number)?;
		host.join_expiry_free(entry)
	}

	fn free_due_join_rows(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<()> {
		let Some(span) = self.widest_retention() else {
			return Ok(());
		};
		let retry = fired.at().saturating_add(span);

		self.seal_fires.inc();
		let mut stalled = false;
		if (self.seal_fires.get() as u64).is_multiple_of(QUEUE_SWEEP_EVERY) {
			let drained = drain(host, &mut StoreReaper, SEAL_BATCH)?;
			stalled = if drained.more {
				!queued(host, SEAL_BATCH)?.groups.is_empty()
			} else {
				!drained.still_queued.is_empty()
			};
		}

		let state = JoinState::new();
		let mut emptied: Vec<GroupId> = Vec::new();
		let mut seen: HashSet<GroupId> = HashSet::new();
		let mut cursor: Option<JoinExpiryDueKey> = None;
		loop {
			let page = host.join_due_page(fired.at(), SEAL_BATCH, cursor.as_ref())?;
			if page.due.is_empty() {
				break;
			}
			let mut order: Vec<GroupId> = Vec::new();
			let mut by_group: HashMap<GroupId, Vec<(JoinSide, JoinDueEntry)>> = HashMap::new();
			for entry in &page.due {
				let side = Self::side_of(entry.side)?;
				by_group.entry(entry.group)
					.or_insert_with(|| {
						order.push(entry.group);
						Vec::new()
					})
					.push((side, *entry));
			}
			for group in &order {
				let rows = &by_group[group];
				for (_, entry) in rows.iter().filter(|(side, _)| *side == JoinSide::Left) {
					self.free_expired_left_row(host, &state, entry)?;
				}
				if rows.iter().any(|(side, _)| *side == JoinSide::Right) {
					let left_numbers = state.left.row_numbers_in(host, *group)?;
					for (_, entry) in rows.iter().filter(|(side, _)| *side == JoinSide::Right) {
						self.free_expired_right_row(host, &state, entry, &left_numbers)?;
					}
				}
				if seen.insert(*group) {
					emptied.push(*group);
				}
			}
			if !page.more {
				break;
			}
			cursor = page.resume;
		}

		for group in emptied {
			if state.left.holds_rows(host, group)?
				|| state.right.holds_rows(host, group)?
				|| !host.state_range_limited(join_expiry_range(group), Some(1))?.is_empty()
			{
				continue;
			}
			host.clear_join_expiries(group, SEAL_BATCH)?;
			enqueue(host, group)?;
			let drained = drain_group(host, group, &mut StoreReaper, SEAL_BATCH)?;
			stalled |= drained.still_queued;
		}

		self.expiry.invalidate();
		self.resync_timer(host, stalled.then_some(retry))
	}

	pub(crate) fn snapshot_ledger(&self) -> SnapshotLedger {
		SnapshotLedger::new(match self.strategy {
			JoinStrategy::LatestLeft(_) | JoinStrategy::LatestInner(_) => Numbering::LeftRow,
			JoinStrategy::Left(_) | JoinStrategy::Inner(_) => Numbering::Pair,
		})
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
		identity: Identity<'_>,
	) -> Result<Emitted> {
		let left_row_number = left.row_numbers()[left_idx];

		let (row_numbers, fresh, existing) =
			self.identities(host, &[Self::unmatched_left_key(left_row_number)], identity)?;
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
		keys: &[JoinRowMappingKey],
		identity: Identity<'_>,
	) -> Result<(Vec<RowNumber>, Vec<usize>, Vec<usize>)> {
		match identity {
			Identity::Carried(supplied) => {
				let (fresh, existing) = (0..keys.len()).partition(|index| supplied[*index].1);
				Ok((supplied.iter().map(|(number, _)| *number).collect(), fresh, existing))
			}
			Identity::Mint => {
				let minted = host.get_or_create_join_row_numbers(keys)?;
				let (fresh, existing) = (0..keys.len()).partition(|index| minted[*index].1);
				Ok((minted.iter().map(|(number, _)| *number).collect(), fresh, existing))
			}
			Identity::Existing | Identity::Consume => {
				let resolved = host.get_join_row_numbers(keys)?;
				let existing: Vec<usize> = resolved
					.iter()
					.enumerate()
					.filter_map(|(index, number)| number.map(|_| index))
					.collect();
				if identity == Identity::Consume {
					let consumed: Vec<JoinRowMappingKey> =
						existing.iter().map(|index| keys[*index]).collect();
					host.remove_join_row_numbers(&consumed)?;
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
		identity: Identity<'_>,
	) -> Result<Emitted> {
		if left_indices.is_empty() {
			return Ok(Emitted::empty());
		}

		let composite_keys: Vec<JoinRowMappingKey> =
			left_indices.iter().map(|&idx| Self::unmatched_left_key(left.row_numbers()[idx])).collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		let built = builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn cleanup_left_row_joins(&self, host: &mut dyn HostContext, left_number: u64) -> Result<()> {
		match self.strategy {
			JoinStrategy::LatestLeft(_) | JoinStrategy::LatestInner(_) => return Ok(()),
			JoinStrategy::Left(_) | JoinStrategy::Inner(_) => {}
		}

		host.remove_join_row_numbers_for_left(JOIN_MAPPING_TAG, left_number)
	}

	fn make_composite_key(left_num: RowNumber, right_num: RowNumber) -> JoinRowMappingKey {
		JoinRowMappingKey {
			tag: Asc(JOIN_MAPPING_TAG),
			left: Desc(left_num.0),
			right: Desc(right_num.0),
		}
	}

	fn unmatched_left_key(left_num: RowNumber) -> JoinRowMappingKey {
		JoinRowMappingKey {
			tag: Asc(JOIN_MAPPING_TAG),
			left: Desc(left_num.0),
			right: Desc(UNMATCHED_RIGHT),
		}
	}

	pub(crate) fn join_columns_one_to_many(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		identity: Identity<'_>,
	) -> Result<Emitted> {
		let right_count = right.row_count();
		if right_count == 0 {
			return Ok(Emitted::empty());
		}

		let left_row_number = left.row_numbers()[left_idx];

		let composite_keys: Vec<JoinRowMappingKey> = (0..right_count)
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
		identity: Identity<'_>,
	) -> Result<Emitted> {
		let left_count = left.row_count();
		if left_count == 0 {
			return Ok(Emitted::empty());
		}

		let right_row_number = right.row_numbers()[right_idx];

		let composite_keys: Vec<JoinRowMappingKey> = (0..left_count)
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
		identity: Identity<'_>,
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

	pub(crate) fn pick(&self) -> &JoinPick {
		self.pick.as_ref().expect("a latest strategy runs only when the join carries a pick")
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
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_join_insert(host, &post, side, &mut state, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_join_remove(host, &pre, side, &mut state, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_join_update(host, &pre, &post, side, &mut state, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.operator, version, result, change.changed_at))
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		if timer.kind == TimerKind::Maintenance {
			self.free_due_join_rows(host, FiredAt::of(&timer))?;
		}
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		self.widest_retention()
	}
}

impl JoinOperator {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_join_insert(
		&mut self,
		host: &mut dyn HostContext,
		post: &Columns,
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(post, self.compiled_exprs_of(side))?;

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
		&mut self,
		host: &mut dyn HostContext,
		pre: &Columns,
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(pre, self.compiled_exprs_of(side))?;

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
		&mut self,
		host: &mut dyn HostContext,
		pre: &Columns,
		post: &Columns,
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let pre_keys = self.compute_join_keys(pre, self.compiled_exprs_of(side))?;
		let post_keys = self.compute_join_keys(post, self.compiled_exprs_of(side))?;
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
			self.move_row_join_expiry(
				host,
				side,
				pre,
				post,
				row_idx,
				(pre_keys[row_idx], post_keys[row_idx]),
			)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod seal_tests {
	use std::ops::Bound;

	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::{
		key::encoded::EncodedKeyRange,
		row::{bytes::EncodedBytes, operator::state::decode},
	};
	use reifydb_core::{
		actors::pending::PendingLayers,
		common::CommitVersion,
		interface::store::MultiVersionRow,
		key::{
			EncodableKey,
			operator::{
				keyspace::join::{JoinLeft, JoinRight, JoinRowExpiryState as JoinRowExpiry},
				state::{KeyspaceId, OperatorStateKey, keyspace_inner_range, node_prefix},
				traits::Keyspace,
			},
		},
		value::column::buffer::ColumnBuffer,
	};
	use reifydb_rql::expression::parse_expression;
	use reifydb_runtime::context::clock::Clock;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		accumulator::ChangeAccumulator,
		multi::{RangeScope, transaction::read::MultiReadTransaction},
	};
	use reifydb_value::{factory::time::at_millis, fragment::Fragment};

	use super::*;
	use crate::{
		operator::{host::TxnHostContext, sink::DurableSink},
		timer::{TimerDue, extension::TimerExtension},
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			join_expiry::{join_due_range, join_expiry_key},
			mock::FlowTxn,
			row_number::RowNumberExtension,
			state::{StateExtension, StateRange},
			substrate::{FlowSubstrate, apply_operator_state},
		},
	};

	fn join(operator: u64, left_retention: Option<Duration>, right_retention: Option<Duration>) -> JoinOperator {
		join_with(operator, false, None, left_retention, right_retention)
	}

	fn join_with(
		operator: u64,
		snapshot: bool,
		pick: Option<JoinPick>,
		left_retention: Option<Duration>,
		right_retention: Option<Duration>,
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
			pick,
			left_retention,
			right_retention,
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

	fn insert(op: &mut JoinOperator, txn: &mut DeferredTransaction, side: JoinSide, post: &Columns) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		let operator = op.operator;
		op.apply_join_insert(
			&mut TxnHostContext::new(txn, operator),
			post,
			side,
			&mut state,
			&mut result,
		)
		.unwrap();
		result
	}

	fn remove(op: &mut JoinOperator, txn: &mut DeferredTransaction, side: JoinSide, pre: &Columns) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		let operator = op.operator;
		op.apply_join_remove(
			&mut TxnHostContext::new(txn, operator),
			pre,
			side,
			&mut state,
			&mut result,
		)
		.unwrap();
		result
	}

	fn update(
		op: &mut JoinOperator,
		txn: &mut DeferredTransaction,
		side: JoinSide,
		pre: &Columns,
		post: &Columns,
	) -> Vec<Diff> {
		let mut state = JoinState::new();
		let mut result = Vec::new();
		let operator = op.operator;
		op.apply_join_update(
			&mut TxnHostContext::new(txn, operator),
			pre,
			post,
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

	fn group_of(hash: &Hash128) -> GroupId {
		GroupId::hashed(*hash)
	}

	fn join_expiry_of(
		op: &JoinOperator,
		txn: &mut DeferredTransaction,
		group: GroupId,
		side: JoinSide,
		row_number: u64,
	) -> Option<DateTime> {
		let key = join_expiry_key(group, side.tag(), RowNumber(row_number));
		txn.state_get(op.operator, &key).unwrap().map(|row| decode::<JoinRowExpiry>(&row).unwrap().at)
	}

	fn due_index_rows(op: &JoinOperator, txn: &mut DeferredTransaction) -> usize {
		// The due index is root scoped, so nothing in a group's own range can prove it was cleaned up.
		txn.state_range(op.operator, StateRange::forward(join_due_range(), "test")).unwrap().items.len()
	}

	fn armed_timers(op: &JoinOperator, txn: &mut DeferredTransaction) -> usize {
		txn.state_range(
			op.operator,
			StateRange::forward(keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.len()
	}

	fn group_rows(op: &JoinOperator, txn: &mut DeferredTransaction, group: GroupId) -> usize {
		// no single range spans a group's keyspaces, so the group filter has to happen after the scan
		txn.state_scan_all(op.operator)
			.unwrap()
			.items
			.iter()
			.filter(|item| OperatorStateKey::decode(&item.key).is_some_and(|key| key.group == group))
			.count()
	}

	fn side_rows(op: &JoinOperator, txn: &mut DeferredTransaction, group: GroupId, side: JoinSide) -> usize {
		// Names the keyspace through the typed constants so a renumbered side fails here, not silently.
		let keyspace = match side {
			JoinSide::Left => JoinLeft::ID,
			JoinSide::Right => JoinRight::ID,
		};
		txn.state_range(op.operator, StateRange::forward(keyspace_inner_range(group, keyspace), "test"))
			.unwrap()
			.items
			.len()
	}

	fn ledger_rows(
		op: &JoinOperator,
		txn: &mut DeferredTransaction,
		group: GroupId,
		keyspace: KeyspaceId,
	) -> usize {
		txn.state_range(op.operator, StateRange::forward(keyspace_inner_range(group, keyspace), "test"))
			.unwrap()
			.items
			.len()
	}

	fn unmatched_mapping(op: &JoinOperator, txn: &mut DeferredTransaction, left: u64) -> Option<RowNumber> {
		txn.get_join_row_numbers(op.operator, &[JoinOperator::unmatched_left_key(RowNumber(left))])
			.unwrap()
			.remove(0)
	}

	fn fire(op: &mut JoinOperator, txn: &mut DeferredTransaction, due: DateTime) -> Option<Change> {
		// The engine lifts a due timer off the wheel before dispatch, so skipping the disarm reads as a leak.
		let operator = op.operator;
		let timer = Timer {
			due,
			kind: TimerKind::Maintenance,
			key: JoinOperator::timer_key(),
		};
		txn.disarm_timer(operator, &timer).unwrap();
		op.on_timer(&mut TxnHostContext::new(txn, operator), timer).unwrap()
	}

	fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
		// State only reaches the store through the batch, so a durable read must go through a commit first.
		apply_operator_state(&engine.inner().operator_state(), &txn.take_pending());
	}

	struct CountingTxn {
		inner: DeferredTransaction,
		scan_starts: Vec<EncodedKey>,
	}

	impl CountingTxn {
		fn scans_from(&self, start: &EncodedKey) -> usize {
			self.scan_starts.iter().filter(|recorded| recorded.as_slice() == start.as_slice()).count()
		}
	}

	impl FlowTransaction for CountingTxn {
		fn version(&self) -> CommitVersion {
			self.inner.version()
		}

		fn clock(&self) -> &Clock {
			self.inner.clock()
		}

		fn catalog(&self) -> &Catalog {
			self.inner.catalog()
		}

		fn query(&self) -> MultiReadTransaction {
			self.inner.query()
		}

		fn substrate(&self) -> &FlowSubstrate {
			self.inner.substrate()
		}

		fn pending_layers(&self) -> &PendingLayers {
			self.inner.pending_layers()
		}

		fn pending_layers_mut(&mut self) -> &mut PendingLayers {
			self.inner.pending_layers_mut()
		}

		fn accumulator_mut(&mut self) -> &mut ChangeAccumulator {
			self.inner.accumulator_mut()
		}

		fn armed_mut(&mut self) -> &mut Vec<TimerDue> {
			self.inner.armed_mut()
		}

		fn change_coordinate(&self) -> Option<ChangeCoordinate> {
			self.inner.change_coordinate()
		}

		fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
			self.inner.set_change_coordinate(coordinate)
		}

		fn flow_watermark(&self) -> Option<DateTime> {
			self.inner.flow_watermark()
		}

		fn set_flow_watermark(&mut self, watermark: DateTime) {
			self.inner.set_flow_watermark(watermark)
		}

		fn run_durable_sink(&mut self, sink: &mut dyn DurableSink, change: Change) -> Result<Change> {
			self.inner.run_durable_sink(sink, change)
		}

		fn run_durable_sink_timer(
			&mut self,
			sink: &mut dyn DurableSink,
			timer: Timer,
		) -> Result<Option<Change>> {
			self.inner.run_durable_sink_timer(sink, timer)
		}

		fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
			self.inner.storage_get(key)
		}

		fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool> {
			self.inner.storage_contains(key)
		}

		fn storage_range(
			&mut self,
			range: EncodedKeyRange,
			scope: RangeScope,
			batch_size: usize,
		) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
			// a repeated enumeration of one keyspace must show up here as a repeated start bound
			match &range.start {
				Bound::Included(start) | Bound::Excluded(start) => self.scan_starts.push(start.clone()),
				Bound::Unbounded => {}
			}
			self.inner.storage_range(range, scope, batch_size)
		}

		fn fetch_state_external(
			&mut self,
			keys: Vec<EncodedKey>,
			items: &mut Vec<MultiVersionRow>,
		) -> Result<()> {
			self.inner.fetch_state_external(keys, items)
		}
	}

	fn left_scan_start(operator: OperatorId, group: GroupId) -> EncodedKey {
		// unprefixed this start matches no range the operator actually issues, so the count would read zero
		let range =
			keyspace_inner_range(group, JoinLeft::ID).with_prefix(EncodedKey::new(node_prefix(operator)));
		match range.start {
			Bound::Included(key) | Bound::Excluded(key) => key,
			Bound::Unbounded => panic!("a keyspace range must be bounded below"),
		}
	}

	fn left_scans_freeing_expired_rights(operator: u64, rights: usize) -> usize {
		// one key, six left rows that outlive the fire, and `rights` right rows all due at the same instant
		let engine = TestEngine::new();
		let mut op = join(operator, Some(seconds(3_600)), Some(seconds(10)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7; 6], &[1, 2, 3, 4, 5, 6], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let numbers: Vec<u64> = (0..rights as u64).map(|offset| 100 + offset).collect();
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&vec![7; rights], &numbers, at_millis(9_000)));
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		let start = left_scan_start(op.operator, group);

		let mut counting = CountingTxn {
			inner: txn,
			scan_starts: Vec::new(),
		};
		let timer = Timer {
			due: at_millis(19_001),
			kind: TimerKind::Maintenance,
			key: JoinOperator::timer_key(),
		};
		counting.disarm_timer(op.operator, &timer).unwrap();
		let operator = op.operator;
		op.on_timer(&mut TxnHostContext::new(&mut counting, operator), timer).unwrap();

		counting.scans_from(&start)
	}

	#[test]
	fn an_inserted_row_is_armed_one_retention_past_its_own_event_time() {
		// the join expiry must be the row's own event time, not wall-clock, or a backfilled row evicts on
		// arrival
		let engine = TestEngine::new();
		let mut op = join(1, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));

		insert(&mut op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the due time is event time + retention + the strict gate step"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and exactly one maintenance timer covers the operator");
	}

	#[test]
	fn an_update_moves_the_rows_timer_rather_than_adding_a_second() {
		// Without cancelling the old arming the row is addressed twice and the stale one fires while it lives.
		let engine = TestEngine::new();
		let mut op = join(2, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let pre = rows(&[7], &[42], at_millis(5_000));
		let post = rows(&[7], &[42], at_millis(20_000));
		insert(&mut op, &mut txn, JoinSide::Left, &pre);

		update(&mut op, &mut txn, JoinSide::Left, &pre, &post);

		let group = group_of(&hash_of(&op, JoinSide::Left, &post, 0));
		assert_eq!(armed_timers(&op, &mut txn), 1, "an update re-arms one timer, it does not add one");
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(30_001)),
			"and the due time follows the row's own last write"
		);
	}

	#[test]
	fn a_row_still_inside_its_retention_survives_a_maintenance_tick() {
		// The gate is strict: a row whose due time lands one tick past the fire must not expire yet.
		let engine = TestEngine::new();
		let mut op = join(3, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));

		fire(&mut op, &mut txn, at_millis(15_000));

		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 1, "the row must keep its state");
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"and its join expiry must survive untouched"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "its timer is not due and must be armed again");
	}

	#[test]
	fn removing_a_row_takes_its_join_expiry_and_its_timer_with_it() {
		// A source delete leaves no row to expire, so a join expiry left behind fires against state that is
		// gone.
		let engine = TestEngine::new();
		let mut op = join(4, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));

		remove(&mut op, &mut txn, JoinSide::Left, &left);

		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			None,
			"the join expiry must go with the row"
		);
		assert_eq!(armed_timers(&op, &mut txn), 0, "and the timer that addressed it must be disarmed");
	}

	#[test]
	fn a_sibling_row_under_the_same_key_outlives_its_neighbours_expiry() {
		// A join group holds many rows, so freeing due rows must free exactly one and re-arm on the next
		// earliest.
		let engine = TestEngine::new();
		let mut op = join(5, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let early = rows(&[7], &[1], at_millis(5_000));
		let late = rows(&[7], &[2], at_millis(50_000));
		insert(&mut op, &mut txn, JoinSide::Left, &early);
		insert(&mut op, &mut txn, JoinSide::Left, &late);
		let group = group_of(&hash_of(&op, JoinSide::Left, &early, 0));

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 1),
			None,
			"the due row's join expiry must go"
		);
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 2),
			Some(at_millis(60_001)),
			"its neighbour under the same key must keep its own join expiry"
		);
		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Left), 1, "exactly one row was freed");
		assert_eq!(armed_timers(&op, &mut txn), 1, "and the operator re-arms on the next earliest join expiry");
	}

	#[test]
	fn right_side_traffic_never_extends_a_left_rows_join_expiry() {
		// A match is not a write to the left row; advancing its join expiry would keep a joined row alive
		// forever.
		let engine = TestEngine::new();
		let mut op = join(6, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));

		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left join expiry must still name the left row's own last write"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and a side with no retention must arm nothing of its own");
	}

	#[test]
	fn a_join_without_a_retention_arms_nothing_at_all() {
		// Arming without a retention leaves one timer and one join expiry per row that nothing ever collects.
		let engine = TestEngine::new();
		let mut op = join(7, None, None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));

		insert(&mut op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			None,
			"no retention means no join expiry"
		);
		assert_eq!(armed_timers(&op, &mut txn), 0, "and no timer");
	}

	#[test]
	fn an_expired_row_frees_its_state_its_join_expiry_and_its_output_mapping_without_emitting() {
		// An expired row is frozen downstream, so it must emit nothing while its mapping is reclaimed.
		let engine = TestEngine::new();
		let mut op = join(8, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert!(unmatched_mapping(&op, &mut txn, 42).is_some(), "precondition: the row published downstream");

		let emitted = fire(&mut op, &mut txn, at_millis(15_001));

		assert!(emitted.is_none(), "freeing an expired row must publish no diff at all");
		assert_eq!(join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42), None, "the join expiry must go");
		assert_eq!(
			unmatched_mapping(&op, &mut txn, 42),
			None,
			"and so must the output row number, or one leaks per emitted row forever"
		);
	}

	#[test]
	fn a_key_whose_last_row_expired_loses_the_group_that_carried_it() {
		// a group's rows only leave through the reaper, so freeing the last row must queue it
		let engine = TestEngine::new();
		let mut op = join(9, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&hash);

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(group_rows(&op, &mut txn, group), 0, "the group's range must be left empty");
		assert_eq!(armed_timers(&op, &mut txn), 0, "and the timer that drove the expiry must not re-arm");
	}

	#[test]
	fn a_group_is_only_reaped_once_both_sides_hold_nothing() {
		// Both sides share one group, so queueing on the first empty side reaps rows the other still reads.
		let engine = TestEngine::new();
		let mut op = join(10, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&hash);

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(
			side_rows(&op, &mut txn, group, JoinSide::Left),
			0,
			"the left row is past its own retention"
		);
		assert_eq!(
			side_rows(&op, &mut txn, group, JoinSide::Right),
			1,
			"an unexpired side keeps its rows and no join expiry holds the group open on its behalf"
		);
	}

	#[test]
	fn a_row_short_of_its_own_retention_holds_the_groups_timer_open() {
		// The two sides expire on independent spans, so the group re-arms on whichever join expiry is next due.
		let engine = TestEngine::new();
		let mut op = join(11, Some(seconds(10)), Some(seconds(3_600)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		let hash = hash_of(&op, JoinSide::Left, &left, 0);
		let group = group_of(&hash);

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(
			side_rows(&op, &mut txn, group, JoinSide::Left),
			0,
			"the left row is past its own retention"
		);
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Right, 99),
			Some(at_millis(3_609_001)),
			"the right row keeps the join expiry its own longer retention gave it"
		);
		assert_eq!(
			armed_timers(&op, &mut txn),
			1,
			"and the operator re-arms on that join expiry rather than dropping it"
		);
	}

	#[test]
	fn a_latest_join_expires_both_sides_on_their_own_retentions() {
		// A latest join keeps every right row per key, so a right retention must arm or that growth is
		// unbounded.
		let engine = TestEngine::new();
		let mut op = join_with(12, false, Some(JoinPick::latest()), Some(seconds(10)), Some(seconds(10)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		insert(&mut op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left side is an ordinary row set and must expire normally"
		);
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Right, 99),
			Some(at_millis(19_001)),
			"the right row is an ordinary kept row and must expire on its own retention"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and one timer covers the group's earliest join expiry");
	}

	#[test]
	fn a_snapshot_join_expires_both_sides_on_their_own_retentions() {
		// A sealed right row retires its bytes into the pin, so pinning it no longer means it may never expire.
		let engine = TestEngine::new();
		let mut op = join_with(13, true, None, Some(seconds(10)), Some(seconds(10)));
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));

		insert(&mut op, &mut txn, JoinSide::Left, &left);

		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"the left side is unaffected by the flag and must expire normally"
		);
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Right, 99),
			Some(at_millis(19_001)),
			"and the pinned right row must arm too, or a snapshot join grows without bound"
		);
		assert_eq!(armed_timers(&op, &mut txn), 1, "and one timer covers the group's earliest join expiry");
	}

	#[test]
	fn an_expired_left_row_releases_the_snapshot_ledger_it_held() {
		// A pin is refcounted, so a left row that expires without releasing holds its retired copy forever.
		let engine = TestEngine::new();
		let mut op = join_with(14, true, None, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PUBLISHED),
			1,
			"precondition: it published"
		);
		assert_eq!(ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PIN), 1, "precondition: it pinned");

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PUBLISHED),
			0,
			"the published pair must go with the left row that owned it"
		);
		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PIN),
			0,
			"and its last reference must take the pin with it"
		);
		assert_eq!(side_rows(&op, &mut txn, group, JoinSide::Right), 1, "while the right row it read survives");
	}

	#[test]
	fn a_siblings_pin_on_the_same_right_row_survives_its_neighbours_expiry() {
		// Release must be scoped to the expiring left row, or a live sibling loses the version it published.
		let engine = TestEngine::new();
		let mut op = join_with(15, true, None, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let early = rows(&[7], &[1], at_millis(5_000));
		let late = rows(&[7], &[2], at_millis(50_000));
		insert(&mut op, &mut txn, JoinSide::Right, &rows(&[7], &[99], at_millis(9_000)));
		insert(&mut op, &mut txn, JoinSide::Left, &early);
		insert(&mut op, &mut txn, JoinSide::Left, &late);
		let group = group_of(&hash_of(&op, JoinSide::Left, &early, 0));
		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PUBLISHED),
			2,
			"precondition: both published"
		);

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PUBLISHED),
			1,
			"exactly the expired row's pair goes, never its neighbour's"
		);
		assert_eq!(
			ledger_rows(&op, &mut txn, group, KeyspaceId::JOIN_PIN),
			1,
			"and the pin the sibling still references must stay"
		);
	}

	#[test]
	fn a_committed_join_expiry_leaves_neither_of_its_two_rows_behind_once_its_group_expires() {
		// A sweep visits the group's own partition, so the root due row must never outlive the group it names.
		let engine = TestEngine::new();
		let mut op = join(16, Some(seconds(10)), None);
		let mut txn = txn_at(&engine, 100);
		let left = rows(&[7], &[42], at_millis(5_000));
		insert(&mut op, &mut txn, JoinSide::Left, &left);
		let group = group_of(&hash_of(&op, JoinSide::Left, &left, 0));
		commit(&engine, &mut txn);
		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			Some(at_millis(15_001)),
			"precondition: the group scoped join expiry is durable, not merely in the batch"
		);
		assert_eq!(due_index_rows(&op, &mut txn), 1, "precondition: so is the root due row indexing it");

		fire(&mut op, &mut txn, at_millis(15_001));
		commit(&engine, &mut txn);

		assert_eq!(
			join_expiry_of(&op, &mut txn, group, JoinSide::Left, 42),
			None,
			"a group driven through free, enqueue and drain must leave no join expiry row"
		);
		assert_eq!(due_index_rows(&op, &mut txn), 0, "and no root due row pointing back into it");
	}

	#[test]
	fn expiring_right_rows_enumerate_their_groups_left_side_once_not_once_each() {
		// the left side never changes under the right sweep, so re-reading it per right row is a wasted scan
		let one = left_scans_freeing_expired_rights(20, 1);
		let four = left_scans_freeing_expired_rights(21, 4);

		assert_eq!(
			one, 2,
			"exactly two left reads: the composite key enumeration and the holds_rows veto on the drain"
		);
		assert_eq!(four, one, "four expiring rights must not cost four enumerations of the same left side");
	}
}
