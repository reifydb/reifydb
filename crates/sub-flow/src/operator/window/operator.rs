// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{ops, sync::LazyLock, time::Duration};

use postcard::to_stdvec;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::{CommitVersion, WindowKind, WindowSize},
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		shape::RowShape,
	},
	error::diagnostic::flow::{flow_window_timestamp_column_not_found, flow_window_timestamp_column_type_mismatch},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal,
	row::Row,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{
	Expression,
	name::{collect_all_column_names, display_label},
};
use reifydb_runtime::{
	context::RuntimeContext,
	hash::{Hash128, xxh3_128},
};
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	error::Error,
	params::Params,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};

use super::{
	aggregate::{FastAgg, detect_fast_agg},
	rolling::apply_rolling_window,
	session::apply_session_window,
	sliding::apply_sliding_window,
	state::WindowEvent,
	tumbling::apply_tumbling_window,
};
use crate::{
	operator::{
		Operator, OperatorCell,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, window::WindowStateful},
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;

static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub struct WindowConfig {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
}

pub struct WindowOperator {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub compiled_aggregations: Vec<CompiledExpr>,
	pub layout: RowShape,
	pub routines: Routines,
	pub row_number_provider: RowNumberProvider,
	pub runtime_context: RuntimeContext,

	pub projected_columns: Vec<String>,

	pub fast_aggregations: Option<Vec<FastAgg>>,
	pub agg_output_names: Vec<String>,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Self {
		let symbols = SymbolTable::new();
		let compile_ctx = CompileContext {
			symbols: &symbols,
		};

		let compiled_group_by: Vec<CompiledExpr> = config
			.group_by
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile group_by expression"))
			.collect();

		let compiled_aggregations: Vec<CompiledExpr> = config
			.aggregations
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile aggregation expression"))
			.collect();

		let mut needed = collect_all_column_names(&config.group_by);
		needed.extend(collect_all_column_names(&config.aggregations));
		let mut projected_columns: Vec<String> = needed.into_iter().collect();
		projected_columns.sort();

		let lagged_rolling = matches!(
			&config.kind,
			WindowKind::Rolling {
				lag: Some(_),
				..
			}
		);
		let detected: Vec<Option<FastAgg>> =
			config.aggregations.iter().map(|e| detect_fast_agg(&config.routines, e)).collect();
		let fast_aggregations =
			if !lagged_rolling && !detected.is_empty() && detected.iter().all(Option::is_some) {
				Some(detected.into_iter().map(Option::unwrap).collect())
			} else {
				None
			};
		let agg_output_names: Vec<String> =
			config.aggregations.iter().map(|e| display_label(e).text().to_string()).collect();

		Self {
			parent: config.parent,
			node: config.node,
			kind: config.kind,
			group_by: config.group_by,
			aggregations: config.aggregations,
			ts: config.ts,
			compiled_group_by,
			compiled_aggregations,
			layout: RowShape::operator_state(),
			routines: config.routines,
			row_number_provider: RowNumberProvider::new(config.node),
			runtime_context: config.runtime_context,
			projected_columns,
			fast_aggregations,
			agg_output_names,
		}
	}

	pub fn current_timestamp(&self) -> u64 {
		self.runtime_context.clock.now_millis()
	}

	pub fn project_columns(&self, columns: &Columns) -> Columns {
		if self.projected_columns.is_empty() {
			return columns.clone();
		}
		columns.project_by_names(&self.projected_columns)
	}

	pub fn is_count_based(&self) -> bool {
		self.kind.size().is_some_and(|m| m.is_count())
	}

	pub fn is_rolling(&self) -> bool {
		matches!(self.kind, WindowKind::Rolling { .. })
	}

	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	pub fn size_count(&self) -> Option<u64> {
		self.kind.size().and_then(|m| m.as_count())
	}

	pub(super) fn eval_session(&self, is_aggregate: bool) -> EvalContext<'_> {
		EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: is_aggregate,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		}
	}

	pub fn compute_group_keys(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_group_by.is_empty() {
			return Ok(vec![Hash128::from(0u128); row_count]);
		}

		let session = self.eval_session(false);
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut group_columns: Vec<ColumnWithName> = Vec::new();
		for compiled_expr in &self.compiled_group_by {
			let col = compiled_expr.execute(&exec_ctx)?;
			group_columns.push(col);
		}

		let mut hashes = Vec::with_capacity(row_count);
		let mut buf = Vec::with_capacity(128);
		for row_idx in 0..row_count {
			buf.clear();
			for col in &group_columns {
				let value = col.data().get_value(row_idx);
				let bytes = to_stdvec(&value).map_err(|e| {
					Error(Box::new(internal!("Failed to encode group-by value: {}", e)))
				})?;
				buf.extend_from_slice(&bytes);
			}
			hashes.push(xxh3_128(&buf));
		}

		Ok(hashes)
	}

	pub fn resolve_event_timestamps(&self, columns: &Columns, row_count: usize) -> Result<Vec<u64>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		match &self.ts {
			Some(ts_col) => {
				let col = columns.column(ts_col).ok_or_else(|| {
					Error(Box::new(flow_window_timestamp_column_not_found(ts_col)))
				})?;
				let mut timestamps = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match col.data().get_value(i) {
						Value::DateTime(dt) => timestamps.push(dt.timestamp_millis() as u64),
						other => {
							return Err(Error(Box::new(
								flow_window_timestamp_column_type_mismatch(
									ts_col,
									other.get_type(),
								),
							)));
						}
					}
				}
				Ok(timestamps)
			}
			None => {
				let now = self.current_timestamp();
				Ok(vec![now; row_count])
			}
		}
	}

	pub(super) fn replace_event_in_windows(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		post_row: &Row,
		post_timestamp: u64,
	) -> Result<Vec<Diff>> {
		let window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if window_ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		for window_id in &window_ids {
			let window_key = self.create_window_key(group_hash, *window_id);
			let mut window_state = self.load_window_state(txn, &window_key)?;

			let event_idx = window_state.events.iter().position(|e| e.row_number == row_number);
			if let Some(idx) = event_idx {
				let layout = match &window_state.window_layout {
					Some(l) => l.clone(),
					None => continue,
				};

				let changed_at = DateTime::from_nanos(post_timestamp);
				let pre_aggregation = self.apply_aggregations(
					txn,
					&window_key,
					&layout,
					&window_state.events,
					changed_at,
					&window_state,
				)?;

				let pre_event_row = window_state.events[idx].to_row(&layout);
				window_state.events[idx] = WindowEvent::from_row(post_row, post_timestamp);
				self.update_running_totals_on_evict(&mut window_state, &pre_event_row);
				self.update_running_totals_on_push(&mut window_state, post_row);

				let post_aggregation = self.apply_aggregations(
					txn,
					&window_key,
					&layout,
					&window_state.events,
					changed_at,
					&window_state,
				)?;

				self.save_window_state(txn, &window_key, &window_state)?;

				if let (Some((pre_row, _)), Some((post_row, _))) = (pre_aggregation, post_aggregation) {
					result.push(Diff::update(
						Columns::from_row(&pre_row),
						Columns::from_row(&post_row),
					));
				}
			}
		}

		Ok(result)
	}

	pub(super) fn process_event_updates(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
	) -> Result<Vec<Diff>> {
		let row_count = pre.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let group_hashes = self.compute_group_keys(pre)?;
		let post_timestamps = self.resolve_event_timestamps(post, row_count)?;
		let mut result = Vec::new();

		for row_idx in 0..row_count {
			let row_number = pre.row_numbers[row_idx];
			let group_hash = group_hashes[row_idx];
			let post_timestamp = post_timestamps[row_idx];

			let single_row = post.extract_row(row_idx);
			let projected = self.project_columns(&single_row);
			let post_row = projected.to_single_row();

			let diffs =
				self.replace_event_in_windows(txn, group_hash, row_number, &post_row, post_timestamp)?;
			result.extend(diffs);
		}

		Ok(result)
	}

	pub(super) fn remove_event_from_windows(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<Diff>> {
		let window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if window_ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();
		for window_id in &window_ids {
			let window_key = self.create_window_key(group_hash, *window_id);
			if let Some(diff) = self.remove_event_from_one_window(txn, &window_key, row_number)? {
				result.push(diff);
			}
		}

		if !self.is_rolling() {
			let index_key = self.create_row_index_key(group_hash, row_number);
			let empty = self.layout.allocate();
			self.save_state(txn, &index_key, empty)?;
		}

		Ok(result)
	}

	#[inline]
	pub(super) fn remove_event_from_one_window(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		row_number: RowNumber,
	) -> Result<Option<Diff>> {
		let mut window_state = self.load_window_state(txn, window_key)?;
		let Some(idx) = window_state.events.iter().position(|e| e.row_number == row_number) else {
			return Ok(None);
		};
		let Some(layout) = window_state.window_layout.clone() else {
			return Ok(None);
		};

		let changed_at = DateTime::from_nanos(txn.clock().now_nanos());
		let pre_aggregation = self.apply_aggregations(
			txn,
			window_key,
			&layout,
			&window_state.events,
			changed_at,
			&window_state,
		)?;

		let evicted_row = window_state.events[idx].to_row(&layout);
		window_state.events.remove(idx);
		window_state.event_count = window_state.event_count.saturating_sub(1);
		self.update_running_totals_on_evict(&mut window_state, &evicted_row);

		if window_state.events.is_empty() {
			self.save_window_state(txn, window_key, &window_state)?;
			return Ok(pre_aggregation.map(|(pre_row, _)| Diff::remove(Columns::from_row(&pre_row))));
		}

		let post_aggregation = self.apply_aggregations(
			txn,
			window_key,
			&layout,
			&window_state.events,
			changed_at,
			&window_state,
		)?;
		self.save_window_state(txn, window_key, &window_state)?;

		Ok(match (pre_aggregation, post_aggregation) {
			(Some((pre_row, _)), Some((post_row, _))) => {
				Some(Diff::update(Columns::from_row(&pre_row), Columns::from_row(&post_row)))
			}
			_ => None,
		})
	}

	pub(super) fn process_event_removals(&self, txn: &mut FlowTransaction, pre: &Columns) -> Result<Vec<Diff>> {
		let row_count = pre.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let group_hashes = self.compute_group_keys(pre)?;
		let mut result = Vec::new();

		for (&row_number, &group_hash) in pre.row_numbers.iter().zip(group_hashes.iter()) {
			let diffs = self.remove_event_from_windows(txn, group_hash, row_number)?;
			result.extend(diffs);
		}

		Ok(result)
	}

	pub fn process_expired_windows(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();

		if let Some(duration) = self.size_duration() {
			let window_size_ms = duration.as_millis() as u64;
			if window_size_ms > 0 {
				let expire_before = current_timestamp.saturating_sub(window_size_ms * 2);
				let cutoff_id = expire_before / window_size_ms;
				if cutoff_id == 0 {
					return Ok(result);
				}

				let groups = self.load_group_registry(txn)?;
				for group_hash in &groups {
					let low_key = self.create_window_key(*group_hash, cutoff_id);
					let high_key = self.create_window_key(*group_hash, 0);
					let range = EncodedKeyRange::new(
						ops::Bound::Excluded(low_key),
						ops::Bound::Included(high_key),
					);

					let expired_keys = self.scan_keys_in_range(txn, &range)?;
					let changed_at = DateTime::from_nanos(current_timestamp);
					for key in &expired_keys {
						let window_state = self.load_window_state(txn, key)?;
						if !window_state.events.is_empty()
							&& let Some(layout) = &window_state.window_layout && let Some((
							row,
							_,
						)) = self
							.apply_aggregations(
								txn,
								key,
								layout,
								&window_state.events,
								changed_at,
								&window_state,
							)? {
							result.push(Diff::remove(Columns::from_row(&row)));
						}
					}

					if !expired_keys.is_empty() {
						let low_key = self.create_window_key(*group_hash, cutoff_id);
						let high_key = self.create_window_key(*group_hash, 0);
						let range = EncodedKeyRange::new(
							ops::Bound::Excluded(low_key),
							ops::Bound::Included(high_key),
						);
						let _ = self.expire_range(txn, range)?;
					}
				}
			}
		}

		Ok(result)
	}

	pub fn tick_expire_windows(&self, txn: &mut FlowTransaction, current_timestamp: u64) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let window_size_ms = match self.size_duration() {
			Some(d) => d.as_millis() as u64,
			None => return Ok(result),
		};
		if window_size_ms == 0 {
			return Ok(result);
		}

		let mut keys_to_remove = Vec::new();
		for window_key in self.scan_window_keys(txn)? {
			if let Some(diff) =
				self.expire_window_if_due(txn, &window_key, current_timestamp, window_size_ms)?
			{
				result.push(diff);
				keys_to_remove.push(window_key);
			}
		}

		for key in &keys_to_remove {
			let empty = self.create_state();
			self.save_state(txn, key, empty)?;
		}

		Ok(result)
	}

	#[inline]
	pub(super) fn expire_window_if_due(
		&self,
		txn: &mut FlowTransaction,
		window_key: &EncodedKey,
		current_timestamp: u64,
		window_size_ms: u64,
	) -> Result<Option<Diff>> {
		let window_state = self.load_window_state(txn, window_key)?;
		if window_state.events.is_empty() {
			return Ok(None);
		}

		let newest_event_time = window_state.events.iter().map(|e| e.timestamp).max().unwrap_or(0);
		if current_timestamp.saturating_sub(newest_event_time) <= window_size_ms {
			return Ok(None);
		}

		let changed_at = DateTime::from_nanos(current_timestamp);
		if let Some(layout) = &window_state.window_layout
			&& let Some((row, _)) = self.apply_aggregations(
				txn,
				window_key,
				layout,
				&window_state.events,
				changed_at,
				&window_state,
			)? {
			return Ok(Some(Diff::remove(Columns::from_row(&row))));
		}
		Ok(None)
	}

	pub fn process_insert(
		&self,
		txn: &mut FlowTransaction,
		columns: &Columns,
		changed_at: DateTime,
		group_fn: impl Fn(&WindowOperator, &mut FlowTransaction, &Columns, Hash128, DateTime) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		let group_hashes = self.compute_group_keys(columns)?;
		let groups = columns.partition_by_keys(&group_hashes);
		let mut result = Vec::new();
		for (group_hash, group_columns) in groups {
			self.register_group(txn, group_hash)?;
			let group_result = group_fn(self, txn, &group_columns, group_hash, changed_at)?;
			result.extend(group_result);
		}
		Ok(result)
	}

	pub fn apply_window_change(
		&self,
		txn: &mut FlowTransaction,
		change: &Change,
		expire: bool,
		process_fn: impl Fn(&WindowOperator, &mut FlowTransaction, &Columns) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		if expire {
			let current_timestamp = self.current_timestamp();
			let expired_diffs = self.process_expired_windows(txn, current_timestamp)?;
			result.extend(expired_diffs);
		}
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => result.extend(process_fn(self, txn, post)?),
				Diff::Update {
					pre,
					post,
					..
				} => result.extend(self.apply_window_update_diff(txn, pre, post, &process_fn)?),
				Diff::Remove {
					pre,
					..
				} => result.extend(self.process_event_removals(txn, pre)?),
			}
		}
		Ok(result)
	}

	#[inline]
	pub(super) fn apply_window_update_diff(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		process_fn: &impl Fn(&WindowOperator, &mut FlowTransaction, &Columns) -> Result<Vec<Diff>>,
	) -> Result<Vec<Diff>> {
		let group_hashes = self.compute_group_keys(pre)?;
		let mut update_indices: Vec<usize> = Vec::new();
		let mut insert_indices: Vec<usize> = Vec::new();
		for (row_idx, &group_hash) in group_hashes.iter().enumerate() {
			let row_number = pre.row_numbers[row_idx];
			if self.lookup_row_index(txn, group_hash, row_number)?.is_empty() {
				insert_indices.push(row_idx);
			} else {
				update_indices.push(row_idx);
			}
		}

		let mut result = Vec::new();
		if !update_indices.is_empty() {
			let pre_subset = pre.extract_by_indices(&update_indices);
			let post_subset = post.extract_by_indices(&update_indices);
			result.extend(self.process_event_updates(txn, &pre_subset, &post_subset)?);
		}
		if !insert_indices.is_empty() {
			let post_subset = post.extract_by_indices(&insert_indices);
			result.extend(process_fn(self, txn, &post_subset)?);
		}
		Ok(result)
	}

	pub fn emit_aggregation_diff(
		aggregated_row: &Row,
		is_new: bool,
		previous_aggregation: Option<(Row, bool)>,
	) -> Diff {
		if is_new {
			Diff::insert(Columns::from_row(aggregated_row))
		} else if let Some((previous_row, _)) = previous_aggregation {
			Diff::update(Columns::from_row(&previous_row), Columns::from_row(aggregated_row))
		} else {
			Diff::insert(Columns::from_row(aggregated_row))
		}
	}
}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> RowShape {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			}
			| WindowKind::Session {
				..
			}
			| WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => Some(Duration::from_secs(1)),
			WindowKind::Rolling {
				size: WindowSize::Count(_),
				..
			} => None,
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_window(self, txn, change),
			WindowKind::Sliding {
				..
			} => apply_sliding_window(self, txn, change),
			WindowKind::Rolling {
				..
			} => apply_rolling_window(self, txn, change),
			WindowKind::Session {
				..
			} => apply_session_window(self, txn, change),
		}
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let current_timestamp = tick.now.to_nanos() / 1_000_000;
		let diffs = match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			} => self.tick_expire_windows(txn, current_timestamp)?,
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => self.tick_rolling_eviction(txn, current_timestamp)?,
			WindowKind::Session {
				..
			} => self.tick_session_expiration(txn, current_timestamp)?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Change::from_flow(
				self.node,
				CommitVersion(0),
				diffs,
				DateTime::from_nanos(self.runtime_context.clock.now_nanos()),
			)))
		}
	}
}
