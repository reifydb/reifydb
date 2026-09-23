// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_row::{RowConverter, Rows};
use reifydb_core::{
	error::diagnostic::query::column_not_found,
	interface::identifier::ColumnObject,
	internal_error,
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_rql::expression::{AccessObjectExpression, Expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	reifydb_assertions,
	value::{row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::common::{
	JoinContext, JoinSlot, NO_MATCH, ensure_join_keyable, eval_join_condition, join_key_types, load_and_merge_all,
	materialize_join, resolve_column_names,
};
use crate::{
	Result,
	vm::volcano::{
		key_rows::{key_arrays, key_rows, key_types},
		query::{QueryContext, QueryNode},
	},
};

pub(crate) struct EquiKeyPair {
	pub left_col_name: String,
	pub right_col_name: String,
	pub left_fragment: Fragment,
	pub right_fragment: Fragment,
}

pub(crate) struct EquiJoinAnalysis {
	pub equi_keys: Vec<EquiKeyPair>,
	pub residual: Vec<Expression>,
}

pub(crate) fn extract_equi_keys(on: &[Expression]) -> EquiJoinAnalysis {
	let mut leaves = Vec::new();
	for expr in on {
		if contains_or(expr) {
			return EquiJoinAnalysis {
				equi_keys: vec![],
				residual: on.to_vec(),
			};
		}
		flatten_and(expr, &mut leaves);
	}

	let mut equi_keys = Vec::new();
	let mut residual = Vec::new();

	for leaf in leaves {
		match try_extract_equi_pair(&leaf) {
			Some(pair) => equi_keys.push(pair),
			None => residual.push(leaf),
		}
	}

	EquiJoinAnalysis {
		equi_keys,
		residual,
	}
}

fn contains_or(expr: &Expression) -> bool {
	match expr {
		Expression::Or(_) => true,
		Expression::And(and) => contains_or(&and.left) || contains_or(&and.right),
		_ => false,
	}
}

fn flatten_and(expr: &Expression, out: &mut Vec<Expression>) {
	match expr {
		Expression::And(and) => {
			flatten_and(&and.left, out);
			flatten_and(&and.right, out);
		}
		other => out.push(other.clone()),
	}
}

fn try_extract_equi_pair(expr: &Expression) -> Option<EquiKeyPair> {
	if let Expression::Equal(eq) = expr {
		if let (Expression::Column(col), Expression::AccessSource(acc)) = (eq.left.as_ref(), eq.right.as_ref())
		{
			return Some(EquiKeyPair {
				left_col_name: col.0.name.text().to_string(),
				right_col_name: acc.column.name.text().to_string(),
				left_fragment: col.0.name.clone(),
				right_fragment: access_fragment(acc),
			});
		}

		if let (Expression::AccessSource(acc), Expression::Column(col)) = (eq.left.as_ref(), eq.right.as_ref())
		{
			return Some(EquiKeyPair {
				left_col_name: col.0.name.text().to_string(),
				right_col_name: acc.column.name.text().to_string(),
				left_fragment: col.0.name.clone(),
				right_fragment: access_fragment(acc),
			});
		}
	}
	None
}

fn access_fragment(acc: &AccessObjectExpression) -> Fragment {
	let source = match &acc.column.object {
		ColumnObject::Qualified {
			name,
			..
		} => name,
		ColumnObject::Alias(alias) => alias,
	};
	Fragment::Statement {
		column: acc.column.name.column(),
		line: acc.column.name.line(),
		text: Arc::from(format!("{}.{}", source.text(), acc.column.name.text())),
	}
}

fn key_index(columns: &Columns, (name, fragment): &(String, Fragment)) -> Result<usize> {
	columns.iter().position(|c| c.name().text() == name).ok_or_else(|| error!(column_not_found(fragment.clone())))
}

#[derive(Clone, Copy, PartialEq)]
enum HashJoinMode {
	Inner,
	Left,
}

type KeyTable = HashMap<Box<[u8]>, Vec<usize>>;

struct HashJoinState {
	build_columns: Columns,
	key_types: Vec<ValueType>,
	converter: Option<RowConverter>,
	hash_table: KeyTable,
	resolved_names: Vec<String>,
	probe_shells: Vec<ColumnBuffer>,
	right_key_indices: Vec<usize>,
	left_key_indices: Vec<usize>,

	probe_batch: Option<Columns>,
	probe_keys: Option<(Rows, Vec<ArrayRef>)>,
	probe_row_idx: usize,
	current_matches: Vec<usize>,
	current_match_idx: usize,
	current_row_matched: bool,
	probe_exhausted: bool,

	compiled_residual: Vec<CompiledExpr>,
}

pub(crate) struct HashJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,

	left_keys: Vec<(String, Fragment)>,
	right_keys: Vec<(String, Fragment)>,
	residual: Vec<Expression>,
	alias: Option<Fragment>,
	mode: HashJoinMode,

	headers: Option<ColumnHeaders>,
	context: JoinContext,

	state: Option<HashJoinState>,
}

impl HashJoinNode {
	pub(crate) fn new_inner(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		analysis: EquiJoinAnalysis,
		alias: Option<Fragment>,
	) -> Self {
		let (left_keys, right_keys) = split_key_names(&analysis.equi_keys);
		Self {
			left,
			right,
			left_keys,
			right_keys,
			residual: analysis.residual,
			alias,
			mode: HashJoinMode::Inner,
			headers: None,
			context: JoinContext::new(),
			state: None,
		}
	}

	pub(crate) fn new_left(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		analysis: EquiJoinAnalysis,
		alias: Option<Fragment>,
	) -> Self {
		let (left_keys, right_keys) = split_key_names(&analysis.equi_keys);
		Self {
			left,
			right,
			left_keys,
			right_keys,
			residual: analysis.residual,
			alias,
			mode: HashJoinMode::Left,
			headers: None,
			context: JoinContext::new(),
			state: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::hash::materialize")]
	fn materialize(
		state: &HashJoinState,
		probe_slots: &[ProbeSlot],
		build_picks: &[usize],
		result_row_numbers: Vec<RowNumber>,
		has_row_numbers: bool,
	) -> Result<Columns> {
		let slots: Vec<JoinSlot<'_>> = if probe_slots.is_empty() {
			vec![JoinSlot {
				columns: &state.probe_shells,
				picks: &[],
			}]
		} else {
			probe_slots
				.iter()
				.map(|slot| JoinSlot {
					columns: &slot.columns,
					picks: &slot.picks,
				})
				.collect()
		};
		materialize_join(
			&state.resolved_names,
			&slots,
			&state.build_columns.columns,
			build_picks,
			result_row_numbers,
			has_row_numbers,
		)
	}

	fn resolve_without_probe(alias: &Option<Fragment>, state: &mut HashJoinState) {
		if state.resolved_names.is_empty() {
			let empty_left = Columns::empty();
			let resolved = resolve_column_names(&empty_left, &state.build_columns, alias, None);
			state.resolved_names = resolved.qualified_names;
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::hash::build")]
	fn build<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<()> {
		let build_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let right_key_indices: Vec<usize> = if build_columns.is_empty() {
			Vec::new()
		} else {
			self.right_keys.iter().map(|key| key_index(&build_columns, key)).collect::<Result<_>>()?
		};
		ensure_join_keyable(&build_columns, &right_key_indices)?;

		let key_columns: Vec<&ColumnBuffer> =
			right_key_indices.iter().map(|&idx| &build_columns[idx]).collect();
		let key_types = key_types(&key_columns);
		let (converter, hash_table) = key_table(&build_columns, &right_key_indices, &key_types)?;

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled_residual: Vec<CompiledExpr> =
			self.residual.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<_>>()?;

		self.state = Some(HashJoinState {
			build_columns,
			key_types,
			converter,
			hash_table,
			resolved_names: Vec::new(),
			probe_shells: Vec::new(),
			right_key_indices,
			left_key_indices: Vec::new(),
			probe_batch: None,
			probe_keys: None,
			probe_row_idx: 0,
			current_matches: Vec::new(),
			current_match_idx: 0,
			current_row_matched: false,
			probe_exhausted: false,
			compiled_residual,
		});

		Ok(())
	}
}

struct ProbeSlot {
	columns: Vec<ColumnBuffer>,
	picks: Vec<usize>,
}

fn picked(slots: &mut [ProbeSlot]) -> &mut Vec<usize> {
	&mut slots.last_mut().expect("the probe batch owning this row is registered before the row is picked").picks
}

type KeyNamePairs = Vec<(String, Fragment)>;

fn split_key_names(pairs: &[EquiKeyPair]) -> (KeyNamePairs, KeyNamePairs) {
	let left = pairs.iter().map(|p| (p.left_col_name.clone(), p.left_fragment.clone())).collect();
	let right = pairs.iter().map(|p| (p.right_col_name.clone(), p.right_fragment.clone())).collect();
	(left, right)
}

fn key_table(
	build_columns: &Columns,
	key_indices: &[usize],
	targets: &[ValueType],
) -> Result<(Option<RowConverter>, KeyTable)> {
	let mut hash_table: KeyTable = HashMap::new();
	if build_columns.is_empty() {
		return Ok((None, hash_table));
	}
	let key_columns: Vec<&ColumnBuffer> = key_indices.iter().map(|&idx| &build_columns[idx]).collect();
	let (converter, arrays) = key_rows(&key_columns, targets)?;
	let rows =
		converter.convert_columns(&arrays).map_err(|e| internal_error!("Failed to build join keys: {}", e))?;
	for j in 0..build_columns.row_count() {
		if arrays.iter().any(|array| array.is_null(j)) {
			continue;
		}
		let key = rows.row(j);
		match hash_table.get_mut(key.as_ref()) {
			Some(indices) => indices.push(j),
			None => {
				hash_table.insert(Box::from(key.as_ref()), vec![j]);
			}
		}
	}
	Ok((Some(converter), hash_table))
}

fn probe_key_rows(
	converter: Option<&RowConverter>,
	probe: &Columns,
	key_indices: &[usize],
	targets: &[ValueType],
) -> Result<Option<(Rows, Vec<ArrayRef>)>> {
	let Some(converter) = converter else {
		return Ok(None);
	};
	let key_columns: Vec<&ColumnBuffer> = key_indices.iter().map(|&idx| &probe[idx]).collect();
	let arrays = key_arrays(&key_columns, targets);
	let rows =
		converter.convert_columns(&arrays).map_err(|e| internal_error!("Failed to build join keys: {}", e))?;
	Ok(Some((rows, arrays)))
}

fn matches_for_probe_row(
	hash_table: &KeyTable,
	probe_keys: Option<&(Rows, Vec<ArrayRef>)>,
	probe_row_idx: usize,
) -> Vec<usize> {
	match probe_keys {
		Some((rows, arrays)) if !arrays.iter().any(|array| array.is_null(probe_row_idx)) => {
			hash_table.get(rows.row(probe_row_idx).as_ref()).cloned().unwrap_or_default()
		}
		_ => Vec::new(),
	}
}

impl QueryNode for HashJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::hash::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::hash::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_initialized(), "HashJoinNode::next() called before initialize()");
		}

		if self.state.is_none() {
			self.build(rx, ctx)?;
		}

		let batch_size = ctx.batch_size as usize;
		let stored_ctx = self.context.get().clone();

		let mut state = self.state.take().unwrap();

		if state.probe_exhausted && state.probe_batch.is_none() {
			if self.headers.is_some() {
				self.state = Some(state);
				return Ok(None);
			}
			Self::resolve_without_probe(&self.alias, &mut state);
			let left_rownum = self.left.headers().is_some_and(|h| h.row_numbers);
			let columns = Self::materialize(&state, &[], &[], Vec::new(), left_rownum)?;
			self.headers = Some(ColumnHeaders::from_columns(&columns));
			self.state = Some(state);
			return Ok(Some(columns));
		}

		let mut probe_slots: Vec<ProbeSlot> = Vec::new();
		let mut build_picks: Vec<usize> = Vec::new();
		let mut result_row_numbers: Vec<RowNumber> = Vec::new();

		if let Some(batch) = state.probe_batch.as_ref() {
			probe_slots.push(ProbeSlot {
				columns: batch.columns.clone(),
				picks: Vec::new(),
			});
		}

		let resolve_names_and_indices = |state: &mut HashJoinState,
		                                 probe: &Columns,
		                                 left_keys: &[(String, Fragment)]|
		 -> Result<()> {
			if state.resolved_names.is_empty() {
				let resolved = resolve_column_names(probe, &state.build_columns, &self.alias, None);
				state.resolved_names = resolved.qualified_names;
				state.probe_shells =
					probe.columns.iter().map(|column| column.extract_rows(&[])).collect();
			}
			if state.left_key_indices.is_empty() {
				state.left_key_indices =
					left_keys.iter().map(|key| key_index(probe, key)).collect::<Result<_>>()?;
			}
			Ok(())
		};

		while build_picks.len() < batch_size {
			if state.probe_batch.is_none() {
				if state.probe_exhausted {
					break;
				}
				match self.left.next(rx, ctx)? {
					Some(batch) => {
						resolve_names_and_indices(&mut state, &batch, &self.left_keys)?;
						ensure_join_keyable(&batch, &state.left_key_indices)?;
						let targets = join_key_types(
							&batch,
							&state.left_key_indices,
							&state.build_columns,
							&state.right_key_indices,
							|key| self.left_keys[key].1.clone(),
						)?;
						if targets != state.key_types {
							let (converter, hash_table) = key_table(
								&state.build_columns,
								&state.right_key_indices,
								&targets,
							)?;
							state.converter = converter;
							state.hash_table = hash_table;
							state.key_types = targets;
						}
						state.probe_batch = Some(batch);
						state.probe_row_idx = 0;

						let probe = state.probe_batch.as_ref().unwrap();
						if probe.row_count() == 0 {
							state.probe_batch = None;
							continue;
						}
						probe_slots.push(ProbeSlot {
							columns: probe.columns.clone(),
							picks: Vec::new(),
						});
						state.probe_keys = probe_key_rows(
							state.converter.as_ref(),
							probe,
							&state.left_key_indices,
							&state.key_types,
						)?;
						state.current_matches = matches_for_probe_row(
							&state.hash_table,
							state.probe_keys.as_ref(),
							0,
						);
						state.current_match_idx = 0;
						state.current_row_matched = false;
					}
					None => {
						state.probe_exhausted = true;
						break;
					}
				}
			}

			let probe = state.probe_batch.as_ref().unwrap();
			let probe_row_count = probe.row_count();

			if state.current_match_idx >= state.current_matches.len() {
				if self.mode == HashJoinMode::Left && !state.current_row_matched {
					picked(&mut probe_slots).push(state.probe_row_idx);
					build_picks.push(NO_MATCH);
					if !probe.row_numbers().is_empty() {
						result_row_numbers.push(probe.row_numbers()[state.probe_row_idx]);
					}
				}

				state.probe_row_idx += 1;
				if state.probe_row_idx >= probe_row_count {
					state.probe_batch = None;
					continue;
				}

				state.current_matches = matches_for_probe_row(
					&state.hash_table,
					state.probe_keys.as_ref(),
					state.probe_row_idx,
				);
				state.current_match_idx = 0;
				state.current_row_matched = false;
				continue;
			}

			let build_idx = state.current_matches[state.current_match_idx];
			state.current_match_idx += 1;

			if !state.compiled_residual.is_empty() {
				let left_row = probe.get_row(state.probe_row_idx);
				let right_row = state.build_columns.get_row(build_idx);
				if !eval_join_condition(
					&state.compiled_residual,
					probe,
					&state.build_columns,
					&left_row,
					&right_row,
					&self.alias,
					&stored_ctx,
				)? {
					continue;
				}
			}

			state.current_row_matched = true;
			picked(&mut probe_slots).push(state.probe_row_idx);
			build_picks.push(build_idx);
			if !probe.row_numbers().is_empty() {
				result_row_numbers.push(probe.row_numbers()[state.probe_row_idx]);
			}
		}

		self.state = Some(state);
		let left_rownum = self.left.headers().is_some_and(|h| h.row_numbers);

		if build_picks.is_empty() {
			if self.headers.is_some() {
				return Ok(None);
			}
			let Some(state) = self.state.as_mut() else {
				return Ok(None);
			};
			Self::resolve_without_probe(&self.alias, state);
			let columns =
				Self::materialize(state, &probe_slots, &build_picks, result_row_numbers, left_rownum)?;
			self.headers = Some(ColumnHeaders::from_columns(&columns));
			return Ok(Some(columns));
		}

		let state = self.state.as_ref().unwrap();
		let columns = Self::materialize(state, &probe_slots, &build_picks, result_row_numbers, left_rownum)?;

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
