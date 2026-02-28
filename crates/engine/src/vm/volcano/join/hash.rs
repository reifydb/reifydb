// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::expression::Expression;
use reifydb_runtime::hash::Hash128;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, row_number::RowNumber},
};
use tracing::instrument;

use super::common::{
	JoinContext, compute_join_hash, eval_join_condition, keys_equal_by_index, load_and_merge_all,
	resolve_column_names,
};
use crate::{
	Result,
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::CompileContext,
	},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct EquiKeyPair {
	pub left_col_name: String,
	pub right_col_name: String,
}

pub(crate) struct EquiJoinAnalysis {
	pub equi_keys: Vec<EquiKeyPair>,
	pub residual: Vec<Expression>,
}

/// Analyze join `ON` expressions to separate equi-join key pairs from residual
/// predicates.  An equi-key pair is `Equal(Column(name), AccessSource(name))`
/// (in either order).  Any `Or` anywhere causes an immediate abort — all
/// expressions are returned as residual.
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

/// Recursively check whether an expression tree contains `Or`.
fn contains_or(expr: &Expression) -> bool {
	match expr {
		Expression::Or(_) => true,
		Expression::And(and) => contains_or(&and.left) || contains_or(&and.right),
		_ => false,
	}
}

/// Flatten a tree of `And` nodes into a flat list of leaf expressions.
fn flatten_and(expr: &Expression, out: &mut Vec<Expression>) {
	match expr {
		Expression::And(and) => {
			flatten_and(&and.left, out);
			flatten_and(&and.right, out);
		}
		other => out.push(other.clone()),
	}
}

/// Try to extract an equi-join key pair from an `Equal` expression.
/// Matches `Equal(Column(..), AccessSource(..))` in either order.
fn try_extract_equi_pair(expr: &Expression) -> Option<EquiKeyPair> {
	if let Expression::Equal(eq) = expr {
		// Column == AccessSource
		if let (Expression::Column(col), Expression::AccessSource(acc)) = (eq.left.as_ref(), eq.right.as_ref())
		{
			return Some(EquiKeyPair {
				left_col_name: col.0.name.text().to_string(),
				right_col_name: acc.column.name.text().to_string(),
			});
		}
		// AccessSource == Column  (swapped)
		if let (Expression::AccessSource(acc), Expression::Column(col)) = (eq.left.as_ref(), eq.right.as_ref())
		{
			return Some(EquiKeyPair {
				left_col_name: col.0.name.text().to_string(),
				right_col_name: acc.column.name.text().to_string(),
			});
		}
	}
	None
}

#[derive(Clone, Copy, PartialEq)]
enum HashJoinMode {
	Inner,
	Left,
}

struct HashJoinState {
	build_columns: Columns,
	hash_table: HashMap<Hash128, Vec<usize>>,
	resolved_names: Vec<String>,
	right_width: usize,
	right_key_indices: Vec<usize>,
	left_key_indices: Vec<usize>,

	// Probe cursor
	probe_batch: Option<Columns>,
	probe_row_idx: usize,
	current_matches: Vec<usize>,
	current_match_idx: usize,
	current_row_matched: bool,
	probe_exhausted: bool,

	// Compiled residual predicates
	compiled_residual: Vec<CompiledExpr>,

	// Reusable scratch buffer for hashing
	hash_buf: Vec<u8>,
}

pub(crate) struct HashJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,

	left_key_names: Vec<String>,
	right_key_names: Vec<String>,
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
			left_key_names: left_keys,
			right_key_names: right_keys,
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
			left_key_names: left_keys,
			right_key_names: right_keys,
			residual: analysis.residual,
			alias,
			mode: HashJoinMode::Left,
			headers: None,
			context: JoinContext::new(),
			state: None,
		}
	}

	/// Build phase: materialize the right side into a hash table.
	fn build<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<()> {
		let build_columns = load_and_merge_all(&mut self.right, rx, ctx)?;
		let right_width = build_columns.len();

		// Pre-resolve right key name → column index (empty when build side has no columns)
		let right_key_indices: Vec<usize> = if build_columns.len() == 0 {
			Vec::new()
		} else {
			self.right_key_names
				.iter()
				.map(|name| {
					build_columns
						.iter()
						.position(|c| c.name().text() == name)
						.unwrap_or_else(|| panic!("right key column '{}' not found", name))
				})
				.collect()
		};

		// Build hash table using index-based hashing
		let mut hash_table: HashMap<Hash128, Vec<usize>> = HashMap::new();
		let mut hash_buf = Vec::with_capacity(256);
		let row_count = build_columns.row_count();
		for j in 0..row_count {
			if let Some(h) = compute_join_hash(&build_columns, &right_key_indices, j, &mut hash_buf) {
				hash_table.entry(h).or_default().push(j);
			}
		}

		// Compile residual predicates
		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbol_table: &ctx.stack,
		};
		let compiled_residual: Vec<CompiledExpr> = self
			.residual
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("compile residual"))
			.collect();

		self.state = Some(HashJoinState {
			build_columns,
			hash_table,
			resolved_names: Vec::new(),
			right_width,
			right_key_indices,
			left_key_indices: Vec::new(), // resolved on first probe batch
			probe_batch: None,
			probe_row_idx: 0,
			current_matches: Vec::new(),
			current_match_idx: 0,
			current_row_matched: false,
			probe_exhausted: false,
			compiled_residual,
			hash_buf,
		});

		Ok(())
	}
}

fn split_key_names(pairs: &[EquiKeyPair]) -> (Vec<String>, Vec<String>) {
	let left: Vec<String> = pairs.iter().map(|p| p.left_col_name.clone()).collect();
	let right: Vec<String> = pairs.iter().map(|p| p.right_col_name.clone()).collect();
	(left, right)
}

/// Compute hash → lookup bucket → filter by key equality for a single probe row.
fn compute_matches_for_probe_row(
	hash_table: &HashMap<Hash128, Vec<usize>>,
	build_columns: &Columns,
	probe: &Columns,
	probe_row_idx: usize,
	left_key_indices: &[usize],
	right_key_indices: &[usize],
	buf: &mut Vec<u8>,
) -> Vec<usize> {
	match compute_join_hash(probe, left_key_indices, probe_row_idx, buf) {
		Some(h) => hash_table
			.get(&h)
			.map(|indices| {
				indices.iter()
					.copied()
					.filter(|&build_idx| {
						keys_equal_by_index(
							probe,
							probe_row_idx,
							left_key_indices,
							build_columns,
							build_idx,
							right_key_indices,
						)
					})
					.collect()
			})
			.unwrap_or_default(),
		None => Vec::new(),
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
		debug_assert!(self.context.is_initialized(), "HashJoinNode::next() called before initialize()");

		// Build phase (first call)
		if self.state.is_none() {
			self.build(rx, ctx)?;
		}

		let batch_size = ctx.batch_size as usize;
		let stored_ctx = self.context.get().clone();

		// We need to work around the borrow checker: take state out, work with it, put it back.
		let mut state = self.state.take().unwrap();

		if state.probe_exhausted && state.probe_batch.is_none() {
			if self.headers.is_some() {
				self.state = Some(state);
				return Ok(None);
			}
			if state.resolved_names.is_empty() {
				let empty_left = Columns::empty();
				let resolved =
					resolve_column_names(&empty_left, &state.build_columns, &self.alias, None);
				state.resolved_names = resolved.qualified_names;
			}
			let names_refs: Vec<&str> = state.resolved_names.iter().map(|s| s.as_str()).collect();
			let empty: Vec<Vec<Value>> = Vec::new();
			let columns = Columns::from_rows(&names_refs, &empty);
			self.headers = Some(ColumnHeaders::from_columns(&columns));
			self.state = Some(state);
			return Ok(Some(columns));
		}

		let mut result_rows: Vec<Vec<Value>> = Vec::new();
		let mut result_row_numbers: Vec<RowNumber> = Vec::new();

		// Resolve column names and left key indices lazily on first probe batch
		let resolve_names_and_indices = |state: &mut HashJoinState,
		                                 probe: &Columns,
		                                 left_key_names: &[String]| {
			if state.resolved_names.is_empty() {
				let resolved = resolve_column_names(probe, &state.build_columns, &self.alias, None);
				state.resolved_names = resolved.qualified_names;
			}
			if state.left_key_indices.is_empty() {
				state.left_key_indices = left_key_names
					.iter()
					.map(|name| {
						probe.iter().position(|c| c.name().text() == name).unwrap_or_else(
							|| panic!("left key column '{}' not found", name),
						)
					})
					.collect();
			}
		};

		while result_rows.len() < batch_size {
			// Ensure we have a probe batch
			if state.probe_batch.is_none() {
				if state.probe_exhausted {
					break;
				}
				match self.left.next(rx, ctx)? {
					Some(batch) => {
						resolve_names_and_indices(&mut state, &batch, &self.left_key_names);
						state.probe_batch = Some(batch);
						state.probe_row_idx = 0;
						// Compute matches for first row
						let probe = state.probe_batch.as_ref().unwrap();
						if probe.row_count() == 0 {
							state.probe_batch = None;
							continue;
						}
						state.current_matches = compute_matches_for_probe_row(
							&state.hash_table,
							&state.build_columns,
							probe,
							0,
							&state.left_key_indices,
							&state.right_key_indices,
							&mut state.hash_buf,
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

			// Check if current probe row's matches are exhausted
			if state.current_match_idx >= state.current_matches.len() {
				// Emit unmatched left row for left joins
				if self.mode == HashJoinMode::Left && !state.current_row_matched {
					let left_row = probe.get_row(state.probe_row_idx);
					let mut combined = left_row;
					combined.extend(vec![Value::none(); state.right_width]);
					result_rows.push(combined);
					if !probe.row_numbers.is_empty() {
						result_row_numbers.push(probe.row_numbers[state.probe_row_idx]);
					}
				}

				// Advance to next probe row
				state.probe_row_idx += 1;
				if state.probe_row_idx >= probe_row_count {
					state.probe_batch = None;
					continue;
				}

				// Compute matches for new probe row
				state.current_matches = compute_matches_for_probe_row(
					&state.hash_table,
					&state.build_columns,
					probe,
					state.probe_row_idx,
					&state.left_key_indices,
					&state.right_key_indices,
					&mut state.hash_buf,
				);
				state.current_match_idx = 0;
				state.current_row_matched = false;
				continue;
			}

			// Emit a match
			let build_idx = state.current_matches[state.current_match_idx];
			state.current_match_idx += 1;

			let left_row = probe.get_row(state.probe_row_idx);
			let right_row = state.build_columns.get_row(build_idx);

			// Evaluate residual predicates
			if !state.compiled_residual.is_empty()
				&& !eval_join_condition(
					&state.compiled_residual,
					probe,
					&state.build_columns,
					&left_row,
					&right_row,
					&self.alias,
					&stored_ctx,
				) {
				continue;
			}

			state.current_row_matched = true;
			let mut combined = left_row;
			combined.extend(right_row);
			result_rows.push(combined);
			if !probe.row_numbers.is_empty() {
				result_row_numbers.push(probe.row_numbers[state.probe_row_idx]);
			}
		}

		self.state = Some(state);

		if result_rows.is_empty() {
			if self.headers.is_some() {
				return Ok(None);
			}
			if let Some(ref mut state) = self.state {
				if state.resolved_names.is_empty() {
					let empty_left = Columns::empty();
					let resolved = resolve_column_names(
						&empty_left,
						&state.build_columns,
						&self.alias,
						None,
					);
					state.resolved_names = resolved.qualified_names;
				}
				let names_refs: Vec<&str> = state.resolved_names.iter().map(|s| s.as_str()).collect();
				let columns = Columns::from_rows(&names_refs, &result_rows);
				self.headers = Some(ColumnHeaders::from_columns(&columns));
				return Ok(Some(columns));
			}
			return Ok(None);
		}

		let state = self.state.as_ref().unwrap();
		let names_refs: Vec<&str> = state.resolved_names.iter().map(|s| s.as_str()).collect();
		let columns = if result_row_numbers.is_empty() {
			Columns::from_rows(&names_refs, &result_rows)
		} else {
			Columns::from_rows_with_row_numbers(&names_refs, &result_rows, result_row_numbers)
		};

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
