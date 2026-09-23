// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_core::{
	common::JoinType,
	error::diagnostic::operation,
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error, fragment::Fragment, reifydb_assertions, util::hash::Hash128, value::row_number::RowNumber};
use tracing::instrument;

use super::common::{
	JoinContext, JoinSlot, NO_MATCH, compute_join_hash, ensure_join_keyable, load_and_merge_all, materialize_join,
	resolve_column_names,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct NaturalJoinNode {
	left: Box<dyn QueryNode>,
	right: Box<dyn QueryNode>,
	join_type: JoinType,
	alias: Option<Fragment>,
	left_name: Option<Fragment>,
	fragment: Fragment,
	headers: Option<ColumnHeaders>,
	context: JoinContext,
}

impl NaturalJoinNode {
	pub(crate) fn new(
		left: Box<dyn QueryNode>,
		right: Box<dyn QueryNode>,
		join_type: JoinType,
		alias: Option<Fragment>,
		left_name: Option<Fragment>,
		fragment: Fragment,
	) -> Self {
		Self {
			left,
			right,
			join_type,
			alias,
			left_name,
			fragment,
			headers: None,
			context: JoinContext::new(),
		}
	}

	fn find_common_columns(left_columns: &Columns, right_columns: &Columns) -> Vec<(String, usize, usize)> {
		let mut common_columns = Vec::new();

		for (left_idx, left_col) in left_columns.iter().enumerate() {
			for (right_idx, right_col) in right_columns.iter().enumerate() {
				if left_col.name().text() == right_col.name().text() {
					common_columns.push((left_col.name().text().to_string(), left_idx, right_idx));
				}
			}
		}

		common_columns
	}
}

impl QueryNode for NaturalJoinNode {
	#[instrument(name = "volcano::join::natural::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context.set(ctx);
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::join::natural::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_initialized(), "NaturalJoinNode::next() called before initialize()");
		}

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.row_count();
		let left_row_numbers = left_columns.row_numbers().to_vec();

		let common_columns = Self::find_common_columns(&left_columns, &right_columns);

		if common_columns.is_empty() {
			let left = self.left_name.as_ref().map_or("the left input", |name| name.text());
			let right = self.alias.as_ref().map_or("the right input", |name| name.text());
			return Err(error!(operation::natural_join_no_shared_column(
				self.fragment.clone(),
				left,
				right
			)));
		}

		let excluded_right_cols: HashSet<usize> =
			common_columns.iter().map(|(_, _, right_idx)| *right_idx).collect();

		let excluded_indices: Vec<usize> = excluded_right_cols.iter().copied().collect();

		let resolved =
			resolve_column_names(&left_columns, &right_columns, &self.alias, Some(&excluded_indices));

		let right_col_indices: Vec<usize> = common_columns.iter().map(|(_, _, ri)| *ri).collect();
		let left_col_indices: Vec<usize> = common_columns.iter().map(|(_, li, _)| *li).collect();
		ensure_join_keyable(&left_columns, &left_col_indices)?;
		ensure_join_keyable(&right_columns, &right_col_indices)?;

		let mut hash_buf = Vec::with_capacity(256);
		let hash_table = Self::build(&right_columns, &right_col_indices, &mut hash_buf)?;

		let (left_picks, right_picks, result_row_numbers) = self.probe(
			&left_columns,
			&right_columns,
			&hash_table,
			&common_columns,
			&left_col_indices,
			&left_row_numbers,
			left_rows,
			&mut hash_buf,
		)?;

		let kept_right: Vec<ColumnBuffer> = right_columns
			.columns
			.iter()
			.enumerate()
			.filter(|(idx, _)| !excluded_right_cols.contains(idx))
			.map(|(_, column)| column.clone())
			.collect();

		let left_rownum = self.left.headers().is_some_and(|h| h.row_numbers);
		let columns = materialize_join(
			&resolved.qualified_names,
			&[JoinSlot {
				columns: &left_columns.columns,
				picks: &left_picks,
			}],
			&kept_right,
			&right_picks,
			result_row_numbers,
			left_rownum,
		)?;

		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}

impl NaturalJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::natural::build")]
	fn build(
		right_columns: &Columns,
		right_col_indices: &[usize],
		hash_buf: &mut Vec<u8>,
	) -> Result<HashMap<Hash128, Vec<usize>>> {
		let mut hash_table: HashMap<Hash128, Vec<usize>> = HashMap::new();
		let right_rows = right_columns.row_count();
		for j in 0..right_rows {
			if let Some(h) = compute_join_hash(right_columns, right_col_indices, j, hash_buf)? {
				hash_table.entry(h).or_default().push(j);
			}
		}
		Ok(hash_table)
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(level = "trace", skip_all, name = "volcano::join::natural::probe")]
	fn probe(
		&self,
		left_columns: &Columns,
		right_columns: &Columns,
		hash_table: &HashMap<Hash128, Vec<usize>>,
		common_columns: &[(String, usize, usize)],
		left_col_indices: &[usize],
		left_row_numbers: &[RowNumber],
		left_rows: usize,
		hash_buf: &mut Vec<u8>,
	) -> Result<(Vec<usize>, Vec<usize>, Vec<RowNumber>)> {
		let mut left_picks: Vec<usize> = Vec::new();
		let mut right_picks: Vec<usize> = Vec::new();
		let mut result_row_numbers: Vec<RowNumber> = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);
			let mut matched = false;

			let candidates = compute_join_hash(left_columns, left_col_indices, i, hash_buf)?
				.and_then(|h| hash_table.get(&h));

			if let Some(indices) = candidates {
				for &j in indices {
					let right_row = right_columns.get_row(j);

					let all_match = common_columns.iter().all(|(_, left_idx, right_idx)| {
						left_row[*left_idx] == right_row[*right_idx]
					});

					if all_match {
						left_picks.push(i);
						right_picks.push(j);
						matched = true;
						if !left_row_numbers.is_empty() {
							result_row_numbers.push(left_row_numbers[i]);
						}
					}
				}
			}

			if !matched && matches!(self.join_type, JoinType::Left) {
				left_picks.push(i);
				right_picks.push(NO_MATCH);
				if !left_row_numbers.is_empty() {
					result_row_numbers.push(left_row_numbers[i]);
				}
			}
		}

		Ok((left_picks, right_picks, result_row_numbers))
	}
}
