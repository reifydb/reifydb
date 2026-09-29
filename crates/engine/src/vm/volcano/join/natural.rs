// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch};
use arrow_row::RowConverter;
use reifydb_core::{
	common::JoinType, error::diagnostic::operation, internal_error, value::column::headers::ColumnHeaders,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	reifydb_assertions,
	value::{system_columns::user_columns, value_type::ValueType},
};
use tracing::instrument;

use super::common::{
	JoinContext, JoinSlot, ensure_join_keyable, join_key_types, load_and_merge_all, materialize_join,
	resolve_column_names, user_key_columns, user_views,
};
use crate::{
	Result,
	vm::volcano::{
		key_rows::{key_arrays, key_rows},
		query::{QueryContext, QueryNode},
	},
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

	fn find_common_columns(left_columns: &RecordBatch, right_columns: &RecordBatch) -> Vec<(String, usize, usize)> {
		let mut common_columns = Vec::new();

		for (left_idx, (left_field, _)) in user_columns(left_columns).enumerate() {
			for (right_idx, (right_field, _)) in user_columns(right_columns).enumerate() {
				if left_field.name() == right_field.name() {
					common_columns.push((left_field.name().to_string(), left_idx, right_idx));
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
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_initialized(), "NaturalJoinNode::next() called before initialize()");
		}

		if self.headers.is_some() {
			return Ok(None);
		}

		let left_columns = load_and_merge_all(&mut self.left, rx, ctx)?;
		let right_columns = load_and_merge_all(&mut self.right, rx, ctx)?;

		let left_rows = left_columns.num_rows();

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
		let left_views = user_views(&left_columns)?;
		let right_views = user_views(&right_columns)?;
		ensure_join_keyable(&left_views, &left_col_indices, |_| self.fragment.clone())?;
		ensure_join_keyable(&right_views, &right_col_indices, |_| self.fragment.clone())?;
		let targets = join_key_types(&left_views, &left_col_indices, &right_views, &right_col_indices, |_| {
			self.fragment.clone()
		})?;

		let (converter, hash_table) = Self::build(&right_columns, &right_col_indices, &targets)?;

		let (left_picks, right_picks) = self.probe(
			&left_columns,
			&ProbeContext {
				converter: &converter,
				hash_table: &hash_table,
				left_col_indices: &left_col_indices,
				targets: &targets,
				left_rows,
			},
		)?;

		let columns = materialize_join(
			&resolved.qualified_names,
			&[JoinSlot {
				columns: &left_columns,
				picks: &left_picks,
			}],
			&right_columns,
			&excluded_indices,
			&right_picks,
			0,
		)?;

		self.headers = Some(ColumnHeaders::from_batch(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}

type KeyIndex = HashMap<Box<[u8]>, Vec<usize>>;

struct ProbeContext<'a> {
	converter: &'a RowConverter,
	hash_table: &'a KeyIndex,
	left_col_indices: &'a [usize],
	targets: &'a [ValueType],
	left_rows: usize,
}

impl NaturalJoinNode {
	#[instrument(level = "trace", skip_all, name = "volcano::join::natural::build")]
	fn build(
		right_columns: &RecordBatch,
		right_col_indices: &[usize],
		targets: &[ValueType],
	) -> Result<(RowConverter, KeyIndex)> {
		let (converter, arrays) = key_rows(&user_key_columns(right_columns, right_col_indices), targets)?;
		let rows = converter
			.convert_columns(&arrays)
			.map_err(|e| internal_error!("Failed to build join keys: {}", e))?;
		let mut hash_table: KeyIndex = HashMap::new();
		for j in 0..right_columns.num_rows() {
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
		Ok((converter, hash_table))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::join::natural::probe")]
	fn probe(
		&self,
		left_columns: &RecordBatch,
		probe_ctx: &ProbeContext,
	) -> Result<(Vec<usize>, Vec<Option<usize>>)> {
		let ProbeContext {
			converter,
			hash_table,
			left_col_indices,
			targets,
			left_rows,
		} = probe_ctx;
		let arrays = key_arrays(&user_key_columns(left_columns, left_col_indices), targets);
		let rows = converter
			.convert_columns(&arrays)
			.map_err(|e| internal_error!("Failed to build join keys: {}", e))?;

		let mut left_picks: Vec<usize> = Vec::new();
		let mut right_picks: Vec<Option<usize>> = Vec::new();

		for i in 0..*left_rows {
			let mut matched = false;

			let candidates = if arrays.iter().any(|array| array.is_null(i)) {
				None
			} else {
				hash_table.get(rows.row(i).as_ref())
			};

			if let Some(indices) = candidates {
				for &j in indices {
					left_picks.push(i);
					right_picks.push(Some(j));
					matched = true;
				}
			}

			if !matched && matches!(self.join_type, JoinType::Left) {
				left_picks.push(i);
				right_picks.push(None);
			}
		}

		Ok((left_picks, right_picks))
	}
}
