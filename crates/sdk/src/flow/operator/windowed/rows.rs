// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow_async::window::driver::rows::{WindowColumns, WindowDiff, WindowRows};
use reifydb_value::{Result, error::Error, value::diff_type::DiffType};

use crate::{
	error::SdkError,
	flow::operator::{
		context::GuestContext,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			all_rows,
			operator::{Contribution, WindowedOperator, extract_rows},
		},
	},
};

pub struct SdkRows<'a, A, C, V> {
	aggregator: &'a A,
	ctx: &'a mut C,
	change: &'a V,
}

impl<'a, A, C, V> SdkRows<'a, A, C, V>
where
	A: WindowedOperator,
	C: GuestContext,
	V: ChangeView,
{
	pub fn new(aggregator: &'a A, ctx: &'a mut C, change: &'a V) -> Self {
		Self {
			aggregator,
			ctx,
			change,
		}
	}
}

fn columns<A: WindowedOperator>(
	aggregator: &A,
	ctx: &mut impl GuestContext,
	cols: &impl ColumnsView,
) -> Result<WindowColumns<A::GroupKey, A::Coord, Contribution<A>>> {
	let extracted = extract_rows(aggregator, ctx, cols, &all_rows(cols))?;
	let numbers = cols.row_numbers();
	let mut index = Vec::with_capacity(extracted.len());
	let mut rows = Vec::with_capacity(extracted.len());
	let mut groups = Vec::with_capacity(extracted.len());
	let mut coords = Vec::with_capacity(extracted.len());
	let mut contributions = Vec::with_capacity(extracted.len());
	for (row, coord, group, contribution) in extracted {
		index.push(row);
		rows.push(numbers.get(row).copied());
		groups.push(group);
		coords.push(Some(coord));
		contributions.push(contribution);
	}
	WindowColumns::new(index, rows, groups, coords, contributions)
}

fn missing_side(index: usize, kind: DiffType, side: &str) -> Error {
	SdkError::Other(format!("diff {index} is {kind:?} but carries no {side} columns")).into()
}

impl<A, C, V> WindowRows for SdkRows<'_, A, C, V>
where
	A: WindowedOperator,
	C: GuestContext,
	V: ChangeView,
{
	type Group = A::GroupKey;
	type Coord = A::Coord;
	type Contribution = Contribution<A>;

	fn diff_count(&self) -> usize {
		self.change.diff_count()
	}

	fn diff(&mut self, index: usize) -> Result<WindowDiff<WindowColumns<A::GroupKey, A::Coord, Contribution<A>>>> {
		let change = self.change;
		let Some(diff) = change.diff(index) else {
			return Err(SdkError::Other(format!(
				"window rows asked for diff {index} of a change holding {}",
				change.diff_count()
			))
			.into());
		};
		let kind = diff.kind();
		match kind {
			DiffType::Insert => {
				let post = diff.post().ok_or_else(|| missing_side(index, kind, "post"))?;
				Ok(WindowDiff::Insert(columns(self.aggregator, self.ctx, &post)?))
			}
			DiffType::Update => {
				let pre = diff.pre().ok_or_else(|| missing_side(index, kind, "pre"))?;
				let post = diff.post().ok_or_else(|| missing_side(index, kind, "post"))?;
				Ok(WindowDiff::Update {
					pre: columns(self.aggregator, self.ctx, &pre)?,
					post: columns(self.aggregator, self.ctx, &post)?,
				})
			}
			DiffType::Remove => {
				let pre = diff.pre().ok_or_else(|| missing_side(index, kind, "pre"))?;
				Ok(WindowDiff::Remove(columns(self.aggregator, self.ctx, &pre)?))
			}
		}
	}
}
