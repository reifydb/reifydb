// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{key::encoded::IntoEncodedKey, row::operator::state::StateCodec};
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::heap::HeapSize, operator_with::ApplyWith};
use reifydb_flow_async::{
	operator::state::seal::domain::SealDomain,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator, invertible::retained_map::RetainedAccumulator},
		engine::rolling::{RollingBuffer, merge_panes},
		settings::WindowSettings,
		span::{WindowAnchor, WindowSpan},
	},
};
use reifydb_value::config::ExtensionParams;

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		OperatorMetadata,
		column::row::{OutputRows, Row},
		context::{GuestContext, Windowed},
		view::{ColumnsView, RowView},
		windowed::{carry::CarryDriver, plain::PlainDriver, retained::RetainedDriver, top_k::TopKDriver},
	},
};

pub type Contribution<T> = <<T as WindowedOperator>::Accumulator as WindowAccumulator>::Contribution;

pub type Value<T> = <<T as WindowedOperator>::Accumulator as WindowAccumulator>::Output;

pub trait WindowedOperator: OperatorMetadata + Send + Sync + Sized {
	type Coord: WindowAnchor + SealDomain + Hash + StateCodec + HeapSize + Send + Sync;
	type GroupKey: Clone + Eq + Ord + Hash + Debug + StateCodec;
	type Accumulator: WindowAccumulator;
	type Output: OutputRows;

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>;

	fn coord(&self, _row: &impl RowView) -> Result<Option<Self::Coord>> {
		Err(batch_only::<Self>("coord"))
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		_row: &impl RowView,
	) -> Result<Option<(Self::GroupKey, Contribution<Self>)>> {
		Err(batch_only::<Self>("extract"))
	}

	fn coord_batch(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<Self::Coord>>> {
		rows.iter().map(|&i| cols.row(i).map(|row| self.coord(&row)).transpose().map(Option::flatten)).collect()
	}

	fn extract_batch(
		&self,
		ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(Self::GroupKey, Contribution<Self>)>>> {
		rows.iter()
			.map(|&i| cols.row(i).map(|row| self.extract(ctx, &row)).transpose().map(Option::flatten))
			.collect()
	}

	fn new_accumulator(&self, settings: &WindowSettings<Self::Coord>) -> Self::Accumulator;
}

pub(crate) type Extracted<T> =
	(usize, <T as WindowedOperator>::Coord, <T as WindowedOperator>::GroupKey, Contribution<T>);

pub(crate) fn extract_rows<T: WindowedOperator>(
	aggregator: &T,
	ctx: &mut impl GuestContext<Windowed>,
	cols: &impl ColumnsView,
	rows: &[usize],
) -> Result<Vec<Extracted<T>>> {
	let coords = batch_len::<T, _>(aggregator.coord_batch(cols, rows)?, rows.len(), "coord_batch")?;
	let kept: Vec<(usize, T::Coord)> =
		rows.iter().copied().zip(coords).filter_map(|(i, coord)| Some((i, coord?))).collect();
	let indices: Vec<usize> = kept.iter().map(|(i, _)| *i).collect();
	let extracted =
		batch_len::<T, _>(aggregator.extract_batch(ctx, cols, &indices)?, indices.len(), "extract_batch")?;
	Ok(kept.into_iter()
		.zip(extracted)
		.filter_map(|((i, coord), extracted)| {
			extracted.map(|(group, contribution)| (i, coord, group, contribution))
		})
		.collect())
}

fn batch_only<T: WindowedOperator>(method: &str) -> SdkError {
	SdkError::OperatorError(format!("{} reads rows only in batches, {method} is not implemented", T::NAME))
}

pub(crate) fn batch_len<T: WindowedOperator, V>(values: Vec<V>, rows: usize, method: &str) -> Result<Vec<V>> {
	if values.len() != rows {
		return Err(SdkError::OperatorError(format!(
			"{} {method} returned {} results for {rows} rows",
			T::NAME,
			values.len()
		)));
	}
	Ok(values)
}

pub trait Emit: WindowedOperator {
	type Kinds: KindSet<Self>;

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<Self::Coord>,
		value: &Value<Self>,
	) -> Option<Self::Output>;
}

pub trait CarryEmit: WindowedOperator {
	type Carry: Clone + Debug + StateCodec + HeapSize;

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<Self::Coord>,
		value: &Value<Self>,
		prev: Option<&Self::Carry>,
	) -> Option<Self::Output>;

	fn carry_forward(&self, value: &Value<Self>, prev: Option<&Self::Carry>) -> Option<Self::Carry>;
}

pub struct AllKinds;

pub struct NoRolling;

pub trait KindSet<T: Emit> {
	const ROLLING: bool;

	fn merge_panes(buffer: &RollingBuffer<T::Coord, T::Accumulator>) -> T::Accumulator;
}

impl<T: Emit> KindSet<T> for AllKinds
where
	T::Accumulator: MergeAccumulator,
{
	const ROLLING: bool = true;

	fn merge_panes(buffer: &RollingBuffer<T::Coord, T::Accumulator>) -> T::Accumulator {
		merge_panes(buffer)
	}
}

impl<T: Emit> KindSet<T> for NoRolling {
	const ROLLING: bool = false;

	fn merge_panes(_buffer: &RollingBuffer<T::Coord, T::Accumulator>) -> T::Accumulator {
		unreachable!("a NoRolling operator never runs a rolling window; create refuses it first")
	}
}

pub struct PlainMarker;

pub struct CarryMarker;

pub struct RetainedMarker;

pub struct TopKMarker;

pub trait WindowDriver<M>: WindowedOperator {
	type Driver;
}

impl<T: Emit> WindowDriver<PlainMarker> for T
where
	T::Output: Row,
	for<'a> &'a T::GroupKey: IntoEncodedKey,
{
	type Driver = PlainDriver<T>;
}

impl<T: CarryEmit> WindowDriver<CarryMarker> for T
where
	T::Output: Row,
	for<'a> &'a T::GroupKey: IntoEncodedKey,
{
	type Driver = CarryDriver<T>;
}

impl<T, K, V> WindowDriver<RetainedMarker> for T
where
	T: Emit<Accumulator = RetainedAccumulator<K, V>>,
	T::Output: Row,
	K: Ord,
	for<'a> &'a T::GroupKey: IntoEncodedKey,
{
	type Driver = RetainedDriver<T, K, V>;
}

impl<T, SK, R> WindowDriver<TopKMarker> for T
where
	T: Emit<Kinds = AllKinds, Output = BTreeMap<SK, R>>,
	SK: Ord,
	R: Row,
	for<'a> &'a T::GroupKey: IntoEncodedKey,
	for<'a> &'a SK: IntoEncodedKey,
{
	type Driver = TopKDriver<T>;
}
