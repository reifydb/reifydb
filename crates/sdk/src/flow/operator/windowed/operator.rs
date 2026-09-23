// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{key::encoded::IntoEncodedKey, row::operator::state::StateCodec};
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::heap::HeapSize, operator_with::ApplyWith};
use reifydb_flow::{
	operator::state::seal::domain::SealDomain,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		engine::rolling::{RollingBuffer, merge_panes},
		settings::WindowSettings,
		span::{WindowAnchor, WindowSpan},
	},
};
use reifydb_value::config::ExtensionParams;

use crate::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::row::{OutputRows, Row},
		context::{GuestContext, Windowed},
		view::RowView,
		windowed::{carry::CarryDriver, plain::PlainDriver, top_k::TopKDriver},
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

	fn coord(&self, row: &impl RowView) -> Option<Self::Coord>;

	fn extract(
		&self,
		ctx: &mut impl GuestContext<Windowed>,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Contribution<Self>)>;

	fn new_accumulator(&self, settings: &WindowSettings<Self::Coord>) -> Self::Accumulator;
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
