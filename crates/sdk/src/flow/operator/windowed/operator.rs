// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::StateCodec;
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::heap::HeapSize, operator_with::ApplyWith};
pub use reifydb_flow::window::settings::WindowSettings;
use reifydb_flow::{
	operator::state::seal::domain::SealDomain,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		span::{WindowAnchor, WindowSpan},
	},
};
use reifydb_value::config::ExtensionParams;

use crate::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::row::OutputRows,
		context::{GuestContext, Windowed},
		view::RowView,
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

pub trait KindSet<T: Emit> {}

impl<T: Emit> KindSet<T> for AllKinds where T::Accumulator: MergeAccumulator {}

impl<T: Emit> KindSet<T> for NoRolling {}

pub struct PlainMarker;

pub struct CarryMarker;

pub trait WindowDriver<M>: WindowedOperator {}

impl<T: Emit> WindowDriver<PlainMarker> for T {}

impl<T: CarryEmit> WindowDriver<CarryMarker> for T {}
