// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_flow_async::window::{
	accumulator::WindowAccumulator,
	driver::hooks::{WindowEmits, WindowHooks},
	settings::WindowSettings,
	span::WindowSpan,
};
use reifydb_value::{
	Result,
	util::hash::{Hash128, xxh3_128},
};

use crate::flow::operator::windowed::operator::Emit;

pub struct SdkHooks<'a, A: Emit> {
	aggregator: &'a A,
	settings: &'a WindowSettings<A::Coord>,
}

impl<'a, A: Emit> SdkHooks<'a, A> {
	pub fn new(aggregator: &'a A, settings: &'a WindowSettings<A::Coord>) -> Self {
		Self {
			aggregator,
			settings,
		}
	}
}

impl<A> WindowHooks for SdkHooks<'_, A>
where
	A: Emit,
	for<'g> &'g A::GroupKey: IntoEncodedKey,
{
	type Group = A::GroupKey;
	type Coord = A::Coord;
	type Accumulator = A::Accumulator;
	type Row = A::Output;
	type Out = WindowEmits<A::Output>;

	fn new_accumulator(&self) -> A::Accumulator {
		self.aggregator.new_accumulator(self.settings)
	}

	fn group_identity(&self, group: &A::GroupKey) -> (Hash128, EncodedKey) {
		let key = group.into_encoded_key();
		(xxh3_128(key.as_slice()), key)
	}

	fn build(
		&self,
		group: &A::GroupKey,
		span: WindowSpan<A::Coord>,
		value: &<A::Accumulator as WindowAccumulator>::Output,
	) -> Option<A::Output> {
		self.aggregator.build_output(group, span, value)
	}

	fn emit(&mut self, emits: WindowEmits<A::Output>) -> Result<WindowEmits<A::Output>> {
		Ok(emits)
	}
}
