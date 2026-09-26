// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};

use crate::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			GroupMeta,
			config::WindowEngineConfig,
			tumbling::{TumblingEngine, TumblingIndexEntry},
		},
		span::{WindowAnchor, WindowSpan},
	},
};

pub struct SlidingEngine<G, S: Coord, Accumulator> {
	tumbling: TumblingEngine<G, S, Accumulator>,
	size: S::Span,
	slide: S::Span,
}

impl<G, S, Accumulator> SlidingEngine<G, S, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
	G: StateCodec,
	GroupMeta<S>: OperatorState,
	TumblingIndexEntry<G, S>: OperatorState,
{
	pub fn new(config: WindowEngineConfig, size: S::Span, slide: S::Span) -> Self {
		Self {
			tumbling: TumblingEngine::new(config),
			size,
			slide,
		}
	}

	pub fn spans(&self, coord: S) -> Vec<WindowSpan<S>> {
		WindowSpan::covering(coord, self.size, self.slide)
	}

	pub fn tumbling_mut(&mut self) -> &mut TumblingEngine<G, S, Accumulator> {
		&mut self.tumbling
	}
}
