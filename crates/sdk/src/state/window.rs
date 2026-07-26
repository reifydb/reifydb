// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::state::OperatorState;
use reifydb_core::key::operator_state::StateKey;

use super::RawStatefulOperator;
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait WindowStateful: RawStatefulOperator {
	type State: OperatorState;

	fn load_state(&self, ctx: &mut impl OperatorContext, window_key: &StateKey) -> Result<Option<Self::State>> {
		ctx.state().get::<Self::State>(window_key)
	}

	fn save_state(&self, ctx: &mut impl OperatorContext, window_key: &StateKey, value: &Self::State) -> Result<()> {
		ctx.state().set(window_key, value)
	}

	fn remove_window(&self, ctx: &mut impl OperatorContext, window_key: &StateKey) -> Result<()> {
		ctx.state().remove(window_key)
	}
}
