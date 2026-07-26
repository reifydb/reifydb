// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::state::OperatorState;
use reifydb_core::key::operator_state::StateKey;

use super::{RawStatefulOperator, utils};
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait SingleStateful: RawStatefulOperator {
	type State: OperatorState;

	fn key(&self) -> StateKey {
		utils::empty_state_key()
	}

	fn load_state(&self, ctx: &mut impl OperatorContext) -> Result<Option<Self::State>> {
		let key = self.key();
		ctx.state().get::<Self::State>(&key)
	}

	fn save_state(&self, ctx: &mut impl OperatorContext, value: &Self::State) -> Result<()> {
		let key = self.key();
		ctx.state().set(&key, value)
	}

	fn clear_state(&self, ctx: &mut impl OperatorContext) -> Result<()> {
		let key = self.key();
		ctx.state().remove(&key)
	}
}
