// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::encoded::key::EncodedKey;
use serde::{Serialize, de::DeserializeOwned};

use super::{RawStatefulOperator, utils};
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait SingleStateful: RawStatefulOperator {
	type State: Serialize + DeserializeOwned;

	fn key(&self) -> EncodedKey {
		utils::empty_key()
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
