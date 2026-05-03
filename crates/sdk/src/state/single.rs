// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::key::EncodedKey;
use serde::{Serialize, de::DeserializeOwned};

use super::{FFIRawStatefulOperator, utils};
use crate::{error::Result, operator::context::OperatorContext};

pub trait FFISingleStateful: FFIRawStatefulOperator {
	type State: Serialize + DeserializeOwned;

	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	fn load_state(&self, ctx: &mut OperatorContext) -> Result<Option<Self::State>> {
		let key = self.key();
		ctx.state().get::<Self::State>(&key)
	}

	fn save_state(&self, ctx: &mut OperatorContext, value: &Self::State) -> Result<()> {
		let key = self.key();
		ctx.state().set(&key, value)
	}

	fn clear_state(&self, ctx: &mut OperatorContext) -> Result<()> {
		let key = self.key();
		ctx.state().remove(&key)
	}
}
