// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::encoded::key::EncodedKey;
use serde::{Serialize, de::DeserializeOwned};

use super::RawStatefulOperator;
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait WindowStateful: RawStatefulOperator {
	type State: Serialize + DeserializeOwned;

	fn load_state(&self, ctx: &mut impl OperatorContext, window_key: &EncodedKey) -> Result<Option<Self::State>> {
		ctx.state().get::<Self::State>(window_key)
	}

	fn save_state(
		&self,
		ctx: &mut impl OperatorContext,
		window_key: &EncodedKey,
		value: &Self::State,
	) -> Result<()> {
		ctx.state().set(window_key, value)
	}

	fn remove_window(&self, ctx: &mut impl OperatorContext, window_key: &EncodedKey) -> Result<()> {
		ctx.state().remove(window_key)
	}
}
