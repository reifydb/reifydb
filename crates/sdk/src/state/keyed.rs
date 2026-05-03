// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::key::EncodedKey, util::encoding::keycode::serializer::KeySerializer};
use reifydb_type::value::{Value, r#type::Type};
use serde::{Serialize, de::DeserializeOwned};

use super::FFIRawStatefulOperator;
use crate::{error::Result, operator::context::OperatorContext};

pub trait FFIKeyedStateful: FFIRawStatefulOperator {
	type State: Serialize + DeserializeOwned;

	fn key_types(&self) -> &[Type];

	fn encode_key(&self, key_values: &[Value]) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		for value in key_values.iter() {
			serializer.extend_value(value);
		}
		EncodedKey::new(serializer.finish())
	}

	fn load_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<Option<Self::State>> {
		let key = self.encode_key(key_values);
		ctx.state().get::<Self::State>(&key)
	}

	fn save_state(&self, ctx: &mut OperatorContext, key_values: &[Value], value: &Self::State) -> Result<()> {
		let key = self.encode_key(key_values);
		ctx.state().set(&key, value)
	}

	fn remove_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<()> {
		let key = self.encode_key(key_values);
		ctx.state().remove(&key)
	}
}
