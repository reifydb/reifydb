// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{encoded::EncodedKey, serializer::KeySerializer},
	state::OperatorState,
};
use reifydb_core::key::operator_group_state::{GroupStateKey, Keyspace, OperatorGroupStateKey};
use reifydb_value::value::{Value, value_type::ValueType};

use super::RawStatefulOperator;
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait KeyedStateful: RawStatefulOperator {
	type State: OperatorState;

	fn key_types(&self) -> &[ValueType];

	fn encode_state_key(&self, ctx: &mut impl OperatorContext, key_values: &[Value]) -> Result<GroupStateKey> {
		let mut serializer = KeySerializer::new();
		for value in key_values.iter() {
			serializer.extend_value(value);
		}
		let group = ctx.intern_group(&EncodedKey::new(serializer.finish().as_ref()))?;
		Ok(OperatorGroupStateKey::inner_encoded(group, Keyspace::FIRST_CUSTOM, []))
	}

	fn load_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value]) -> Result<Option<Self::State>> {
		let key = self.encode_state_key(ctx, key_values)?;
		ctx.state().get::<Self::State>(&key)
	}

	fn save_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value], value: &Self::State) -> Result<()> {
		let key = self.encode_state_key(ctx, key_values)?;
		ctx.state().set(&key, value)
	}

	fn remove_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value]) -> Result<()> {
		let key = self.encode_state_key(ctx, key_values)?;
		ctx.state().remove(&key)
	}
}
