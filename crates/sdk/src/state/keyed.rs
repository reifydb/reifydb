// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::serializer::KeySerializer, state::OperatorState};
use reifydb_core::key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey};
use reifydb_value::value::{Value, value_type::ValueType};

use super::RawStatefulOperator;
use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi},
};

pub trait KeyedStateful: RawStatefulOperator {
	type State: OperatorState;

	fn key_types(&self) -> &[ValueType];

	fn encode_state_key(&self, key_values: &[Value]) -> StateKey {
		let mut serializer = KeySerializer::new();
		for value in key_values.iter() {
			serializer.extend_value(value);
		}
		OperatorStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::FIRST_CUSTOM,
			serializer.finish().as_ref().to_vec(),
		)
	}

	fn load_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value]) -> Result<Option<Self::State>> {
		let key = self.encode_state_key(key_values);
		ctx.state().get::<Self::State>(&key)
	}

	fn save_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value], value: &Self::State) -> Result<()> {
		let key = self.encode_state_key(key_values);
		ctx.state().set(&key, value)
	}

	fn remove_state(&self, ctx: &mut impl OperatorContext, key_values: &[Value]) -> Result<()> {
		let key = self.encode_state_key(key_values);
		ctx.state().remove(&key)
	}
}
