// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupStateKey, NODE_PREFIX_LEN, OperatorStateKey, extend_node_prefix},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorScope {
	pub operator: OperatorId,
	pub inner: EncodedKey,
}

pub fn scoped_key(id: OperatorId, key: &GroupStateKey) -> EncodedKey {
	let suffix = key.as_slice();
	let mut serializer = KeySerializer::with_capacity(NODE_PREFIX_LEN + suffix.len());
	extend_node_prefix(&mut serializer, id);
	serializer.extend_raw(suffix);
	serializer.finish()
}

pub fn operator_state_coordinates(key: &EncodedKey) -> Option<OperatorScope> {
	OperatorStateKey::decode_operator(key).map(|(operator, inner)| OperatorScope {
		operator,
		inner,
	})
}
