// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::{
	Bound,
	Bound::{Excluded, Included, Unbounded},
};

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		kind::KeyKind,
		operator::state::{GroupStateKey, NODE_PREFIX_LEN, OperatorStateKey, extend_node_prefix, node_prefix},
	},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorScope {
	pub operator: OperatorId,
	pub inner: EncodedKey,
}

#[derive(Debug, Clone)]
pub struct OperatorRangeScope {
	pub operator: OperatorId,
	pub inner: EncodedKeyRange,
}

pub(crate) fn scoped_key(id: OperatorId, key: &GroupStateKey) -> EncodedKey {
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

pub(crate) fn operator_state_scope(range: &EncodedKeyRange) -> Option<OperatorRangeScope> {
	let start_key = match range.start.as_ref() {
		Included(key) | Excluded(key) => key,
		Unbounded => return None,
	};
	if KeyKind::of(start_key) != Some(KeyKind::OperatorState) {
		return None;
	}
	let operator = operator_state_coordinates(start_key)
		.expect("an OperatorState-routed key must carry an operator id")
		.operator;
	let prefix = EncodedKey::new(node_prefix(operator));
	let strip = |bound: Bound<&EncodedKey>| match bound {
		Included(key) if key.as_slice().starts_with(prefix.as_slice()) => {
			Included(EncodedKey::new(&key.as_slice()[prefix.len()..]))
		}
		Excluded(key) if key.as_slice().starts_with(prefix.as_slice()) => {
			Excluded(EncodedKey::new(&key.as_slice()[prefix.len()..]))
		}
		_ => Unbounded,
	};
	Some(OperatorRangeScope {
		operator,
		inner: EncodedKeyRange::new(strip(range.start.as_ref()), strip(range.end.as_ref())),
	})
}
