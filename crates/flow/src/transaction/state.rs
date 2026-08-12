// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupStateKey, node_prefix},
};

pub(crate) fn scoped_key(id: OperatorId, key: &GroupStateKey) -> EncodedKey {
	let mut bytes = node_prefix(id);
	bytes.extend_from_slice(key.as_slice());
	EncodedKey::new(bytes)
}

