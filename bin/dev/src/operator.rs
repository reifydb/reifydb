// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::core::key::operator_state::OperatorStateKey;

const UNFRAMED: &str = "<unframed>";
const UNDECODABLE: &str = "<undecodable>";

pub(crate) fn classify(key: &[u8]) -> (String, Option<u64>) {
	let Some((group, keyspace, _suffix)) = OperatorStateKey::decode_inner(key) else {
		return (UNDECODABLE.to_string(), None);
	};
	if !keyspace.is_known() {
		return (UNFRAMED.to_string(), None);
	}
	(keyspace.name().to_string(), Some(group.0))
}

pub(crate) fn shown(len: usize, top: usize) -> usize {
	if top == 0 {
		len
	} else {
		top.min(len)
	}
}

pub(crate) fn human(bytes: u64) -> String {
	const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
	let mut value = bytes as f64;
	let mut unit = 0;
	while value >= 1024.0 && unit + 1 < UNITS.len() {
		value /= 1024.0;
		unit += 1;
	}
	if unit == 0 {
		format!("{bytes} B")
	} else {
		format!("{value:.1} {}", UNITS[unit])
	}
}
