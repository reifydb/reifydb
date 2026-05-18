// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use dashmap::DashMap;
use once_cell::sync::Lazy;

static CALLSITE_NAMES: Lazy<DashMap<u64, &'static str>> = Lazy::new(DashMap::new);

pub fn register(callsite_id: u64, name: &'static str) {
	CALLSITE_NAMES.entry(callsite_id).or_insert(name);
}

pub fn resolve(callsite_id: u64) -> Option<&'static str> {
	CALLSITE_NAMES.get(&callsite_id).map(|r| *r.value())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn register_and_resolve_round_trip() {
		register(0xDEAD_BEEF, "test::span::name");
		assert_eq!(resolve(0xDEAD_BEEF), Some("test::span::name"));
	}

	#[test]
	fn unregistered_resolves_to_none() {
		assert_eq!(resolve(0xCAFE_BABE_C0DE_1234), None);
	}

	#[test]
	fn register_is_idempotent_first_write_wins() {
		register(0x4242, "first");
		register(0x4242, "second");
		assert_eq!(resolve(0x4242), Some("first"));
	}
}
