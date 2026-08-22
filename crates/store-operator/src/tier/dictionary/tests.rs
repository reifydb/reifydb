// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::dictionary::{DictionaryKey, OperatorDictionaryConfig, OperatorDictionaryTier, owns};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);

fn tier(limit: u64, shards: usize) -> OperatorDictionaryTier {
	OperatorDictionaryTier::new(OperatorDictionaryConfig {
		resident_bytes: Some(ByteSize::from_bytes(limit)),
		shards,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn roomy() -> OperatorDictionaryTier {
	tier(ByteSize::from_mib(1).as_bytes(), 1)
}

fn key(suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::GROUP_DICTIONARY, suffix).into_encoded()
}

fn foreign_key(group: GroupId, keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn fill(tier: &OperatorDictionaryTier, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
	assert!(tier.begin_fill(operator, key), "the fixture must be allowed to start the fill it is staging");
	assert!(tier.finish_fill(operator, key, Some(row)), "the fixture must be allowed to publish the fill");
}

#[test]
fn a_key_outside_the_root_group_is_declined() {
	let grouped = foreign_key(GroupId(7), Keyspace::GROUP_DICTIONARY, b"g");

	assert!(!owns(&grouped), "a non-root group must not be claimed by the dictionary tier");
	assert!(DictionaryKey::of(OP_A, &grouped).is_none(), "a non-root group must not decode into a dictionary key");

	let tier = roomy();
	tier.overwrite(OP_A, &grouped, row("v"));
	assert_eq!(tier.entries(), 0, "a declined key must never occupy a slot");
	assert!(tier.get(OP_A, &grouped).is_none(), "a declined key must never be served from this tier");
}

#[test]
fn a_key_outside_the_dictionary_keyspace_is_declined() {
	let other = foreign_key(GroupId::ROOT, Keyspace::ROW_NUMBER_MAPPING, b"g");

	assert!(!owns(&other), "a keyspace other than the dictionary must not be claimed");
	assert!(DictionaryKey::of(OP_A, &other).is_none(), "a foreign keyspace must not decode into a dictionary key");

	let tier = roomy();
	tier.overwrite(OP_A, &other, row("v"));
	assert_eq!(tier.entries(), 0, "a declined key must never occupy a slot");
}

#[test]
fn a_key_too_short_to_carry_a_keyspace_is_declined() {
	let stub = EncodedKey::new(vec![0xFFu8; OperatorStateKey::KEYSPACE_INNER_OFFSET as usize]);
	assert_eq!(
		&stub.as_slice()[..OperatorStateKey::KEYSPACE_INNER_OFFSET as usize],
		key(b"").as_slice()[..OperatorStateKey::KEYSPACE_INNER_OFFSET as usize].to_vec().as_slice(),
		"the stub must carry the root group, or the group check declines it and the length guard goes untested"
	);

	assert!(!owns(&stub), "a key with no keyspace byte must be declined rather than parsed past its end");
	assert!(DictionaryKey::of(OP_A, &stub).is_none(), "a truncated key must not decode into a dictionary key");

	let tier = roomy();
	tier.overwrite(OP_A, &stub, row("v"));
	assert_eq!(tier.entries(), 0, "a declined key must never occupy a slot");
}

#[test]
fn a_remembered_row_is_served_back_and_an_unknown_key_is_a_miss() {
	let tier = roomy();
	let known = key(b"known");
	let unknown = key(b"unknown");

	assert!(tier.get(OP_A, &known).is_none(), "nothing is known before the first fill");
	assert_eq!(tier.misses(), 1);

	fill(&tier, OP_A, &known, row("v"));

	let served = tier.get(OP_A, &known).expect("the tier knows the key it was just told about");
	assert_eq!(body(&served), "v", "the tier must hand back the row it was given");
	assert_eq!(tier.hits(), 1, "a served row must be counted as a hit");

	assert!(tier.get(OP_A, &unknown).is_none(), "an unfilled key must stay unknown");
	assert_eq!(tier.misses(), 2, "an unknown key must be counted as a miss");
	assert_eq!(tier.entries(), 1, "only the filled key occupies a slot");
}

#[test]
fn a_write_during_a_fill_makes_the_fill_install_nothing() {
	let tier = roomy();
	let k = key(b"raced");

	assert!(tier.begin_fill(OP_A, &k), "the fill that races the write must be allowed to start");
	tier.invalidate(OP_A, &k);

	assert!(!tier.finish_fill(OP_A, &k, Some(row("stale"))), "a fill the write dirtied must refuse to install");
	assert_eq!(tier.entries(), 0, "a dirtied fill must leave no entry, or the pre-write row is served forever");
	assert!(tier.get(OP_A, &k).is_none(), "the racing write must leave the key unknown, not stale");
	assert_eq!(tier.metrics().fills_dirty_aborted, 1, "the dirty abort must be visible in the metrics");

	fill(&tier, OP_A, &k, row("fresh"));
	assert_eq!(
		body(&tier.get(OP_A, &k).expect("an undisturbed fill installs")),
		"fresh",
		"a later undisturbed fill must still be allowed to install"
	);
}

#[test]
fn a_duplicate_fill_is_refused_and_an_aborted_fill_frees_the_registration() {
	let tier = roomy();
	let k = key(b"once");

	assert!(tier.begin_fill(OP_A, &k), "the first reader owns the fill");
	assert!(!tier.begin_fill(OP_A, &k), "a second reader must be refused so only one install can happen");
	assert_eq!(tier.metrics().fills_duplicate, 1);

	tier.abort_fill(OP_A, &k);
	assert!(tier.begin_fill(OP_A, &k), "an aborted fill must release the key for the next reader");
	assert!(tier.finish_fill(OP_A, &k, Some(row("v"))), "the re-registered fill must install");
	assert_eq!(tier.entries(), 1);
}

#[test]
fn eviction_over_budget_leaves_every_survivor_findable() {
	let tier = tier(3_000, 1);
	let count = 60usize;
	let keys: Vec<EncodedKey> = (0..count).map(|index| key(format!("g{index}").as_bytes())).collect();

	for (index, k) in keys.iter().enumerate() {
		tier.overwrite(OP_A, k, row(&format!("v{index}")));
	}

	assert!(tier.evictions() > 0, "the budget must actually have been exceeded, or this proves nothing");
	let resident = tier.entries();
	assert!(resident > 0, "eviction must not empty the tier");
	assert!(resident < count, "eviction must have dropped something");
	assert!(tier.index_is_consistent(), "every index position must still address its own slot");

	let mut found = 0usize;
	for (index, k) in keys.iter().enumerate() {
		let Some(served) = tier.get(OP_A, k) else {
			continue;
		};
		assert_eq!(body(&served), format!("v{index}"), "a survivor must serve its own row, not a neighbour's");
		found += 1;
	}
	assert_eq!(found, resident, "every resident slot must still be reachable through the index");
	assert!(
		tier.resident_bytes().as_bytes() <= tier.shard_limit_bytes().as_bytes(),
		"eviction must bring the shard back inside its budget"
	);
}

#[test]
fn invalidate_operator_drops_only_that_operator() {
	let tier = roomy();
	let shared = key(b"shared");
	let extra = key(b"extra");
	tier.overwrite(OP_A, &shared, row("a"));
	tier.overwrite(OP_A, &extra, row("a-extra"));
	tier.overwrite(OP_B, &shared, row("b"));
	assert_eq!(tier.entries(), 3, "the fixture must hold both operators before the drop");

	tier.invalidate_operator(OP_A);

	assert!(tier.get(OP_A, &shared).is_none(), "a dropped operator must leave no cached state behind");
	assert!(tier.get(OP_A, &extra).is_none(), "every key of the dropped operator must go");
	let survivor = tier.get(OP_B, &shared).expect("another operator's identical suffix must survive");
	assert_eq!(body(&survivor), "b", "operator scoping must not collect a namesake key");
	assert_eq!(tier.entries(), 1);
	assert!(tier.index_is_consistent(), "the rebuilt index must address the surviving slots");
}

#[test]
fn one_operators_dictionary_spreads_across_shards() {
	let tier = tier(ByteSize::from_mib(1).as_bytes(), 16);
	for index in 0..512 {
		let k = key(format!("g{index}").as_bytes());
		tier.overwrite(OP_A, &k, row("v"));
	}

	assert_eq!(tier.entries(), 512, "the fixture must fit without eviction so the spread is what is measured");
	assert!(
		tier.occupied_shards() > 8,
		"one operator's dictionary must land on many shards, not collapse onto a single one"
	);
}

#[test]
fn clear_empties_every_shard_and_releases_the_budget() {
	let tier = tier(ByteSize::from_mib(1).as_bytes(), 4);
	for index in 0..64 {
		tier.overwrite(OP_A, &key(format!("g{index}").as_bytes()), row("v"));
	}
	assert!(tier.resident_bytes().as_bytes() > 0);

	tier.clear();

	assert_eq!(tier.entries(), 0, "a clear must drop every slot");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a clear must release everything it charged");
	assert!(tier.index_is_consistent());
}
