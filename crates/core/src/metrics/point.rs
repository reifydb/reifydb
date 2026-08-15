// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::{Cell, RefCell},
	collections::{HashMap, HashSet, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
};

thread_local! {
	static GETS: Cell<u64> = const { Cell::new(0) };
}

pub fn record_get() {
	GETS.with(|c| c.set(c.get().wrapping_add(1)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointCounters {
	pub gets: u64,
}

impl PointCounters {
	pub fn sample() -> Self {
		Self {
			gets: GETS.with(|c| c.get()),
		}
	}

	pub fn since(self) -> Self {
		let now = Self::sample();
		Self {
			gets: now.gets.wrapping_sub(self.gets),
		}
	}
}

#[derive(Debug, Default, Clone, Copy)]
struct OperatorCensus {
	applies: u64,
	gets: u64,
	distinct: u64,
	group_touches: u64,
	persistent: u64,
	found: u64,
	widest_apply: u64,
}

thread_local! {
	static APPLY_KEYS: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
	static APPLY_GROUPS: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
	static APPLY_GETS: Cell<u64> = const { Cell::new(0) };
	static APPLY_PERSISTENT: Cell<u64> = const { Cell::new(0) };
	static APPLY_FOUND: Cell<u64> = const { Cell::new(0) };
	static CENSUS: RefCell<HashMap<u64, OperatorCensus>> = RefCell::new(HashMap::new());
	static KEYSPACE_GETS: RefCell<HashMap<(u64, u8), u64>> = RefCell::new(HashMap::new());
	static CENSUS_APPLIES: Cell<u64> = const { Cell::new(0) };
}

const CENSUS_DUMP_EVERY: u64 = 25_000;

const KEYSPACE_OFFSET: usize = 8;

pub fn census_get(operator: u64, key: &[u8], persistent: bool, found: bool) {
	let mut hasher = DefaultHasher::new();
	operator.hash(&mut hasher);
	key.hash(&mut hasher);
	let digest = hasher.finish();
	APPLY_KEYS.with(|set| set.borrow_mut().insert(digest));
	if key.len() > KEYSPACE_OFFSET {
		let mut group = DefaultHasher::new();
		operator.hash(&mut group);
		key[..KEYSPACE_OFFSET].hash(&mut group);
		APPLY_GROUPS.with(|set| set.borrow_mut().insert(group.finish()));
		KEYSPACE_GETS.with(|map| *map.borrow_mut().entry((operator, key[KEYSPACE_OFFSET])).or_insert(0) += 1);
	}
	APPLY_GETS.with(|c| c.set(c.get() + 1));
	if persistent {
		APPLY_PERSISTENT.with(|c| c.set(c.get() + 1));
	}
	if found {
		APPLY_FOUND.with(|c| c.set(c.get() + 1));
	}
}

pub fn census_begin_apply() {
	APPLY_KEYS.with(|set| set.borrow_mut().clear());
	APPLY_GROUPS.with(|set| set.borrow_mut().clear());
	APPLY_GETS.with(|c| c.set(0));
	APPLY_PERSISTENT.with(|c| c.set(0));
	APPLY_FOUND.with(|c| c.set(0));
}

pub fn census_end_apply(operator: u64) {
	let gets = APPLY_GETS.with(|c| c.get());
	if gets > 0 {
		let distinct = APPLY_KEYS.with(|set| set.borrow().len() as u64);
		let groups = APPLY_GROUPS.with(|set| set.borrow().len() as u64);
		let persistent = APPLY_PERSISTENT.with(|c| c.get());
		let found = APPLY_FOUND.with(|c| c.get());
		CENSUS.with(|census| {
			let mut census = census.borrow_mut();
			let entry = census.entry(operator).or_default();
			entry.applies += 1;
			entry.gets += gets;
			entry.distinct += distinct;
			entry.group_touches += groups;
			entry.persistent += persistent;
			entry.found += found;
			entry.widest_apply = entry.widest_apply.max(gets);
		});
	}

	let seen = CENSUS_APPLIES.with(|c| {
		c.set(c.get() + 1);
		c.get()
	});
	if seen % CENSUS_DUMP_EVERY == 0 {
		census_dump();
	}
}

pub fn census_dump() {
	let mut rows: Vec<(u64, OperatorCensus)> =
		CENSUS.with(|census| census.borrow().iter().map(|(id, c)| (*id, *c)).collect());
	rows.sort_by_key(|(_, c)| std::cmp::Reverse(c.gets));
	let total: u64 = rows.iter().map(|(_, c)| c.gets).sum();
	println!("[getcensus] operators={} gets={}", rows.len(), total);
	println!(
		"[getcensus] {:>6} {:>9} {:>12} {:>12} {:>7} {:>12} {:>8} {:>12} {:>7} {:>8} {:>8}",
		"op", "applies", "gets", "distinct", "repeat", "groups", "gets/grp", "persistent", "miss%", "gets/ap",
		"widest"
	);
	for (id, c) in rows.iter().take(30) {
		let repeat = if c.distinct == 0 {
			0.0
		} else {
			c.gets as f64 / c.distinct as f64
		};
		let per_group = if c.group_touches == 0 {
			0.0
		} else {
			c.gets as f64 / c.group_touches as f64
		};
		let miss = if c.persistent == 0 {
			0.0
		} else {
			(c.persistent - c.found.min(c.persistent)) as f64 * 100.0 / c.persistent as f64
		};
		println!(
			"[getcensus] {:>6} {:>9} {:>12} {:>12} {:>6.1}x {:>12} {:>7.2}x {:>12} {:>6.0}% {:>8} {:>8}",
			id,
			c.applies,
			c.gets,
			c.distinct,
			repeat,
			c.group_touches,
			per_group,
			c.persistent,
			miss,
			c.gets / c.applies.max(1),
			c.widest_apply
		);
	}

	let keyspaces: Vec<((u64, u8), u64)> =
		KEYSPACE_GETS.with(|map| map.borrow().iter().map(|(k, v)| (*k, *v)).collect());
	let mut by_operator: HashMap<u64, Vec<(u8, u64)>> = HashMap::new();
	for ((operator, keyspace), gets) in keyspaces {
		by_operator.entry(operator).or_default().push((keyspace, gets));
	}
	println!("[getkeyspace] operator keyspace_gets (tag=count, tags are stored inverted)");
	for (id, _) in rows.iter().take(12) {
		let Some(mut split) = by_operator.remove(id) else {
			continue;
		};
		split.sort_by_key(|(_, gets)| std::cmp::Reverse(*gets));
		let rendered: Vec<String> =
			split.iter().take(10).map(|(tag, gets)| format!("{:02X}={}", tag, gets)).collect();
		println!("[getkeyspace] {:>6}  {}", id, rendered.join(" "));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn since_reports_only_what_the_caller_bracketed() {
		// Reading the absolute counter would bill an apply for every get that ran before it.
		record_get();
		let before = PointCounters::sample();
		record_get();
		record_get();

		assert_eq!(before.since().gets, 2, "gets issued before the bracket must not be attributed to it");
	}

	#[test]
	fn a_bracket_with_no_get_reports_nothing() {
		// An apply that reads nothing must report zero, never the thread's running total.
		record_get();
		let before = PointCounters::sample();

		assert_eq!(
			before.since(),
			PointCounters {
				gets: 0
			}
		);
	}

	#[test]
	fn a_repeated_key_counts_once_as_distinct() {
		// A repeat that inflated distinct would hide exactly the redundancy the census looks for.
		census_begin_apply();
		census_get(7, b"same", true, true);
		census_get(7, b"same", true, true);
		census_get(7, b"other", true, false);

		let gets = APPLY_GETS.with(|c| c.get());
		let distinct = APPLY_KEYS.with(|set| set.borrow().len() as u64);
		assert_eq!(gets, 3);
		assert_eq!(distinct, 2, "the same key read twice must not inflate the distinct count");
	}
}
