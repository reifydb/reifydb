// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	io::Write,
	sync::{Mutex, OnceLock},
	time::{Duration, Instant},
};

const DIGEST_INTERVAL: Duration = Duration::from_secs(2);

pub enum Event {
	FfiGet,
	FfiGetMany(usize),
	FfiPrefix(usize),
	FfiRange(usize),
	FfiSet,
	FfiRemove,
	FfiClear,
	FfiInternalGet,
	FfiInternalGetMany(usize),
	FfiInternalSet,
	FfiInternalRemove,
	CacheHit,
	CacheDirtyHit,
	CacheMiss,
}

#[derive(Default)]
struct Counters {
	ffi_get: u64,
	ffi_get_many: u64,
	ffi_get_many_rows: u64,
	ffi_prefix: u64,
	ffi_prefix_rows: u64,
	ffi_range: u64,
	ffi_range_rows: u64,
	ffi_set: u64,
	ffi_remove: u64,
	ffi_clear: u64,
	ffi_internal_get: u64,
	ffi_internal_get_many: u64,
	ffi_internal_get_many_rows: u64,
	ffi_internal_set: u64,
	ffi_internal_remove: u64,
	cache_hit: u64,
	cache_dirty_hit: u64,
	cache_miss: u64,
}

impl Counters {
	fn total(&self) -> u64 {
		self.ffi_get
			+ self.ffi_get_many + self.ffi_prefix
			+ self.ffi_range + self.ffi_set
			+ self.ffi_remove + self.ffi_clear
			+ self.ffi_internal_get
			+ self.ffi_internal_get_many
			+ self.ffi_internal_set
			+ self.ffi_internal_remove
			+ self.cache_hit + self.cache_dirty_hit
			+ self.cache_miss
	}

	fn apply(&mut self, event: Event) {
		match event {
			Event::FfiGet => self.ffi_get += 1,
			Event::FfiGetMany(rows) => {
				self.ffi_get_many += 1;
				self.ffi_get_many_rows += rows as u64;
			}
			Event::FfiPrefix(rows) => {
				self.ffi_prefix += 1;
				self.ffi_prefix_rows += rows as u64;
			}
			Event::FfiRange(rows) => {
				self.ffi_range += 1;
				self.ffi_range_rows += rows as u64;
			}
			Event::FfiSet => self.ffi_set += 1,
			Event::FfiRemove => self.ffi_remove += 1,
			Event::FfiClear => self.ffi_clear += 1,
			Event::FfiInternalGet => self.ffi_internal_get += 1,
			Event::FfiInternalGetMany(rows) => {
				self.ffi_internal_get_many += 1;
				self.ffi_internal_get_many_rows += rows as u64;
			}
			Event::FfiInternalSet => self.ffi_internal_set += 1,
			Event::FfiInternalRemove => self.ffi_internal_remove += 1,
			Event::CacheHit => self.cache_hit += 1,
			Event::CacheDirtyHit => self.cache_dirty_hit += 1,
			Event::CacheMiss => self.cache_miss += 1,
		}
	}
}

struct Registry {
	names: HashMap<u64, &'static str>,
	counters: HashMap<u64, Counters>,
	last_digest: Instant,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
	REGISTRY.get_or_init(|| {
		Mutex::new(Registry {
			names: HashMap::new(),
			counters: HashMap::new(),
			last_digest: Instant::now(),
		})
	})
}

pub fn register_name(operator_id: u64, name: &'static str) {
	let mut reg = registry().lock().unwrap();
	reg.names.entry(operator_id).or_insert(name);
}

pub fn record(operator_id: u64, event: Event) {
	let mut reg = registry().lock().unwrap();
	reg.counters.entry(operator_id).or_default().apply(event);

	if reg.last_digest.elapsed() < DIGEST_INTERVAL {
		return;
	}
	digest(&mut reg);
}

fn digest(reg: &mut Registry) {
	let elapsed = reg.last_digest.elapsed().as_secs_f64();

	let mut ids: Vec<u64> = reg.counters.keys().copied().collect();
	ids.sort_unstable();

	let mut buf = String::new();
	for id in ids {
		let c = &reg.counters[&id];
		if c.total() == 0 {
			continue;
		}
		let name = reg.names.get(&id).copied().unwrap_or("?");
		let from_mem = c.cache_hit + c.cache_dirty_hit;
		let logical = from_mem + c.cache_miss;
		let hitrate = if logical > 0 {
			format!("{:.1}%", 100.0 * from_mem as f64 / logical as f64)
		} else {
			"n/a".to_string()
		};
		buf.push_str(&format!(
			"[state-stats {:.1}s] op={}#{} | ffi get={} getmany={}(r{}) prefix={}(r{}) range={}(r{}) set={} rm={} clear={} | internal ig={} igm={}(r{}) is={} irm={} | cache hit={} dirty={} miss={} rate={}\n",
			elapsed,
			name,
			id,
			c.ffi_get,
			c.ffi_get_many,
			c.ffi_get_many_rows,
			c.ffi_prefix,
			c.ffi_prefix_rows,
			c.ffi_range,
			c.ffi_range_rows,
			c.ffi_set,
			c.ffi_remove,
			c.ffi_clear,
			c.ffi_internal_get,
			c.ffi_internal_get_many,
			c.ffi_internal_get_many_rows,
			c.ffi_internal_set,
			c.ffi_internal_remove,
			c.cache_hit,
			c.cache_dirty_hit,
			c.cache_miss,
			hitrate,
		));
	}

	if !buf.is_empty() {
		let mut out = std::io::stdout().lock();
		let _ = out.write_all(buf.as_bytes());
		let _ = out.flush();
	}

	reg.counters.clear();
	reg.last_digest = Instant::now();
}
