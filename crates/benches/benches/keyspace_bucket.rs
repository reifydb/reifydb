// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{GlobalAlloc, Layout},
	collections::BTreeMap,
	hint::black_box,
	sync::atomic::{AtomicU64, Ordering::Relaxed},
	time::{Duration, Instant},
};

use reifydb_allocator::backend::ALLOCATOR as BACKEND;
use reifydb_benches::{BenchReport, env_list_usize, env_usize};
use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{
				KEYSPACES, KeyspaceVisitor, columns_width, dispatch, join::JoinLeft, timer::TimerIndex,
				window::GuestBuffer,
			},
			state::GroupId,
			traits::Keyspace,
		},
		typed::layout::KeyLayout,
	},
	state::typed::SuffixBytes,
};
use reifydb_store_operator::resident::bucket::write::StandardBucket;

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards its arguments unchanged to jemalloc, which upholds the GlobalAlloc contract.
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Relaxed);
		// SAFETY: the caller's layout guarantees are passed to jemalloc unchanged.
		unsafe { BACKEND.alloc(layout) }
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Relaxed);
		// SAFETY: the caller's layout guarantees are passed to jemalloc unchanged.
		unsafe { BACKEND.alloc_zeroed(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		// SAFETY: ptr was returned by this allocator for exactly this layout, as the caller guarantees.
		unsafe { BACKEND.dealloc(ptr, layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Relaxed);
		// SAFETY: ptr, layout and new_size satisfy the realloc contract, as the caller guarantees.
		unsafe { BACKEND.realloc(ptr, layout, new_size) }
	}
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const INLINE: usize = 64;
const DEFAULT_REPEATS: usize = 5;

struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
}

fn suffixes<K: Keyspace>(count: usize, seed: u64) -> Vec<K::Suffix> {
	let width = columns_width(<K::Suffix as KeyLayout>::COLUMNS);
	let mut rng = Rng(seed);
	let mut bytes = vec![0u8; width];
	let mut out = Vec::with_capacity(count);
	let mut attempts = 0usize;
	while out.len() < count {
		attempts += 1;
		assert!(attempts < count * 64 + 64, "{} rejects random suffix bytes", K::NAME);
		for chunk in bytes.chunks_mut(8) {
			let word = rng.next().to_be_bytes();
			chunk.copy_from_slice(&word[..chunk.len()]);
		}
		if let Some(suffix) = <K::Suffix as SuffixBytes>::from_suffix_bytes(&bytes) {
			out.push(suffix);
		}
	}
	out
}

fn inline(bytes: &[u8]) -> [u8; INLINE] {
	let mut out = [0u8; INLINE];
	out[..bytes.len()].copy_from_slice(bytes);
	out
}

struct Sample {
	elapsed: Duration,
	allocations: u64,
}

fn measure(run: impl FnOnce()) -> Sample {
	let allocations = ALLOCATIONS.load(Relaxed);
	let start = Instant::now();
	run();
	Sample {
		elapsed: start.elapsed(),
		allocations: ALLOCATIONS.load(Relaxed) - allocations,
	}
}

fn median(mut samples: Vec<Sample>) -> Sample {
	samples.sort_by_key(|sample| sample.elapsed);
	samples.swap_remove(samples.len() / 2)
}

#[allow(clippy::disallowed_types)]
fn report_line(report: &mut BenchReport, label: &str, keys: usize, sample: &Sample) {
	let line_label = format!(
		"{label} ns_per_op={:.1} allocs_per_op={:.2}",
		sample.elapsed.as_nanos() as f64 / keys as f64,
		sample.allocations as f64 / keys as f64
	);
	report.record_throughput(&line_label, keys as u64, sample.elapsed);
}

#[allow(clippy::disallowed_types)]
fn bench_keyspace<K: Keyspace>(report: &mut BenchReport, keys: usize, repeats: usize) {
	let operator = OperatorId(1);
	let group = GroupId::ROOT;
	let row = EncodedPodRow::new(&[0u8; 16]);
	let inserts = suffixes::<K>(keys, 0x9E37_79B9_7F4A_7C15);
	let mut lookups = inserts.clone();
	lookups.reverse();
	let encoded: Vec<Vec<u8>> = inserts.iter().map(SuffixBytes::to_suffix_bytes).collect();
	let encoded_lookups: Vec<Vec<u8>> = lookups.iter().map(SuffixBytes::to_suffix_bytes).collect();
	let prefix = format!("{} width={} keys={keys}", K::NAME, columns_width(<K::Suffix as KeyLayout>::COLUMNS));

	let phase = |report: &mut BenchReport, name: &str, mut run: Box<dyn FnMut() -> Sample + '_>| {
		let samples = (0..repeats).map(|_| run()).collect();
		report_line(report, &format!("{prefix} {name}"), keys, &median(samples));
	};

	phase(
		report,
		"today_bucket_record",
		Box::new(|| {
			let mut bucket = StandardBucket::<K>::new(operator);
			let sample = measure(|| {
				for suffix in &inserts {
					bucket.record(group, suffix.clone(), Some(row.clone()));
				}
			});
			black_box(bucket.len());
			sample
		}),
	);

	let mut bucket = StandardBucket::<K>::new(operator);
	for suffix in &inserts {
		bucket.record(group, suffix.clone(), Some(row.clone()));
	}
	phase(
		report,
		"today_bucket_get",
		Box::new(|| {
			measure(|| {
				for suffix in &lookups {
					black_box(bucket.get(group, suffix));
				}
			})
		}),
	);
	phase(
		report,
		"today_bucket_scan",
		Box::new(|| {
			measure(|| {
				black_box(bucket.range(group, ..).count());
			})
		}),
	);

	phase(
		report,
		"typed_map_insert",
		Box::new(|| {
			let mut map = BTreeMap::new();
			let sample = measure(|| {
				for suffix in &inserts {
					map.insert(suffix.clone(), row.clone());
				}
			});
			black_box(map.len());
			sample
		}),
	);
	let typed: BTreeMap<K::Suffix, EncodedPodRow> = inserts.iter().map(|s| (s.clone(), row.clone())).collect();
	phase(
		report,
		"typed_map_get",
		Box::new(|| {
			measure(|| {
				for suffix in &lookups {
					black_box(typed.get(suffix));
				}
			})
		}),
	);

	phase(
		report,
		"vec_map_encode_insert",
		Box::new(|| {
			let mut map = BTreeMap::new();
			let sample = measure(|| {
				for suffix in &inserts {
					map.insert(suffix.to_suffix_bytes(), row.clone());
				}
			});
			black_box(map.len());
			sample
		}),
	);
	let bytes_map: BTreeMap<Vec<u8>, EncodedPodRow> = encoded.iter().map(|b| (b.clone(), row.clone())).collect();
	phase(
		report,
		"vec_map_encode_get",
		Box::new(|| {
			measure(|| {
				for suffix in &lookups {
					black_box(bytes_map.get(&suffix.to_suffix_bytes()));
				}
			})
		}),
	);

	phase(
		report,
		"inline_map_insert_preencoded",
		Box::new(|| {
			let mut map = BTreeMap::new();
			let sample = measure(|| {
				for bytes in &encoded {
					map.insert(inline(bytes), row.clone());
				}
			});
			black_box(map.len());
			sample
		}),
	);
	let inline_map: BTreeMap<[u8; INLINE], EncodedPodRow> =
		encoded.iter().map(|b| (inline(b), row.clone())).collect();
	phase(
		report,
		"inline_map_get_preencoded",
		Box::new(|| {
			measure(|| {
				for bytes in &encoded_lookups {
					black_box(inline_map.get(&inline(bytes)));
				}
			})
		}),
	);

	phase(
		report,
		"encode_only",
		Box::new(|| {
			measure(|| {
				for suffix in &inserts {
					black_box(suffix.to_suffix_bytes());
				}
			})
		}),
	);
}

struct OrderCheck {
	pairs: usize,
}

impl KeyspaceVisitor for OrderCheck {
	type Output = usize;

	fn visit<K: Keyspace>(self) -> usize {
		if columns_width(<K::Suffix as KeyLayout>::COLUMNS) == 0 {
			return 0;
		}
		let left = suffixes::<K>(self.pairs, 0xA076_1D64_78BD_642F);
		let right = suffixes::<K>(self.pairs, 0xE703_7ED1_A0B4_28DB);
		for (a, b) in left.iter().zip(&right) {
			assert_eq!(
				a.cmp(b),
				a.to_suffix_bytes().cmp(&b.to_suffix_bytes()),
				"{}: byte order differs from typed order for {a:?} vs {b:?}",
				K::NAME
			);
		}
		self.pairs
	}
}

fn main() {
	let sizes = env_list_usize("KEYS", &[10_000, 100_000]);
	let repeats = env_usize("REPEATS", DEFAULT_REPEATS);
	let widest = KEYSPACES.iter().map(|spec| spec.suffix_width()).max().unwrap_or(0);
	assert!(widest <= INLINE, "widest suffix is {widest} bytes, inline key holds {INLINE}");

	let checked: usize = KEYSPACES
		.iter()
		.map(|spec| {
			dispatch(
				spec.id,
				OrderCheck {
					pairs: 10_000,
				},
			)
			.expect("catalogued")
		})
		.sum();
	println!("order check: {} keyspaces, {checked} pairs, widest suffix {widest} bytes", KEYSPACES.len());

	let mut report = BenchReport::new("keyspace_bucket");
	for keys in sizes {
		bench_keyspace::<JoinLeft>(&mut report, keys, repeats);
		bench_keyspace::<GuestBuffer>(&mut report, keys, repeats);
		bench_keyspace::<TimerIndex>(&mut report, keys, repeats);
	}
	report.save();
}
