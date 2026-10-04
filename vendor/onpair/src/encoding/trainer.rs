// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! CompactDictionary training: discover merge tokens by a frequency-threshold scan,
//! then sort the dictionary lexicographically.
//!
//! The first 256 tokens are always the single-byte values `0x00..=0xFF`
//! (completeness); subsequent tokens are pair merges discovered during the
//! scan. The result is sorted by token byte sequence with ids reassigned to
//! match — see the [dictionary invariants](crate::CompactDictionary).

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use rand::SeedableRng;
use rand::seq::SliceRandom;

use crate::core::dictionary::{CompactDictionary, Dictionary, pad_raw};
use crate::core::types::MAX_TOKEN_SIZE;
use crate::encoding::config::{ThresholdSpec, TrainingConfig};
use crate::encoding::lpm::LongestPrefixMatcher;
use crate::encoding::rows::Rows;

#[inline(always)]
fn hash_pair(key: u32) -> u64 {
    let hash = u64::from(key).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    hash ^ (hash >> 32)
}

struct PairFrequencies {
    table: HashTable<(u32, u8)>,
}

impl PairFrequencies {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            table: HashTable::with_capacity(capacity),
        }
    }

    #[inline(always)]
    fn increment(&mut self, key: u32) -> u8 {
        let hash = hash_pair(key);
        match self.table.entry(
            hash,
            |(candidate, _)| *candidate == key,
            |(candidate, _)| hash_pair(*candidate),
        ) {
            Entry::Occupied(entry) => {
                let frequency = &mut entry.into_mut().1;
                *frequency = frequency.saturating_add(1);
                *frequency
            }
            Entry::Vacant(entry) => {
                entry.insert((key, 1));
                1
            }
        }
    }

    #[inline]
    fn remove(&mut self, key: u32) {
        if let Ok(entry) = self
            .table
            .find_entry(hash_pair(key), |(candidate, _)| *candidate == key)
        {
            entry.remove();
        }
    }
}

/// Result of [`train`]: a sorted dictionary and a matching matcher whose token
/// ids correspond to the dictionary's sorted order. The dictionary is **not**
/// yet read-padded — the parser pads it when it builds a column.
#[derive(Debug, Clone)]
pub(crate) struct TrainResult {
    pub(crate) dict: CompactDictionary,
    pub(crate) lpm: LongestPrefixMatcher,
}

/// Largest dictionary size for a training budget: `2^max_dict_bits`.
///
/// `max_dict_bits` is validated as `9..=16` at the public boundary by
/// [`crate::MaxDictBits`].
#[inline]
const fn max_dict_size(max_dict_bits: u8) -> usize {
    1usize << max_dict_bits
}

// ─────────────────────────────────────────────────────────────────────────────
// DynamicThresholdController — adaptive merge threshold.
// ─────────────────────────────────────────────────────────────────────────────

struct DynamicThresholdController {
    capacity: usize,
    scan_budget: usize,
    check_interval: usize,
    threshold: u8,
    entries_created: usize,
    bytes_scanned: usize,
    entries_at_check: usize,
    bytes_at_check: usize,
    next_checkpoint: usize,
}

impl DynamicThresholdController {
    fn new(capacity: usize, total_bytes: usize, scan_fraction: f64) -> Self {
        let scan_budget = (total_bytes as f64 * scan_fraction) as usize;
        let check_interval = (capacity / 128).max(64);
        Self {
            capacity,
            scan_budget,
            check_interval,
            threshold: 2,
            entries_created: 0,
            bytes_scanned: 0,
            entries_at_check: 0,
            bytes_at_check: 0,
            next_checkpoint: check_interval,
        }
    }

    #[inline]
    fn get(&self) -> u8 {
        self.threshold
    }

    #[inline]
    fn budget_exhausted(&self) -> bool {
        self.bytes_scanned > self.scan_budget
    }

    #[inline]
    fn on_bytes_scanned(&mut self, n: usize) {
        self.bytes_scanned += n;
    }

    fn on_entry_created(&mut self) {
        self.entries_created += 1;
        if self.entries_created >= self.next_checkpoint {
            self.rebalance();
        }
    }

    fn rebalance(&mut self) {
        let delta_e = self.entries_created - self.entries_at_check;
        let delta_b = self.bytes_scanned - self.bytes_at_check;

        let recent_rate = if delta_b > 0 {
            delta_e as f64 / delta_b as f64
        } else {
            1e9
        };

        let e_rem = if self.capacity > self.entries_created {
            self.capacity - self.entries_created
        } else {
            1
        };
        let b_rem = if self.scan_budget > self.bytes_scanned {
            self.scan_budget - self.bytes_scanned
        } else {
            1
        };

        let target_rate = e_rem as f64 / b_rem as f64;
        let ratio = if target_rate > 0.0 {
            recent_rate / target_rate
        } else {
            1e9
        };

        if ratio > 2.0 && self.threshold < 255 {
            self.threshold += 1;
        } else if ratio < 0.5 && self.threshold > 2 {
            self.threshold -= 1;
        }

        self.entries_at_check = self.entries_created;
        self.bytes_at_check = self.bytes_scanned;
        self.next_checkpoint = self.entries_created + self.check_interval;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// train()
// ─────────────────────────────────────────────────────────────────────────────

fn scan_budget(threshold: ThresholdSpec, total_bytes: usize) -> Option<usize> {
    match threshold {
        ThresholdSpec::Dynamic(dt) => Some((total_bytes as f64 * dt.sample_fraction) as usize),
        ThresholdSpec::Fixed(_) => None,
    }
}

/// Build the randomized row order consumed by training and return the first
/// selected position. Dynamic training only partially shuffles the row ids: the
/// selected, randomly permuted rows occupy `order[selected_start..]` and are the
/// only rows the byte-budgeted scan consumes. If unusually skewed row lengths
/// leave that selection short of the byte budget, retry with a larger partial
/// sample instead of falling through into the unshuffled remainder.
fn make_training_order<R: Rows + ?Sized>(
    rows: &R,
    threshold: ThresholdSpec,
    total_bytes: usize,
    seed: u64,
) -> (Vec<u32>, usize) {
    let n = rows.num_rows();
    let scan_budget = scan_budget(threshold, total_bytes);
    let mut shuffle_k = match threshold {
        ThresholdSpec::Dynamic(dt) => {
            (((dt.sample_fraction * 2.0).min(1.0) * n as f64) as usize + 1024).min(n)
        }
        ThresholdSpec::Fixed(_) => n,
    };

    loop {
        let mut order: Vec<u32> = (0..n as u32).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        {
            // `partial_shuffle` returns the selected, randomly permuted slice
            // first. rand stores that slice in the final `shuffle_k` positions.
            let (selected, _) = order.partial_shuffle(&mut rng, shuffle_k);
            debug_assert_eq!(selected.len(), shuffle_k);
        }
        let selected_start = n - shuffle_k;

        let Some(scan_budget) = scan_budget else {
            return (order, selected_start);
        };
        let selected_bytes = order[selected_start..]
            .iter()
            .map(|&idx| rows.row(idx as usize).len())
            .sum::<usize>();
        if selected_bytes > scan_budget || shuffle_k == n {
            return (order, selected_start);
        }

        // Rare guard for highly skewed row sizes. Normal dynamic training
        // selects roughly twice the expected row count and returns on the first
        // pass, retaining O(shuffle_k) random-swap work.
        shuffle_k = shuffle_k.saturating_mul(2).min(n);
    }
}

fn selected_rows<'a, R: Rows + ?Sized>(
    rows: &'a R,
    order: &'a [u32],
) -> impl ExactSizeIterator<Item = &'a [u8]> + 'a {
    order.iter().map(move |&idx| rows.row(idx as usize))
}

struct GatheredSample {
    bytes: Vec<u8>,
    offsets: Vec<usize>,
}

impl GatheredSample {
    fn rows(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.offsets
            .windows(2)
            .map(|bounds| &self.bytes[bounds[0]..bounds[1]])
    }
}

/// Copy a budgeted random sample into scan order for sequential access.
fn gather_sample<'a>(
    rows: impl ExactSizeIterator<Item = &'a [u8]>,
    budget: usize,
) -> GatheredSample {
    let mut bytes = Vec::with_capacity(budget);
    let mut offsets = Vec::with_capacity(rows.len().min(1024) + 1);
    offsets.push(0);
    for row in rows {
        if bytes.len() > budget {
            break;
        }
        let len = row.len();
        if bytes.len() + len > bytes.capacity() {
            bytes.reserve_exact(len);
        }
        bytes.extend_from_slice(row);
        offsets.push(bytes.len());
    }
    GatheredSample { bytes, offsets }
}

/// Discover merge tokens via frequency-threshold scanning, then sort the
/// dictionary lexicographically. The caller guarantees `cfg.max_dict_bits` is in
/// `9..=16`.
pub(crate) fn train<R: Rows + ?Sized>(rows: &R, cfg: &TrainingConfig) -> TrainResult {
    let total_bytes = rows.total_bytes();
    let seed = cfg.seed.unwrap_or_else(|| {
        use rand::Rng;
        rand::rng().random()
    });
    let (order, selected_start) = make_training_order(rows, cfg.threshold, total_bytes, seed);
    let selected_order = &order[selected_start..];

    if let Some(budget) =
        scan_budget(cfg.threshold, total_bytes).filter(|&budget| budget <= total_bytes / 2)
    {
        let sample = gather_sample(selected_rows(rows, selected_order), budget);
        discover_tokens(sample.rows(), cfg, total_bytes)
    } else {
        discover_tokens(selected_rows(rows, selected_order), cfg, total_bytes)
    }
}

fn discover_tokens<'a>(
    rows: impl Iterator<Item = &'a [u8]>,
    cfg: &TrainingConfig,
    total_bytes: usize,
) -> TrainResult {
    let dict_capacity = max_dict_size(cfg.max_dict_bits);

    // Accumulate into local buffers, then seal once into a trusted dictionary at
    // the end — a move, no copy. The dictionary is never viewed mid-build (the
    // matcher, not the dictionary, drives matching).
    let mut dict_bytes: Vec<u8> = Vec::with_capacity(dict_capacity * MAX_TOKEN_SIZE);
    let mut dict_offsets: Vec<u32> = Vec::with_capacity(dict_capacity + 1);
    dict_offsets.push(0);
    for i in 0u16..=255 {
        dict_bytes.push(i as u8);
        dict_offsets.push(dict_bytes.len() as u32);
    }
    let mut lpm = LongestPrefixMatcher::new();
    lpm.reserve(dict_capacity);

    let mut threshold: u8;
    let mut dyn_ctrl: Option<DynamicThresholdController> = None;
    match cfg.threshold {
        ThresholdSpec::Fixed(ft) => {
            threshold = ft.value;
        }
        ThresholdSpec::Dynamic(dt) => {
            let capacity = dict_capacity - 256;
            let ctrl = DynamicThresholdController::new(capacity, total_bytes, dt.sample_fraction);
            threshold = ctrl.get();
            dyn_ctrl = Some(ctrl);
        }
    }

    // Pair frequency map. Key packs two Token values into a u32.
    let pair_capacity = (dict_capacity * 8)
        .max(1024)
        .min((total_bytes / 2).max(1024));
    let mut freq = PairFrequencies::with_capacity(pair_capacity);

    let mut full_dictionary = false;
    let mut budget_exhausted = false;

    for row in rows {
        if full_dictionary || budget_exhausted {
            break;
        }

        if row.is_empty() {
            continue;
        }

        let (mut prev_id, mut prev_len) = lpm.find_longest_match(row);
        let mut pos = prev_len;

        if let Some(ref mut dyn_) = dyn_ctrl {
            dyn_.on_bytes_scanned(prev_len);
            budget_exhausted = dyn_.budget_exhausted();
            if budget_exhausted {
                break;
            }
        }

        while pos < row.len() {
            let (curr_id, curr_len) = lpm.find_longest_match(&row[pos..]);

            if let Some(ref mut dyn_) = dyn_ctrl {
                dyn_.on_bytes_scanned(curr_len);
                budget_exhausted = dyn_.budget_exhausted();
                if budget_exhausted {
                    break;
                }
            }

            let pair_len = prev_len + curr_len;

            if pair_len <= MAX_TOKEN_SIZE {
                let key = ((prev_id as u32) << 16) | (curr_id as u32);
                if freq.increment(key) >= threshold {
                    let pair = &row[pos - prev_len..pos + curr_len];
                    let new_id = lpm.insert(pair);
                    dict_bytes.extend_from_slice(pair);
                    dict_offsets.push(dict_bytes.len() as u32);

                    if lpm.size() == dict_capacity {
                        full_dictionary = true;
                        break;
                    }

                    if let Some(ref mut dyn_) = dyn_ctrl {
                        dyn_.on_entry_created();
                        threshold = dyn_.get();
                    }

                    freq.remove(key);
                    prev_id = new_id;
                    prev_len = pair_len;
                    pos += curr_len;
                    continue;
                }
            }

            prev_id = curr_id;
            prev_len = curr_len;
            pos += curr_len;
        }
    }

    // Sort the tokens into final order, pad, and seal exactly once: the only
    // `CompactDictionary` that exists is sorted and read-padded by construction.
    // The merge-loop matcher used unsorted ids, so rebuild it from the sealed dict.
    let (mut bytes, offsets) = sort_tokens(&dict_bytes, &dict_offsets);
    pad_raw(&mut bytes, &offsets);
    let dict = CompactDictionary::from_raw(bytes, offsets);
    let lpm = LongestPrefixMatcher::from_dictionary(dict.as_view());
    TrainResult { dict, lpm }
}

/// Sort the tokens into ascending bytewise-lexicographic order, returning fresh
/// `(bytes, offsets)` with ids reassigned to sorted position. Reads tokens
/// straight from the raw buffers; the result is unpadded — the caller pads before
/// sealing.
fn sort_tokens(bytes: &[u8], offsets: &[u32]) -> (Vec<u8>, Vec<u32>) {
    let n = offsets.len() - 1;
    let token = |id: usize| -> &[u8] { &bytes[offsets[id] as usize..offsets[id + 1] as usize] };

    let mut perm: Vec<usize> = (0..n).collect();
    perm.sort_by(|&a, &b| token(a).cmp(token(b)));

    let mut out_bytes: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut out_offsets: Vec<u32> = Vec::with_capacity(n + 1);
    out_offsets.push(0);
    for &old in &perm {
        out_bytes.extend_from_slice(token(old));
        out_offsets.push(out_bytes.len() as u32);
    }
    (out_bytes, out_offsets)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — adapted from `tests/encoding/test_trainer.cpp`.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::core::dictionary::{CompactDictionaryView, DictionaryView};
    use crate::core::types::Token;
    use crate::encoding::config::{DynamicThreshold, FixedThreshold};
    use crate::encoding::rows::ArrowRows;
    use crate::test_corpus::{
        alternating_strings as make_alternating_strings, binary_strings as make_binary_strings,
        fixed_length_strings as make_fixed_length_strings,
        homogeneous_strings as make_homogeneous_strings, make_raw,
        mixed_length_strings as make_mixed_length_strings,
        random_ascii_strings as make_random_strings, user_strings as make_user_strings,
    };

    fn train_strings<S: AsRef<[u8]>>(strings: &[S], cfg: &TrainingConfig) -> TrainResult {
        let raw = make_raw(strings);
        train(&ArrowRows::new(&raw.data, &raw.offsets), cfg)
    }

    fn check_base_tokens(d: CompactDictionaryView<'_>) {
        assert!(d.num_tokens() >= 256);
        let mut found = [false; 256];
        for i in 0..d.num_tokens() {
            let t = d.token(i as Token);
            if t.len() == 1 {
                found[t[0] as usize] = true;
            }
        }
        for (i, &f) in found.iter().enumerate() {
            assert!(f, "base token for byte {i} not found in dictionary");
        }
    }

    fn is_lex_sorted(d: CompactDictionaryView<'_>) -> bool {
        (1..d.num_tokens()).all(|i| d.token((i - 1) as Token) <= d.token(i as Token))
    }

    #[test]
    fn base_tokens_always_single_bytes() {
        let result = train_strings(&make_user_strings(50), &TrainingConfig::default());
        check_base_tokens(result.dict.as_view());
    }

    #[test]
    fn base_tokens_on_empty_input() {
        let result = train(&ArrowRows::new(&[], &[0u32]), &TrainingConfig::default());
        check_base_tokens(result.dict.as_view());
        assert_eq!(result.dict.num_tokens(), 256);
    }

    #[test]
    fn base_tokens_on_single_empty_string() {
        let result = train(&ArrowRows::new(&[], &[0u32, 0]), &TrainingConfig::default());
        check_base_tokens(result.dict.as_view());
        assert_eq!(result.dict.num_tokens(), 256);
    }

    #[test]
    fn dictionary_size_does_not_exceed_capacity() {
        let cfg = TrainingConfig {
            max_dict_bits: 12,
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
        };
        let result = train_strings(&make_user_strings(500), &cfg);
        assert!(result.dict.num_tokens() <= max_dict_size(cfg.max_dict_bits));
    }

    #[test]
    fn threshold_gates_merges() {
        let corpus: Vec<&str> = (0..100).map(|_| "ab").collect();

        let cfg_low = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        assert!(train_strings(&corpus, &cfg_low).dict.num_tokens() > 256);

        let cfg_high = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 101 }),
            seed: Some(42),
            ..Default::default()
        };
        assert_eq!(train_strings(&corpus, &cfg_high).dict.num_tokens(), 256);
    }

    #[test]
    fn merged_token_content_is_correct() {
        let corpus: Vec<&str> = (0..50).map(|_| "ab").collect();
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        let result = train_strings(&corpus, &cfg);
        let view = result.dict.as_view();
        let found = (0..view.num_tokens()).any(|i| view.token(i as Token) == b"ab");
        assert!(found, "merged token \"ab\" not found in dictionary");
    }

    #[test]
    fn same_seed_produces_identical_dictionaries() {
        let corpus = make_random_strings(100, 40, 12345);
        let cfg = TrainingConfig {
            seed: Some(42),
            ..Default::default()
        };
        let r1 = train_strings(&corpus, &cfg);
        let r2 = train_strings(&corpus, &cfg);
        assert_eq!(r1.dict.bytes(), r2.dict.bytes());
        assert_eq!(r1.dict.offsets(), r2.dict.offsets());
    }

    #[test]
    fn dictionary_is_always_sorted() {
        let result = train_strings(&make_user_strings(100), &TrainingConfig::default());
        assert!(is_lex_sorted(result.dict.as_view()));
    }

    #[test]
    fn lpm_remaps_correctly() {
        let result = train_strings(&make_user_strings(30), &TrainingConfig::default());
        let view = result.dict.as_view();
        let n = view.num_tokens();
        for id in 0..n {
            let bytes = view.token(id as Token);
            let (tok, len) = result.lpm.find_longest_match(bytes);
            assert_eq!(tok, id as Token, "id mismatch for token {id}");
            assert_eq!(len, bytes.len(), "length mismatch for token {id}");
        }
    }

    #[test]
    fn no_token_exceeds_max_token_size() {
        let result = train_strings(
            &make_random_strings(100, 50, 99),
            &TrainingConfig::default(),
        );
        let view = result.dict.as_view();
        for i in 0..view.num_tokens() {
            assert!(
                view.token(i as Token).len() <= MAX_TOKEN_SIZE,
                "token {i} too large"
            );
        }
    }

    #[test]
    fn no_token_has_zero_length() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        let corpora: Vec<(&str, Vec<Vec<u8>>)> = vec![
            ("random", make_random_strings(100, 50, 77)),
            (
                "user",
                make_user_strings(50)
                    .into_iter()
                    .map(String::into_bytes)
                    .collect(),
            ),
            ("binary", make_binary_strings(50, 30, 13)),
            ("fixed_len", make_fixed_length_strings(20, MAX_TOKEN_SIZE)),
        ];
        for (name, c) in &corpora {
            let result = train_strings(c, &cfg);
            let view = result.dict.as_view();
            for i in 0..view.num_tokens() {
                assert!(
                    !view.token(i as Token).is_empty(),
                    "corpus={name} token {i} empty"
                );
            }
        }
    }

    #[test]
    fn dynamic_threshold_produces_merged_tokens() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Dynamic(DynamicThreshold {
                sample_fraction: 0.5,
            }),
            seed: Some(42),
            ..Default::default()
        };
        assert!(
            train_strings(&make_user_strings(200), &cfg)
                .dict
                .num_tokens()
                > 256
        );
    }

    #[test]
    fn dynamic_threshold_does_not_exceed_capacity() {
        let cfg = TrainingConfig {
            max_dict_bits: 12,
            threshold: ThresholdSpec::Dynamic(DynamicThreshold {
                sample_fraction: 1.0,
            }),
            seed: Some(42),
        };
        let result = train_strings(&make_user_strings(500), &cfg);
        assert!(result.dict.num_tokens() <= max_dict_size(cfg.max_dict_bits));
    }

    #[test]
    fn gathered_and_in_place_scans_agree() {
        let corpora: Vec<Vec<Vec<u8>>> = vec![
            make_user_strings(400)
                .into_iter()
                .map(String::into_bytes)
                .collect(),
            make_random_strings(400, 60, 7),
            make_binary_strings(300, 40, 99),
            make_mixed_length_strings(400, 120, 5),
        ];

        for corpus in &corpora {
            let raw = make_raw(corpus);
            let rows = ArrowRows::new(&raw.data, &raw.offsets);
            let total_bytes = rows.total_bytes();

            for bits in [9u8, 12, 16] {
                for fraction in [0.15, 0.5, 1.0] {
                    let cfg = TrainingConfig {
                        max_dict_bits: bits,
                        threshold: ThresholdSpec::Dynamic(DynamicThreshold {
                            sample_fraction: fraction,
                        }),
                        seed: Some(42),
                    };
                    let (order, start) = make_training_order(&rows, cfg.threshold, total_bytes, 42);
                    let selected_order = &order[start..];
                    let budget = scan_budget(cfg.threshold, total_bytes).unwrap();
                    let gathered = gather_sample(selected_rows(&rows, selected_order), budget);

                    let in_place =
                        discover_tokens(selected_rows(&rows, selected_order), &cfg, total_bytes);
                    let copied = discover_tokens(gathered.rows(), &cfg, total_bytes);
                    assert_eq!(in_place.dict.bytes(), copied.dict.bytes());
                    assert_eq!(in_place.dict.offsets(), copied.dict.offsets());
                }
            }
        }
    }

    #[test]
    fn contiguous_and_separate_rows_train_identically() {
        let corpus: Vec<Vec<u8>> = make_user_strings(300)
            .into_iter()
            .map(String::into_bytes)
            .collect();
        let raw = make_raw(&corpus);
        let slices: Vec<&[u8]> = corpus.iter().map(Vec::as_slice).collect();
        let cfg = TrainingConfig {
            seed: Some(42),
            ..Default::default()
        };

        let contiguous = train(&ArrowRows::new(&raw.data, &raw.offsets), &cfg);
        let by_rows = train(slices.as_slice(), &cfg);
        assert_eq!(contiguous.dict.bytes(), by_rows.dict.bytes());
        assert_eq!(contiguous.dict.offsets(), by_rows.dict.offsets());
    }

    #[test]
    fn dynamic_threshold_dictionary_is_sorted() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Dynamic(DynamicThreshold {
                sample_fraction: 0.3,
            }),
            seed: Some(42),
            ..Default::default()
        };
        assert!(is_lex_sorted(
            train_strings(&make_user_strings(100), &cfg).dict.as_view()
        ));
    }

    #[test]
    fn no_duplicate_tokens_in_dictionary() {
        let result = train_strings(&make_user_strings(100), &TrainingConfig::default());
        let view = result.dict.as_view();
        let n = view.num_tokens();
        for i in 1..n {
            assert!(
                view.token((i - 1) as Token) != view.token(i as Token),
                "duplicate token at {} and {}",
                i - 1,
                i
            );
        }
    }

    #[test]
    fn homogeneous_corpus_produces_merges() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        let result = train_strings(&make_homogeneous_strings(50, 16, b'a'), &cfg);
        assert!(result.dict.num_tokens() > 256);
        check_base_tokens(result.dict.as_view());
    }

    #[test]
    fn alternating_corpus_produces_merges() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        let result = train_strings(&make_alternating_strings(50, 16), &cfg);
        assert!(result.dict.num_tokens() > 256);
        check_base_tokens(result.dict.as_view());
    }

    #[test]
    fn mixed_length_corpus_produces_valid_dictionary() {
        let cfg = TrainingConfig {
            threshold: ThresholdSpec::Fixed(FixedThreshold { value: 2 }),
            seed: Some(42),
            ..Default::default()
        };
        let result = train_strings(&make_mixed_length_strings(200, 64, 7), &cfg);
        check_base_tokens(result.dict.as_view());
        assert!(is_lex_sorted(result.dict.as_view()));
        assert!(result.dict.num_tokens() <= max_dict_size(cfg.max_dict_bits));
    }

    #[test]
    fn all_bit_widths_produce_valid_dictionary() {
        let corpus = make_user_strings(50);
        for b in 9u8..=16 {
            let cfg = TrainingConfig {
                max_dict_bits: b,
                seed: Some(42),
                ..Default::default()
            };
            let result = train_strings(&corpus, &cfg);
            check_base_tokens(result.dict.as_view());
            assert!(
                is_lex_sorted(result.dict.as_view()),
                "not sorted for bits={b}"
            );
            assert!(
                result.dict.num_tokens() <= max_dict_size(b),
                "overflow for bits={b}"
            );
        }
    }
}
