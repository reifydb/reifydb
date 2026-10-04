// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Longest-prefix matcher: maps byte sequences (`1..=MAX_TOKEN_SIZE` bytes) to
//! token ids and answers "what is the longest dictionary token that is a prefix
//! of this input?".
//!
//! Two-tier storage:
//!   * **short map** — tokens of length `1..=8` keyed by their bytes packed into
//!     a `u64` plus the length.
//!   * **long map** — tokens of length `9..=16` bucketed by their 8-byte prefix.
//!     Each bucket holds the `(suffix, length, token)` triples sharing that
//!     prefix and is searched for the longest matching suffix. A bucket starts
//!     as a vector and switches to length-grouped binary search once it grows.
//!
//! Matching loads up to 16 bytes once, probes the long-token bucket, then checks
//! short tokens from longest to shortest.

use hashbrown::HashMap;

use crate::core::dictionary::{CompactDictionaryView, DictionaryView};
use crate::core::types::{MAX_TOKEN_SIZE, Token};

/// Tokens of this length or shorter live in the short map; longer tokens are
/// bucketed by their first `BUCKET_PREFIX_LEN` bytes.
const BUCKET_PREFIX_LEN: usize = 8;

const MAX_SUFFIX_LEN: usize = MAX_TOKEN_SIZE - BUCKET_PREFIX_LEN;
const PROMOTE_THRESHOLD: usize = 48;

#[inline]
fn load_window(data: &[u8]) -> (u64, u64) {
    let n = data.len();
    if n >= MAX_TOKEN_SIZE {
        return (
            u64::from_le_bytes(data[..8].try_into().unwrap()),
            u64::from_le_bytes(data[8..16].try_into().unwrap()),
        );
    }
    if n >= 8 {
        let lo = u64::from_le_bytes(data[..8].try_into().unwrap());
        let hi = if n > 8 {
            u64::from_le_bytes(data[n - 8..].try_into().unwrap()) >> ((MAX_TOKEN_SIZE - n) * 8)
        } else {
            0
        };
        return (lo, hi);
    }
    let lo = if n >= 4 {
        u32::from_le_bytes(data[..4].try_into().unwrap()) as u64
            | (u32::from_le_bytes(data[n - 4..].try_into().unwrap()) as u64) << ((n - 4) * 8)
    } else if n >= 2 {
        u16::from_le_bytes(data[..2].try_into().unwrap()) as u64
            | (u16::from_le_bytes(data[n - 2..].try_into().unwrap()) as u64) << ((n - 2) * 8)
    } else {
        data[0] as u64
    };
    (lo, 0)
}

/// Pack the low `min(len, data.len(), 8)` bytes of `data` into a little-endian
/// `u64`; higher bytes read as zero. The full-8-byte case is a single load.
#[inline]
fn load_le_u64(data: &[u8], len: usize) -> u64 {
    if len >= BUCKET_PREFIX_LEN && data.len() >= BUCKET_PREFIX_LEN {
        return u64::from_le_bytes(data[..BUCKET_PREFIX_LEN].try_into().unwrap());
    }
    let mut buf = [0u8; 8];
    let n = len.min(data.len());
    buf[..n].copy_from_slice(&data[..n]);
    u64::from_le_bytes(buf)
}

/// Mask of the low `len * 8` bits in a `u64`.
#[inline]
fn mask_u64(len: usize) -> u64 {
    if len >= 8 {
        u64::MAX
    } else {
        (1u64 << (len * 8)) - 1
    }
}

/// One long-token entry within a bucket: the suffix bytes after the shared
/// 8-byte prefix (`slen` of them, packed little-endian and masked to that
/// length) and the token id.
#[derive(Copy, Clone, Debug)]
struct LongEntry {
    suffix: u64,
    slen: u8,
    token: Token,
}

/// Long tokens with the same 8-byte prefix.
#[derive(Debug, Clone)]
enum Bucket {
    Linear(Vec<LongEntry>),
    Grouped(Box<GroupedBucket>),
}

#[inline]
fn search_linear(entries: &[LongEntry], val: u64, max_slen: usize) -> Option<(Token, usize)> {
    for e in entries {
        let elen = e.slen as usize;
        // Matching low bytes = trailing-zero bytes of the XOR.
        if elen <= max_slen && ((val ^ e.suffix).trailing_zeros() >> 3) as usize >= elen {
            return Some((e.token, elen));
        }
    }
    None
}

/// Entries grouped by suffix length and sorted by suffix within each group.
#[derive(Debug, Clone)]
struct GroupedBucket {
    entries: Vec<LongEntry>,
    ends: [u32; MAX_SUFFIX_LEN + 2],
    present: u16,
}

impl GroupedBucket {
    fn build(entries: &[LongEntry]) -> Self {
        let mut sorted = entries.to_vec();
        sorted.sort_unstable_by(|a, b| b.slen.cmp(&a.slen).then(a.suffix.cmp(&b.suffix)));

        let mut ends = [0u32; MAX_SUFFIX_LEN + 2];
        let mut present = 0u16;
        let mut counts = [0u32; MAX_SUFFIX_LEN + 1];
        for e in &sorted {
            counts[e.slen as usize] += 1;
            present |= 1u16 << e.slen;
        }
        let mut acc = 0u32;
        for slen in (1..=MAX_SUFFIX_LEN).rev() {
            acc += counts[slen];
            ends[slen] = acc;
        }
        Self {
            entries: sorted,
            ends,
            present,
        }
    }

    fn insert(&mut self, entry: LongEntry) {
        let slen = entry.slen as usize;
        let start = self.ends[slen + 1] as usize;
        let end = self.ends[slen] as usize;
        let pos = start + self.entries[start..end].partition_point(|e| e.suffix < entry.suffix);
        self.entries.insert(pos, entry);
        for e in &mut self.ends[1..=slen] {
            *e += 1;
        }
        self.present |= 1u16 << slen;
    }

    #[inline]
    fn find(&self, val: u64, max_slen: usize) -> Option<(Token, usize)> {
        let mut lens = self.present & ((1u16 << (max_slen + 1)) - 1);
        while lens != 0 {
            let slen = (u16::BITS - 1 - lens.leading_zeros()) as usize;
            lens &= !(1u16 << slen);

            let group = &self.entries[self.ends[slen + 1] as usize..self.ends[slen] as usize];
            let target = val & mask_u64(slen);
            if let Ok(i) = group.binary_search_by_key(&target, |e| e.suffix) {
                return Some((group[i].token, slen));
            }
        }
        None
    }
}

/// Maps byte sequences (`1..=MAX_TOKEN_SIZE` bytes) to [`Token`] ids. Always
/// holds the 256 single-byte tokens after construction, so
/// [`find_longest_match`](Self::find_longest_match) is total.
#[derive(Default, Debug, Clone)]
pub(crate) struct LongestPrefixMatcher {
    /// Length `1..=8` tokens keyed by (low-`len`-byte u64, length).
    short_map: HashMap<(u64, u8), Token>,
    /// Length `9..=16` tokens bucketed by their 8-byte prefix.
    long_map: HashMap<u64, Bucket>,
    /// Longest short-map token length present (`1..=8`).
    max_short_len: u8,
    /// Next id to assign. `u32` so the full 16-bit token space (65 536 entries)
    /// is representable without overflow.
    next_id: u32,
}

impl LongestPrefixMatcher {
    /// Pre-inserts the 256 single-byte tokens with ids `0..=255`.
    pub(crate) fn new() -> Self {
        let mut short_map = HashMap::with_capacity(256);
        for i in 0u16..=255 {
            short_map.insert((i as u64, 1u8), i);
        }
        Self {
            short_map,
            long_map: HashMap::new(),
            max_short_len: 1,
            next_id: 256,
        }
    }

    /// Reserve training-time maps for the configured dictionary budget.
    pub(crate) fn reserve(&mut self, token_capacity: usize) {
        self.short_map
            .reserve(token_capacity.saturating_sub(self.short_map.len()));
        let long_capacity = (token_capacity / 4).max(16);
        self.long_map
            .reserve(long_capacity.saturating_sub(self.long_map.len()));
    }

    /// Build a matcher from a complete dictionary: token at index `i` receives
    /// id `i`. The caller guarantees the dictionary contains every single-byte
    /// token so [`find_longest_match`](Self::find_longest_match) stays total.
    pub(crate) fn from_dictionary(dict: CompactDictionaryView<'_>) -> Self {
        let n = dict.num_tokens();
        let long_capacity = (0..n)
            .filter(|&i| dict.token(i as Token).len() > BUCKET_PREFIX_LEN)
            .count();
        let mut me = Self {
            short_map: HashMap::with_capacity(n),
            long_map: HashMap::with_capacity(long_capacity),
            max_short_len: 1,
            next_id: n as u32,
        };
        for i in 0..n {
            let id = i as Token;
            me.insert_internal(dict.token(id), id);
        }
        me
    }

    /// Insert `data` and assign it the next available token id.
    ///
    /// Precondition: `1 <= data.len() <= MAX_TOKEN_SIZE` and `size() < 65_536`.
    pub(crate) fn insert(&mut self, data: &[u8]) -> Token {
        let id = self.next_id as Token;
        self.next_id += 1;
        self.insert_internal(data, id);
        id
    }

    #[inline]
    fn insert_internal(&mut self, data: &[u8], id: Token) {
        debug_assert!(!data.is_empty() && data.len() <= MAX_TOKEN_SIZE);
        let len = data.len();
        if len <= BUCKET_PREFIX_LEN {
            let key = load_le_u64(data, len);
            self.short_map.insert((key, len as u8), id);
            self.max_short_len = self.max_short_len.max(len as u8);
            return;
        }

        let prefix = load_le_u64(data, BUCKET_PREFIX_LEN);
        let slen = len - BUCKET_PREFIX_LEN;
        let suffix = load_le_u64(&data[BUCKET_PREFIX_LEN..], slen);
        let entry = LongEntry {
            suffix,
            slen: slen as u8,
            token: id,
        };
        let bucket = self
            .long_map
            .entry(prefix)
            .or_insert_with(|| Bucket::Linear(Vec::new()));
        match bucket {
            Bucket::Linear(entries) => {
                let pos = entries.partition_point(|e| e.slen > entry.slen);
                entries.insert(pos, entry);
                if entries.len() > PROMOTE_THRESHOLD {
                    *bucket = Bucket::Grouped(Box::new(GroupedBucket::build(entries)));
                }
            }
            Bucket::Grouped(grouped) => grouped.insert(entry),
        }
    }

    /// Longest token whose bytes are a prefix of `data`, with that prefix's
    /// length.
    ///
    /// Precondition: `!data.is_empty()` and the matcher contains every
    /// single-byte token (always true after [`new`](Self::new) or
    /// [`from_dictionary`](Self::from_dictionary) with a complete dictionary).
    #[inline]
    pub(crate) fn find_longest_match(&self, data: &[u8]) -> (Token, usize) {
        let (lo64, hi64) = load_window(data);
        let win = data.len().min(MAX_TOKEN_SIZE);

        if win > BUCKET_PREFIX_LEN
            && !self.long_map.is_empty()
            && let Some(bucket) = self.long_map.get(&lo64)
        {
            let max_slen = win - BUCKET_PREFIX_LEN;
            let hit = match bucket {
                Bucket::Linear(entries) => {
                    search_linear(entries, hi64 & mask_u64(max_slen), max_slen)
                }
                Bucket::Grouped(grouped) => grouped.find(hi64, max_slen),
            };
            if let Some((t, slen)) = hit {
                return (t, BUCKET_PREFIX_LEN + slen);
            }
        }

        let short_max = win.min(self.max_short_len as usize);
        for len in (1..=short_max).rev() {
            if let Some(&t) = self.short_map.get(&(lo64 & mask_u64(len), len as u8)) {
                return (t, len);
            }
        }
        unreachable!("LPM precondition: every single-byte token must be present")
    }

    /// Number of tokens currently in the matcher.
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.next_id as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dictionary::{CompactDictionary, Dictionary};

    fn insert_str(lpm: &mut LongestPrefixMatcher, s: &str) -> Token {
        lpm.insert(s.as_bytes())
    }

    fn find_str(lpm: &LongestPrefixMatcher, s: &str) -> (Token, usize) {
        lpm.find_longest_match(s.as_bytes())
    }

    fn make_test_dictionary(extra: &[&str]) -> CompactDictionary {
        let mut bytes = Vec::new();
        let mut offsets = vec![0u32];
        for i in 0u16..=255 {
            bytes.push(i as u8);
            offsets.push(bytes.len() as u32);
        }
        for &s in extra {
            bytes.extend_from_slice(s.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        CompactDictionary::from_raw(bytes, offsets)
    }

    // ── Construction ─────────────────────────────────────────────────────────

    #[test]
    fn default_constructor_size_is_256() {
        assert_eq!(LongestPrefixMatcher::new().size(), 256);
    }

    #[test]
    fn all_single_bytes_found_after_construction() {
        let lpm = LongestPrefixMatcher::new();
        for i in 0u16..=255 {
            let b = [i as u8];
            let (tok, len) = lpm.find_longest_match(&b);
            assert_eq!(tok, i, "wrong token for byte {i}");
            assert_eq!(len, 1, "wrong length for byte {i}");
        }
    }

    // ── Insert ───────────────────────────────────────────────────────────────

    #[test]
    fn first_insert_returns_id_256() {
        let mut lpm = LongestPrefixMatcher::new();
        assert_eq!(insert_str(&mut lpm, "ab"), 256);
    }

    #[test]
    fn subsequent_inserts_increment_id() {
        let mut lpm = LongestPrefixMatcher::new();
        assert_eq!(insert_str(&mut lpm, "ab"), 256);
        assert_eq!(insert_str(&mut lpm, "cd"), 257);
        assert_eq!(insert_str(&mut lpm, "ef"), 258);
    }

    #[test]
    fn exactly_eight_bytes_short_store() {
        let mut lpm = LongestPrefixMatcher::new();
        let id = insert_str(&mut lpm, "12345678");
        let (tok, len) = find_str(&lpm, "12345678");
        assert_eq!((tok, len), (id, 8));
    }

    #[test]
    fn exactly_nine_bytes_long_store() {
        let mut lpm = LongestPrefixMatcher::new();
        let id = insert_str(&mut lpm, "123456789");
        let (tok, len) = find_str(&lpm, "123456789X");
        assert_eq!((tok, len), (id, 9));
    }

    #[test]
    fn max_token_size_insert_and_find() {
        let mut lpm = LongestPrefixMatcher::new();
        let pat = "0123456789abcdef";
        assert_eq!(pat.len(), MAX_TOKEN_SIZE);
        let id = lpm.insert(pat.as_bytes());
        let (tok, len) = lpm.find_longest_match(pat.as_bytes());
        assert_eq!((tok, len), (id, MAX_TOKEN_SIZE));
    }

    // ── find_longest_match ───────────────────────────────────────────────────

    #[test]
    fn longest_match_wins_over_shorter() {
        let mut lpm = LongestPrefixMatcher::new();
        insert_str(&mut lpm, "abc");
        let long_id = insert_str(&mut lpm, "abcdefghi");
        let (tok, len) = find_str(&lpm, "abcdefghi");
        assert_eq!((tok, len), (long_id, 9));
    }

    #[test]
    fn falls_back_to_shorter_if_long_not_present() {
        let mut lpm = LongestPrefixMatcher::new();
        let short_id = insert_str(&mut lpm, "abc");
        let (tok, len) = find_str(&lpm, "abcdef");
        assert_eq!((tok, len), (short_id, 3));
    }

    #[test]
    fn falls_back_to_single_byte() {
        let mut lpm = LongestPrefixMatcher::new();
        insert_str(&mut lpm, "XY");
        let (tok, len) = find_str(&lpm, "XZ");
        assert_eq!((tok, len), (b'X' as Token, 1));
    }

    #[test]
    fn nine_byte_beats_eight_byte() {
        let mut lpm = LongestPrefixMatcher::new();
        insert_str(&mut lpm, "ABCDEFGH");
        let id9 = insert_str(&mut lpm, "ABCDEFGHI");
        let (tok, len) = find_str(&lpm, "ABCDEFGHIJ");
        assert_eq!((tok, len), (id9, 9));
    }

    #[test]
    fn multiple_tokens_same_long_prefix() {
        let mut lpm = LongestPrefixMatcher::new();
        let id1 = insert_str(&mut lpm, "ABCDEFGHX");
        let id2 = insert_str(&mut lpm, "ABCDEFGHYZ");
        assert_eq!(find_str(&lpm, "ABCDEFGHX__"), (id1, 9));
        assert_eq!(find_str(&lpm, "ABCDEFGHYZ_"), (id2, 10));
    }

    #[test]
    fn binary_all_zeros_long_sequence() {
        let mut lpm = LongestPrefixMatcher::new();
        let data = [0u8; 10];
        let id = lpm.insert(&data);
        assert_eq!(lpm.find_longest_match(&data), (id, 10));
    }

    // ── trie promotion (>128 entries in one bucket) ───────────────────────────

    #[test]
    fn all_tokens_findable_with_shared_long_prefix() {
        let mut lpm = LongestPrefixMatcher::new();
        let prefix = vec![b'X'; 8];
        let mut inserted = Vec::with_capacity(130);
        for i in 0..130u32 {
            let mut buf = prefix.clone();
            buf.push(i as u8);
            inserted.push(lpm.insert(&buf));
        }
        for i in 0..130u32 {
            let mut buf = prefix.clone();
            buf.push(i as u8);
            buf.push(0xFF);
            let (tok, len) = lpm.find_longest_match(&buf);
            assert_eq!((tok, len), (inserted[i as usize], 9), "token index {i}");
        }
    }

    #[test]
    fn deep_trie_multi_level_suffix() {
        let mut lpm = LongestPrefixMatcher::new();
        let prefix = vec![b'Z'; 8];
        let mut inserted = Vec::with_capacity(130);
        for i in 0..130u32 {
            let mut buf = prefix.clone();
            buf.push(0x00);
            buf.push(i as u8);
            inserted.push(lpm.insert(&buf));
        }
        for i in 0..130u32 {
            let mut buf = prefix.clone();
            buf.push(0x00);
            buf.push(i as u8);
            buf.push(0xFF);
            let (tok, len) = lpm.find_longest_match(&buf);
            assert_eq!((tok, len), (inserted[i as usize], 10), "token index {i}");
        }
    }

    // ── from_dictionary ──────────────────────────────────────────────────────

    #[test]
    fn from_dict_size_matches_extra_tokens() {
        let d = make_test_dictionary(&["ab", "abcde"]);
        assert_eq!(
            LongestPrefixMatcher::from_dictionary(d.as_view()).size(),
            258
        );
    }

    #[test]
    fn from_dict_multi_byte_token_found_with_correct_id() {
        let d = make_test_dictionary(&["ab", "abcde"]);
        let lpm = LongestPrefixMatcher::from_dictionary(d.as_view());
        assert_eq!(find_str(&lpm, "abcde"), (257, 5));
        assert_eq!(find_str(&lpm, "abc"), (256, 2));
    }

    #[test]
    fn from_dict_long_token_from_dictionary() {
        let d = make_test_dictionary(&["ABCDEFGHI"]);
        let lpm = LongestPrefixMatcher::from_dictionary(d.as_view());
        assert_eq!(find_str(&lpm, "ABCDEFGHIX"), (256, 9));
    }

    #[test]
    fn from_dict_insert_continues_id() {
        let d = make_test_dictionary(&["ab", "cd"]);
        let mut lpm = LongestPrefixMatcher::from_dictionary(d.as_view());
        assert_eq!(insert_str(&mut lpm, "ef"), 258);
        assert_eq!(lpm.size(), 259);
    }
}
