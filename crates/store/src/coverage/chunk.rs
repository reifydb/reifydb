// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor<S> {
	pub last_key: Option<EncodedKey>,
	pub exhausted: bool,

	pub stop: Option<S>,
}

impl<S> Default for Cursor<S> {
	fn default() -> Self {
		Self {
			last_key: None,
			exhausted: false,
			stop: None,
		}
	}
}

pub type RangeCursor = Cursor<()>;

pub trait ScannedStop {
	fn scanned(&self) -> bool;
}

impl<S: ScannedStop> Cursor<S> {
	pub fn scanned_to_end(&self) -> bool {
		self.exhausted && self.stop.as_ref().is_some_and(ScannedStop::scanned)
	}
}

impl<S> Cursor<S> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}

	pub fn advance(&mut self, key: EncodedKey) {
		self.last_key = Some(key);
	}

	pub fn finish(&mut self) {
		self.exhausted = true;
	}

	pub fn reset(&mut self) {
		self.last_key = None;
		self.exhausted = false;
		self.stop = None;
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServedChunk<B> {
	Served(B),
	Gap,
}

impl<B> ServedChunk<B> {
	pub fn is_gap(&self) -> bool {
		matches!(self, ServedChunk::Gap)
	}

	pub fn served(self) -> Option<B> {
		match self {
			ServedChunk::Served(batch) => Some(batch),
			ServedChunk::Gap => None,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(bytes: &[u8]) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	#[test]
	fn new_starts_before_the_first_key_and_unexhausted() {
		// A fresh cursor must mean "not started", otherwise the first chunk skips the caller's own lower bound.
		let cursor = RangeCursor::new();
		assert_eq!(cursor.last_key, None);
		assert!(!cursor.is_exhausted());
	}

	#[test]
	fn advance_records_the_key_without_exhausting() {
		// advance only moves the start of the next chunk; exhausting here would end a scan that has more pages.
		let mut cursor = RangeCursor::new();
		cursor.advance(key(b"c"));
		assert_eq!(cursor.last_key, Some(key(b"c")));
		assert!(!cursor.is_exhausted());
	}

	#[test]
	fn advance_replaces_the_previous_key() {
		// Keeping the older key would re-serve every row between it and the newer one on the next chunk.
		let mut cursor = RangeCursor::new();
		cursor.advance(key(b"c"));
		cursor.advance(key(b"f"));
		assert_eq!(cursor.last_key, Some(key(b"f")));
	}

	#[test]
	fn advance_after_finish_leaves_the_scan_exhausted() {
		// advance must never clear exhausted, or a finished scan restarts and the tier loop spins forever.
		let mut cursor = RangeCursor::new();
		cursor.finish();
		cursor.advance(key(b"c"));
		assert!(cursor.is_exhausted());
	}

	#[test]
	fn finish_marks_the_scan_exhausted() {
		// The caller ends its loop only on exhausted; leaving it false reads the persistent tier forever.
		let mut cursor = RangeCursor::new();
		cursor.finish();
		assert!(cursor.is_exhausted());
	}

	#[test]
	fn finish_keeps_the_last_key_for_attribution() {
		// Clearing last_key on finish misattributes every exhausted scan in the metrics.
		let mut cursor = RangeCursor::new();
		cursor.advance(key(b"f"));
		cursor.finish();
		assert_eq!(cursor.last_key, Some(key(b"f")));
	}

	#[test]
	fn reset_clears_both_fields() {
		// A reused cursor that keeps either field replays a stale start or reports an already finished scan.
		let mut cursor = RangeCursor::new();
		cursor.advance(key(b"f"));
		cursor.finish();
		cursor.reset();
		assert_eq!(cursor.last_key, None);
		assert!(!cursor.is_exhausted());
		assert_eq!(cursor, RangeCursor::new());
	}

	#[test]
	fn is_gap_is_false_for_a_served_chunk() {
		// Treating served rows as a gap re-reads the persistent tier for a span the cache already proved.
		let chunk: ServedChunk<Vec<u8>> = ServedChunk::Served(vec![1, 2, 3]);
		assert!(!chunk.is_gap());
	}

	#[test]
	fn is_gap_is_true_for_a_gap() {
		// Missing the gap signal silently drops every row the cache cannot speak for.
		let chunk: ServedChunk<Vec<u8>> = ServedChunk::Gap;
		assert!(chunk.is_gap());
	}

	#[test]
	fn served_yields_the_batch() {
		// Dropping the batch here loses rows the cache already proved and the caller never re-reads them.
		let chunk = ServedChunk::Served(vec![7u8, 8]);
		assert_eq!(chunk.served(), Some(vec![7u8, 8]));
	}

	#[test]
	fn served_yields_none_for_a_gap() {
		// A gap must not fabricate an empty batch, which the caller would merge as "nothing in this span".
		let chunk: ServedChunk<Vec<u8>> = ServedChunk::Gap;
		assert_eq!(chunk.served(), None);
	}
}
