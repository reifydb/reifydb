// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::key::encoded::{EncodedKey, INLINE_CAP};

pub enum KeyBuf {
	Inline {
		len: u8,
		buf: [u8; INLINE_CAP],
	},
	Spill(Vec<u8>),
}

impl KeyBuf {
	pub fn new() -> Self {
		KeyBuf::Inline {
			len: 0,
			buf: [0u8; INLINE_CAP],
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		if capacity <= INLINE_CAP {
			Self::new()
		} else {
			KeyBuf::Spill(Vec::with_capacity(capacity))
		}
	}

	pub fn as_slice(&self) -> &[u8] {
		match self {
			KeyBuf::Inline {
				len,
				buf,
			} => &buf[..*len as usize],
			KeyBuf::Spill(v) => v.as_slice(),
		}
	}

	pub fn len(&self) -> usize {
		match self {
			KeyBuf::Inline {
				len,
				..
			} => *len as usize,
			KeyBuf::Spill(v) => v.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn push(&mut self, byte: u8) {
		match self {
			KeyBuf::Inline {
				len,
				buf,
			} => {
				let cur = *len as usize;
				if cur < INLINE_CAP {
					buf[cur] = byte;
					*len += 1;
					return;
				}
				let mut vec = Vec::with_capacity(cur + 1);
				vec.extend_from_slice(&buf[..cur]);
				vec.push(byte);
				*self = KeyBuf::Spill(vec);
			}
			KeyBuf::Spill(v) => v.push(byte),
		}
	}

	pub fn extend_from_slice(&mut self, slice: &[u8]) {
		match self {
			KeyBuf::Inline {
				len,
				buf,
			} => {
				let cur = *len as usize;
				let total = cur + slice.len();
				if total <= INLINE_CAP {
					buf[cur..total].copy_from_slice(slice);
					*len = total as u8;
					return;
				}
				let mut vec = Vec::with_capacity(total);
				vec.extend_from_slice(&buf[..cur]);
				vec.extend_from_slice(slice);
				*self = KeyBuf::Spill(vec);
			}
			KeyBuf::Spill(v) => v.extend_from_slice(slice),
		}
	}

	pub fn finish(self) -> EncodedKey {
		match self {
			KeyBuf::Inline {
				len,
				buf,
			} => EncodedKey::Inline {
				len,
				buf,
			},
			KeyBuf::Spill(v) => EncodedKey::new(v),
		}
	}
}

impl Default for KeyBuf {
	fn default() -> Self {
		Self::new()
	}
}
