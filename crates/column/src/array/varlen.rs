// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

// Variable-length byte storage — an offsets-plus-bytes layout for `Utf8` and
// `Blob` columns. `offsets[i]..offsets[i+1]` spans row `i`'s bytes in `data`.
// `offsets.len() == element_count + 1`, and `offsets[0] == 0`.
#[derive(Clone, Debug)]
pub struct VarLenArray {
	pub ty: Type,
	pub offsets: Vec<u32>,
	pub data: Vec<u8>,
}

impl VarLenArray {
	pub fn new(ty: Type) -> Self {
		Self {
			ty,
			offsets: vec![0],
			data: Vec::new(),
		}
	}

	pub fn from_strings(ty: Type, strings: impl IntoIterator<Item = String>) -> Self {
		let mut a = Self::new(ty);
		for s in strings {
			a.push_bytes(s.as_bytes());
		}
		a
	}

	pub fn push_bytes(&mut self, bytes: &[u8]) {
		self.data.extend_from_slice(bytes);
		self.offsets.push(self.data.len() as u32);
	}

	pub fn len(&self) -> usize {
		self.offsets.len().saturating_sub(1)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn bytes_at(&self, row: usize) -> &[u8] {
		let start = self.offsets[row] as usize;
		let end = self.offsets[row + 1] as usize;
		&self.data[start..end]
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_strings_preserves_contents() {
		let a = VarLenArray::from_strings(
			Type::Utf8,
			["alpha".into(), "bravo".into(), "".into(), "delta".into()],
		);
		assert_eq!(a.len(), 4);
		assert_eq!(a.bytes_at(0), b"alpha");
		assert_eq!(a.bytes_at(1), b"bravo");
		assert_eq!(a.bytes_at(2), b"");
		assert_eq!(a.bytes_at(3), b"delta");
	}
}
