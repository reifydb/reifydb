// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	interface::fragment::BorrowedFragment, OwnedFragment, Type, err, error::diagnostic::cast,
	value::Blob,
};

use crate::columnar::ColumnData;

pub fn to_blob(
	data: &ColumnData,
	_fragment: impl Fn() -> OwnedFragment,
) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Utf8(container) => {
			let mut out = ColumnData::with_capacity(
				Type::Blob,
				container.len(),
			);
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let temp_fragment = BorrowedFragment::new_internal(
						container[idx].as_str(),
					);
					out.push(Blob::from_utf8(temp_fragment));
				} else {
					out.push_undefined()
				}
			}
			Ok(out)
		}
		_ => {
			let source_type = data.get_type();
			err!(cast::unsupported_cast(
				_fragment(),
				source_type,
				Type::Blob
			))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::BitVec;

	use super::*;

	#[test]
	fn test_from_utf8() {
		let strings = vec!["Hello".to_string(), "World".to_string()];
		let bitvec = BitVec::repeat(2, true);
		let container = ColumnData::utf8_with_bitvec(strings, bitvec);

		let result = to_blob(&container, || OwnedFragment::testing_empty())
			.unwrap();

		match result {
			ColumnData::Blob(container) => {
				assert_eq!(container[0].as_bytes(), b"Hello");
				assert_eq!(container[1].as_bytes(), b"World");
			}
			_ => panic!("Expected BLOB column data"),
		}
	}

	#[test]
	fn test_unsupported() {
		let ints = vec![42i32];
		let bitvec = BitVec::repeat(1, true);
		let container = ColumnData::int4_with_bitvec(ints, bitvec);

		let result = to_blob(&container, || OwnedFragment::testing_empty());
		assert!(result.is_err());
	}
}
