// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::factory;
use reifydb_store_column::{
	compress::Compressor,
	convert::to_arrow,
	persist::{deserialize_block, serialize_block},
	session::new_session,
	snapshot::ColumnBlock,
};
use reifydb_value::value::value_type::ValueType;

fn one_chunk_bytes() -> Vec<u8> {
	let session = new_session();
	let column =
		Compressor::new(session.clone()).compress(ValueType::Int4, &factory::int4("a", [1, 5, 2, 9])).unwrap();
	let block = ColumnBlock::new(Arc::new(vec![("a".to_string(), ValueType::Int4, false)]), vec![column]);
	serialize_block(&block, &session).unwrap()
}

fn rejection_code(bytes: &[u8]) -> String {
	match deserialize_block(bytes, &new_session()) {
		Ok(_) => panic!("a damaged block must be rejected"),
		Err(err) => err.0.code.clone(),
	}
}

#[test]
fn the_prefix_is_magic_version_and_header_length() {
	// A reader keyed on this prefix misreads every stored block if a single prefix byte moves.
	let bytes = one_chunk_bytes();
	assert_eq!(&bytes[0..4], b"BORG");
	assert_eq!(&bytes[4..6], &1u16.to_le_bytes());
	let header_len = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
	assert!(header_len + 10 <= bytes.len(), "the header must fit inside the block");
	assert!(deserialize_block(&bytes, &new_session()).is_ok(), "the untouched block must load");
}

#[test]
fn a_wrong_magic_is_rejected() {
	// Without the magic check any byte string would be decoded as a block.
	let mut bytes = one_chunk_bytes();
	bytes[0] ^= 0xff;
	assert_eq!(rejection_code(&bytes), "COL_019");
}

#[test]
fn an_unknown_version_is_rejected() {
	// A future layout must fail loudly, never be decoded with this version's rules.
	let mut bytes = one_chunk_bytes();
	bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
	assert_eq!(rejection_code(&bytes), "COL_020");
}

#[test]
fn a_truncated_header_is_rejected() {
	// A header cut short must be an error, never a panic on an out-of-bounds slice.
	let bytes = one_chunk_bytes();
	assert_eq!(rejection_code(&bytes[..12]), "COL_019");
}

#[test]
fn a_truncated_data_section_is_rejected() {
	// A chunk running past the end must be an error, never a read of foreign memory.
	let bytes = one_chunk_bytes();
	assert_eq!(rejection_code(&bytes[..bytes.len() - 1]), "COL_019");
}

#[test]
fn garbage_is_rejected_without_panicking() {
	// Random bytes must surface as an error, never abort the process.
	assert!(deserialize_block(&[0xff; 5], &new_session()).is_err());
}

fn utf8_block_with_invalid_bytes(values: Vec<String>) -> Vec<u8> {
	let session = new_session();
	let column = Compressor::new(session.clone()).compress(ValueType::Utf8, &factory::utf8("s", values)).unwrap();
	let block = ColumnBlock::new(Arc::new(vec![("s".to_string(), ValueType::Utf8, false)]), vec![column]);
	let mut bytes = serialize_block(&block, &session).unwrap();
	let at = bytes
		.windows(2)
		.position(|pair| pair == "\u{e9}".as_bytes())
		.expect("the block stores the raw utf8 bytes");
	bytes[at..at + 2].copy_from_slice(&[0xff, 0xfe]);
	bytes
}

#[test]
fn a_utf8_column_whose_stored_bytes_are_not_utf8_fails_to_read() {
	// Vortex trusts a utf8 dtype and exports unchecked, so bad bytes on disk would become strings that are not
	// utf8.
	let plain = vec!["ok".to_string(), "caf\u{e9}".to_string()];
	let repeated = (0..2000).map(|i| format!("caf\u{e9} number {}", i % 7)).collect();
	let distinct = (0..2000).map(|i| format!("row \u{e9}t\u{e9} {i} {}", "x".repeat(i % 13))).collect();
	for (label, values) in [("plain", plain), ("repeated", repeated), ("distinct", distinct)] {
		let session = new_session();
		let code = match deserialize_block(&utf8_block_with_invalid_bytes(values), &session) {
			Err(err) => err.0.code.clone(),
			Ok(block) => {
				let column = &block.columns[0];
				let read = column
					.chunks
					.iter()
					.map(|chunk| to_arrow(&session, "s", &column.field_type, chunk.clone()))
					.find_map(Result::err);
				read.unwrap_or_else(|| panic!("{label}: a utf8 column holding 0xff 0xfe was read back"))
					.0
					.code
			}
		};
		assert_eq!(code, "COL_019", "{label}");
	}
}
