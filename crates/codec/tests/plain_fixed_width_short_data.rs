// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	error::DecodeError,
	frame::{
		decode::decode_frames,
		format::{Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE, RBCF_MAGIC, RBCF_VERSION},
	},
	tag::ValueKind,
};

const NAME: &[u8] = b"c";

const FIXED_WIDTH_KINDS: [(ValueKind, usize); 12] = [
	(ValueKind::Int4, 4),
	(ValueKind::Int1, 1),
	(ValueKind::Int2, 2),
	(ValueKind::Int8, 8),
	(ValueKind::Int16, 16),
	(ValueKind::Uint1, 1),
	(ValueKind::Uint2, 2),
	(ValueKind::Uint4, 4),
	(ValueKind::Uint8, 8),
	(ValueKind::Uint16, 16),
	(ValueKind::Float4, 4),
	(ValueKind::Float8, 8),
];

fn plain_column_message(kind: ValueKind, row_count: u32, data: &[u8]) -> Vec<u8> {
	let mut column = vec![kind.byte(), Encoding::Plain as u8, 0, 0];
	column.extend_from_slice(&(NAME.len() as u16).to_le_bytes());
	column.extend_from_slice(&0u16.to_le_bytes());
	for field in [row_count, 0, data.len() as u32, 0, 0] {
		column.extend_from_slice(&field.to_le_bytes());
	}
	column.extend_from_slice(NAME);
	column.resize(column.len().next_multiple_of(4), 0);
	column.extend_from_slice(data);

	let mut frame = row_count.to_le_bytes().to_vec();
	frame.extend_from_slice(&1u16.to_le_bytes());
	frame.extend_from_slice(&[0, 0]);
	frame.extend_from_slice(&((FRAME_HEADER_SIZE + column.len()) as u32).to_le_bytes());
	frame.extend_from_slice(&column);

	let mut message = RBCF_MAGIC.to_le_bytes().to_vec();
	message.extend_from_slice(&RBCF_VERSION.to_le_bytes());
	message.extend_from_slice(&0u16.to_le_bytes());
	message.extend_from_slice(&1u32.to_le_bytes());
	message.extend_from_slice(&((MESSAGE_HEADER_SIZE + frame.len()) as u32).to_le_bytes());
	message.extend_from_slice(&frame);
	message
}

fn decoded_rows(message: &[u8]) -> Result<usize, DecodeError> {
	decode_frames(message).map(|frames| frames[0].columns[0].data.len())
}

fn is_unexpected_eof(error: &DecodeError) -> bool {
	match error {
		DecodeError::UnexpectedEof {
			..
		} => true,
		DecodeError::ColumnDecodeFailed {
			source,
			..
		} => is_unexpected_eof(source),
		_ => false,
	}
}

fn assert_unexpected_eof(kind: ValueKind, message: &[u8]) {
	let result = decoded_rows(message);
	assert!(
		matches!(&result, Err(error) if is_unexpected_eof(error)),
		"a {kind:?} column cut short must decode to an unexpected eof, got {result:?}"
	);
}

#[test]
fn a_plain_fixed_width_column_with_data_shorter_than_its_rows_is_an_unexpected_eof() {
	// A column one byte short of row_count * width must be a decode error, never a panic.
	for (kind, width) in FIXED_WIDTH_KINDS {
		let full = vec![0u8; 4 * width];
		assert_eq!(decoded_rows(&plain_column_message(kind, 4, &full)), Ok(4), "{kind:?} baseline must decode");
		assert_unexpected_eof(kind, &plain_column_message(kind, 4, &full[..full.len() - 1]));
	}
}

#[test]
fn a_plain_int4_column_cut_to_six_bytes_for_four_rows_is_an_unexpected_eof() {
	// Six bytes hold 1.5 Int4 rows, so four rows must be an error, not an index past the end.
	let full = [1u8, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0];
	assert_eq!(decoded_rows(&plain_column_message(ValueKind::Int4, 4, &full)), Ok(4));
	assert_unexpected_eof(ValueKind::Int4, &plain_column_message(ValueKind::Int4, 4, &full[..6]));
}

#[test]
fn a_plain_bool_column_with_fewer_bytes_than_its_rows_need_is_an_unexpected_eof() {
	// Sixteen rows need two packed bytes, so one byte must be an error, never a panic.
	assert_eq!(decoded_rows(&plain_column_message(ValueKind::Boolean, 16, &[0xFF, 0x01])), Ok(16));
	assert_unexpected_eof(ValueKind::Boolean, &plain_column_message(ValueKind::Boolean, 16, &[0xFF]));
}

#[test]
fn a_plain_dictionary_id_column_with_data_shorter_than_its_rows_is_an_unexpected_eof() {
	// Two 4 byte ids after the width byte need nine bytes, so five must be an error, never a panic.
	let full = [4u8, 1, 0, 0, 0, 2, 0, 0, 0];
	assert_eq!(decoded_rows(&plain_column_message(ValueKind::DictionaryId, 2, &full)), Ok(2));
	assert_unexpected_eof(ValueKind::DictionaryId, &plain_column_message(ValueKind::DictionaryId, 2, &full[..5]));
}

#[test]
fn a_plain_dictionary_id_column_with_rows_but_no_data_is_a_decode_error() {
	// Declared rows with no data must be an error, not an empty column that silently drops them.
	let result = decoded_rows(&plain_column_message(ValueKind::DictionaryId, 2, &[]));
	assert!(result.is_err(), "two declared dictionary id rows with no data must not decode, got {result:?}");
}
