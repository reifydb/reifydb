// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod any;
mod fixed;
mod varlen;

use reifydb_type::value::frame::{data::FrameColumnData, frame::Frame};
use tracing::{Span, instrument};

use crate::{
	encoding::plain::{encode_bitvec, encode_plain},
	error::EncodeError,
	format::{
		COL_FLAG_HAS_NONES, Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE, META_HAS_CREATED_AT,
		META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT, RBCF_MAGIC, RBCF_VERSION,
	},
	heuristics::choose_encoding,
	options::EncodeOptions,
};

pub(crate) struct EncodedColumn {
	pub(crate) type_code: u8,
	pub(crate) encoding: Encoding,
	pub(crate) flags: u8,
	pub(crate) nones: Vec<u8>,
	pub(crate) data: Vec<u8>,
	pub(crate) offsets: Vec<u8>,
	pub(crate) extra: Vec<u8>,
	pub(crate) row_count: u32,
}

#[instrument(
	name = "wire::encode_frames",
	level = "debug",
	skip_all,
	fields(
		frame_count = frames.len(),
		total_rows = frames.iter().map(|f| f.columns.first().map_or(0, |c| c.data.len())).sum::<usize>(),
		bytes,
	),
)]
pub fn encode_frames(frames: &[Frame], options: &EncodeOptions) -> Result<Vec<u8>, EncodeError> {
	let mut buf = Vec::with_capacity(4096);
	reserve_message_header(&mut buf);
	for frame in frames {
		encode_frame(frame, &mut buf, options)?;
	}
	write_message_header(&mut buf, frames.len() as u32);
	Span::current().record("bytes", buf.len());
	Ok(buf)
}

#[inline]
fn reserve_message_header(buf: &mut Vec<u8>) {
	buf.extend_from_slice(&[0u8; MESSAGE_HEADER_SIZE]);
}

#[inline]
fn write_message_header(buf: &mut [u8], frame_count: u32) {
	let total_size = buf.len() as u32;
	buf[0..4].copy_from_slice(&RBCF_MAGIC.to_le_bytes());
	buf[4..6].copy_from_slice(&RBCF_VERSION.to_le_bytes());
	buf[6..8].copy_from_slice(&0u16.to_le_bytes());
	buf[8..12].copy_from_slice(&frame_count.to_le_bytes());
	buf[12..16].copy_from_slice(&total_size.to_le_bytes());
}

fn encode_frame(frame: &Frame, buf: &mut Vec<u8>, options: &EncodeOptions) -> Result<(), EncodeError> {
	let frame_start = buf.len();
	let row_count = frame.columns.first().map_or(0, |c| c.data.len()) as u32;
	let column_count = frame.columns.len() as u16;
	let meta_flags = compute_meta_flags(frame);

	reserve_frame_header(buf);
	write_frame_metadata(frame, meta_flags, buf);
	encode_frame_columns(frame, buf, options)?;

	let frame_size = (buf.len() - frame_start) as u32;
	write_frame_header(buf, frame_start, row_count, column_count, meta_flags, frame_size);
	Ok(())
}

#[inline]
fn compute_meta_flags(frame: &Frame) -> u8 {
	let mut flags = 0u8;
	if !frame.row_numbers.is_empty() {
		flags |= META_HAS_ROW_NUMBERS;
	}
	if !frame.created_at.is_empty() {
		flags |= META_HAS_CREATED_AT;
	}
	if !frame.updated_at.is_empty() {
		flags |= META_HAS_UPDATED_AT;
	}
	flags
}

#[inline]
fn reserve_frame_header(buf: &mut Vec<u8>) {
	buf.extend_from_slice(&[0u8; FRAME_HEADER_SIZE]);
}

#[inline]
fn write_frame_metadata(frame: &Frame, meta_flags: u8, buf: &mut Vec<u8>) {
	if meta_flags & META_HAS_ROW_NUMBERS != 0 {
		for rn in &frame.row_numbers {
			buf.extend_from_slice(&rn.value().to_le_bytes());
		}
	}
	if meta_flags & META_HAS_CREATED_AT != 0 {
		for dt in &frame.created_at {
			buf.extend_from_slice(&dt.to_nanos().to_le_bytes());
		}
	}
	if meta_flags & META_HAS_UPDATED_AT != 0 {
		for dt in &frame.updated_at {
			buf.extend_from_slice(&dt.to_nanos().to_le_bytes());
		}
	}
}

#[inline]
fn encode_frame_columns(frame: &Frame, buf: &mut Vec<u8>, options: &EncodeOptions) -> Result<(), EncodeError> {
	for col in &frame.columns {
		encode_column(&col.name, &col.data, buf, options)?;
	}
	Ok(())
}

#[inline]
fn write_frame_header(
	buf: &mut [u8],
	frame_start: usize,
	row_count: u32,
	column_count: u16,
	meta_flags: u8,
	frame_size: u32,
) {
	let h = frame_start;
	buf[h..h + 4].copy_from_slice(&row_count.to_le_bytes());
	buf[h + 4..h + 6].copy_from_slice(&column_count.to_le_bytes());
	buf[h + 6] = meta_flags;
	buf[h + 7] = 0;
	buf[h + 8..h + 12].copy_from_slice(&frame_size.to_le_bytes());
}

fn encode_column(
	name: &str,
	col_data: &FrameColumnData,
	buf: &mut Vec<u8>,
	options: &EncodeOptions,
) -> Result<(), EncodeError> {
	let desired = options.force_encoding.unwrap_or_else(|| choose_encoding(col_data, options.compression));
	let enc = try_encode_with(col_data, desired)?;
	write_column(name, &enc, buf);
	Ok(())
}

fn try_encode_with(col_data: &FrameColumnData, desired: Encoding) -> Result<EncodedColumn, EncodeError> {
	let (inner, nones, has_nones) = match col_data {
		FrameColumnData::Option {
			inner,
			bitvec,
		} => {
			let bitmap = encode_bitvec(bitvec);
			(inner.as_ref(), bitmap, true)
		}
		other => (other, vec![], false),
	};

	let row_count = inner.len() as u32;

	let result = match desired {
		Encoding::Dict => varlen::try_dict_varlen(inner),
		Encoding::Rle => fixed::try_rle_fixed(inner).or_else(|| varlen::try_rle_varlen(inner)),
		Encoding::Delta => fixed::try_delta_fixed(inner),
		Encoding::DeltaRle => fixed::try_delta_rle_fixed(inner),
		_ => None,
	};

	let mut enc = match result {
		Some(enc) => enc,
		None => {
			let plain = encode_plain(col_data)?;
			EncodedColumn {
				type_code: plain.type_code,
				encoding: Encoding::Plain,
				flags: 0,
				nones: plain.nones,
				data: plain.data,
				offsets: plain.offsets,
				extra: vec![],
				row_count,
			}
		}
	};

	if has_nones {
		enc.nones = nones;
		enc.flags |= COL_FLAG_HAS_NONES;

		enc.type_code |= 0x80;
	}
	enc.row_count = row_count;
	Ok(enc)
}

fn write_column(name: &str, enc: &EncodedColumn, buf: &mut Vec<u8>) {
	let name_bytes = name.as_bytes();
	let name_len = name_bytes.len() as u16;
	let name_pad = (4 - (name_bytes.len() % 4)) % 4;

	buf.push(enc.type_code);
	buf.push(enc.encoding as u8);
	buf.push(enc.flags);
	buf.push(0);
	buf.extend_from_slice(&name_len.to_le_bytes());
	buf.extend_from_slice(&0u16.to_le_bytes());
	buf.extend_from_slice(&enc.row_count.to_le_bytes());
	buf.extend_from_slice(&(enc.nones.len() as u32).to_le_bytes());
	buf.extend_from_slice(&(enc.data.len() as u32).to_le_bytes());
	buf.extend_from_slice(&(enc.offsets.len() as u32).to_le_bytes());
	buf.extend_from_slice(&(enc.extra.len() as u32).to_le_bytes());

	buf.extend_from_slice(name_bytes);
	for _ in 0..name_pad {
		buf.push(0);
	}

	buf.extend_from_slice(&enc.nones);

	buf.extend_from_slice(&enc.data);

	buf.extend_from_slice(&enc.offsets);

	buf.extend_from_slice(&enc.extra);
}
