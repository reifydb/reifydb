// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod any;
mod fixed;
mod varlen;

use arrow_buffer::BooleanBuffer;
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	diff_type::DiffType,
	frame::frame::Frame,
};
use tracing::{Span, instrument};

use crate::{
	error::EncodeError,
	frame::{
		encoding::plain::{encode_bitvec, encode_plain},
		format::{
			COL_FLAG_HAS_NONES, Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE, RBCF_MAGIC, RBCF_VERSION,
		},
		heuristics::choose_encoding,
		options::EncodeOptions,
	},
	tag::TypeTag,
	typeinfo::encode_digest_params,
	unscaled::params,
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
	level = "trace",
	skip_all,
	fields(
		frame_count = frames.len(),
		total_rows = frames.iter().map(|f| f.batch.num_rows()).sum::<usize>(),
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
	let row_count = frame.batch.num_rows() as u32;
	let column_count = frame.batch.num_columns() as u16;

	reserve_frame_header(buf);
	encode_frame_columns(frame, buf, options)?;

	let frame_size = (buf.len() - frame_start) as u32;
	let op = frame.op.map_or(0, DiffType::as_u8);
	write_frame_header(buf, frame_start, row_count, column_count, op, frame_size);
	Ok(())
}

#[inline]
fn reserve_frame_header(buf: &mut Vec<u8>) {
	buf.extend_from_slice(&[0u8; FRAME_HEADER_SIZE]);
}

#[inline]
fn encode_frame_columns(frame: &Frame, buf: &mut Vec<u8>, options: &EncodeOptions) -> Result<(), EncodeError> {
	let schema = frame.batch.schema_ref();
	for (field, array) in schema.fields().iter().zip(frame.batch.columns()) {
		let view = ColumnView::try_from((array, field.as_ref()))
			.map_err(|error| EncodeError::UnsupportedType(error.to_string()))?;
		encode_column(field.name(), &view, buf, options)?;
	}
	Ok(())
}

#[inline]
fn write_frame_header(buf: &mut [u8], frame_start: usize, row_count: u32, column_count: u16, op: u8, frame_size: u32) {
	let h = frame_start;
	buf[h..h + 4].copy_from_slice(&row_count.to_le_bytes());
	buf[h + 4..h + 6].copy_from_slice(&column_count.to_le_bytes());
	buf[h + 6] = 0;
	buf[h + 7] = op;
	buf[h + 8..h + 12].copy_from_slice(&frame_size.to_le_bytes());
}

fn encode_column(
	name: &str,
	view: &ColumnView<'_>,
	buf: &mut Vec<u8>,
	options: &EncodeOptions,
) -> Result<(), EncodeError> {
	let desired = options.force_encoding.unwrap_or_else(|| choose_encoding(view, options.compression));
	let enc = try_encode_with(view, desired)?;
	write_column(name, &enc, buf);
	Ok(())
}

fn try_encode_with(view: &ColumnView<'_>, desired: Encoding) -> Result<EncodedColumn, EncodeError> {
	let row_count = view.len();
	let has_nones = view.is_nullable() || view.is_none();
	let nones = match (has_nones, view.logical_nulls()) {
		(false, _) => Vec::new(),
		(true, Some(nulls)) => encode_bitvec(nulls.inner()),
		(true, None) => encode_bitvec(&BooleanBuffer::new_set(row_count)),
	};

	let row_count = row_count as u32;

	let result = match desired {
		Encoding::Dict => varlen::try_dict_varlen(view),
		Encoding::Rle => fixed::try_rle_fixed(view),
		Encoding::Delta => fixed::try_delta_fixed(view),
		Encoding::DeltaRle => fixed::try_delta_rle_fixed(view),
		_ => None,
	};

	let mut enc = match result {
		Some(enc) => enc,
		None => {
			let plain = encode_plain(view)?;
			let mut extra = Vec::new();
			if let ViewData::Digest {
				inner: digest_inner,
				accuracy,
				..
			} = &view.data
			{
				encode_digest_params(digest_inner, *accuracy, &mut extra)?;
			}
			EncodedColumn {
				type_code: plain.type_code,
				encoding: Encoding::Plain,
				flags: 0,
				nones: vec![],
				data: plain.data,
				offsets: plain.offsets,
				extra,
				row_count,
			}
		}
	};

	if let Some((precision, scale)) = params(&view.base_type()) {
		enc.extra = vec![precision.value(), scale.value()];
	}

	if has_nones {
		enc.type_code = TypeTag::of_type(&view.get_type())?.byte();
		enc.nones = nones;
		enc.flags |= COL_FLAG_HAS_NONES;
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
