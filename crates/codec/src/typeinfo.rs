// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::from_utf8;

use reifydb_value::value::value_type::ValueType;

use crate::{
	error::{DecodeError, EncodeError},
	reader::Reader,
	tag::{EXTENDED_TYPE_TAG, MAX_OPTION_DEPTH, TypeTag, ValueKind, peel_options},
};

pub fn encode_value_type(ty: &ValueType, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	let (base, depth) = peel_options(ty);
	if depth > MAX_OPTION_DEPTH as u32 {
		if depth > u8::MAX as u32 {
			return Err(EncodeError::OptionDepthTooDeep {
				depth,
				max: u8::MAX,
			});
		}
		buf.push(EXTENDED_TYPE_TAG);
		buf.push(depth as u8);
		encode_base(base, 0, buf)
	} else {
		encode_base(base, depth as u8, buf)
	}
}

fn encode_base(base: &ValueType, depth: u8, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	let tag = TypeTag::new(ValueKind::of_type(base), depth)?;
	buf.push(tag.byte());
	match base {
		ValueType::List(element) => encode_value_type(element, buf)?,
		ValueType::Record(fields) => {
			buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());
			for (name, field_ty) in fields {
				let name_bytes = name.as_bytes();
				buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
				buf.extend_from_slice(name_bytes);
				encode_value_type(field_ty, buf)?;
			}
		}
		ValueType::Tuple(elements) => {
			buf.extend_from_slice(&(elements.len() as u16).to_le_bytes());
			for element in elements {
				encode_value_type(element, buf)?;
			}
		}

		ValueType::Vector(dims) => buf.extend_from_slice(&dims.to_le_bytes()),
		_ => {}
	}
	Ok(())
}

pub fn decode_value_type(r: &mut Reader) -> Result<ValueType, DecodeError> {
	let first = r.u8()?;
	if first == EXTENDED_TYPE_TAG {
		let depth = r.u8()?;
		let base = decode_value_type(r)?;
		return Ok((0..depth).fold(base, |ty, _| ValueType::Option(Box::new(ty))));
	}
	let tag = TypeTag::from_byte(first)?;
	let kind = tag.kind().ok_or(DecodeError::UnknownTypeCode(first))?;
	let base = match kind {
		ValueKind::List => ValueType::List(Box::new(decode_value_type(r)?)),
		ValueKind::Record => {
			let count = r.u16()?;
			let mut fields = Vec::with_capacity(count as usize);
			for _ in 0..count {
				let name_len = r.u16()? as usize;
				let name = from_utf8(r.take(name_len)?)
					.map_err(|e| {
						DecodeError::InvalidData(format!(
							"invalid UTF-8 in record field name: {e}"
						))
					})?
					.to_string();
				fields.push((name, decode_value_type(r)?));
			}
			ValueType::Record(fields)
		}
		ValueKind::Tuple => {
			let count = r.u16()?;
			let mut elements = Vec::with_capacity(count as usize);
			for _ in 0..count {
				elements.push(decode_value_type(r)?);
			}
			ValueType::Tuple(elements)
		}
		ValueKind::Vector => ValueType::Vector(r.u32()?),
		_ => return tag.to_type(),
	};
	Ok((0..tag.depth()).fold(base, |ty, _| ValueType::Option(Box::new(ty))))
}
