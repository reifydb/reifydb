// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

use crate::error::EncodeError;

pub(crate) fn encode_any_value(val: &Value, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	let type_tag = val.get_type().to_u8();
	buf.push(type_tag);
	match val {
		Value::None {
			..
		} => {}
		Value::Boolean(b) => buf.push(if *b {
			1
		} else {
			0
		}),
		Value::Float4(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Float8(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Int1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Date(d) => buf.extend_from_slice(&d.to_days_since_epoch().to_le_bytes()),
		Value::DateTime(dt) => buf.extend_from_slice(&dt.to_nanos().to_le_bytes()),
		Value::Time(t) => buf.extend_from_slice(&t.to_nanos_since_midnight().to_le_bytes()),
		Value::Duration(d) => {
			buf.extend_from_slice(&d.get_months().to_le_bytes());
			buf.extend_from_slice(&d.get_days().to_le_bytes());
			buf.extend_from_slice(&d.get_nanos().to_le_bytes());
		}
		Value::IdentityId(id) => buf.extend_from_slice(id.0.0.as_bytes()),
		Value::Uuid4(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Uuid7(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Utf8(s) => {
			let bytes = s.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		Value::Blob(b) => {
			let bytes = b.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		Value::Int(v) => {
			let bytes = v.0.to_signed_bytes_le();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(&bytes);
		}
		Value::Uint(v) => {
			let bytes = v.0.to_signed_bytes_le();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(&bytes);
		}
		Value::Decimal(v) => {
			let s = v.to_string();
			let bytes = s.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		_ => {
			return Err(EncodeError::UnsupportedType(format!("{:?}", val.get_type())));
		}
	}
	Ok(())
}
