// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes},
	dictionary::DictionaryId,
	sumtype::SumTypeId,
};

use crate::{
	error::{DecodeError, EncodeError},
	reader::Reader,
	typeinfo::{decode_value_type, encode_value_type},
};

pub fn encode_type_constraint_info(tc: &TypeConstraint, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	encode_value_type(&tc.get_type(), buf)?;
	encode_constraint_info(tc.constraint().as_ref(), buf)
}

pub fn decode_type_constraint_info(r: &mut Reader) -> Result<TypeConstraint, DecodeError> {
	let ty = decode_value_type(r)?;
	Ok(match decode_constraint_info(r)? {
		Some(constraint) => TypeConstraint::with_constraint(ty, constraint),
		None => TypeConstraint::unconstrained(ty),
	})
}

pub fn encode_constraint_info(constraint: Option<&Constraint>, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	match constraint {
		None => buf.push(0),
		Some(Constraint::MaxBytes(max)) => {
			buf.push(1);
			buf.extend_from_slice(&max.value().to_le_bytes());
		}
		Some(Constraint::Dictionary(dict_id, id_type)) => {
			buf.push(3);
			buf.extend_from_slice(&dict_id.to_u64().to_le_bytes());
			encode_value_type(id_type, buf)?;
		}
		Some(Constraint::SumType(id)) => {
			buf.push(4);
			buf.extend_from_slice(&id.to_u64().to_le_bytes());
		}
	}
	Ok(())
}

pub fn decode_constraint_info(r: &mut Reader) -> Result<Option<Constraint>, DecodeError> {
	Ok(match r.u8()? {
		0 => None,
		1 => Some(Constraint::MaxBytes(MaxBytes::new(r.u32()?))),
		3 => {
			let dict_id = DictionaryId::from(r.u64()?);
			Some(Constraint::Dictionary(dict_id, decode_value_type(r)?))
		}
		4 => Some(Constraint::SumType(SumTypeId::from(r.u64()?))),
		other => return Err(DecodeError::InvalidData(format!("unknown constraint tag {other}"))),
	})
}
