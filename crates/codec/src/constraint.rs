// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
	dictionary::DictionaryId,
	sumtype::SumTypeId,
};

use crate::{
	error::{DecodeError, EncodeError},
	tag::TypeTag,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedTypeConstraint {
	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,
}

pub fn encode_type_constraint(tc: &TypeConstraint) -> Result<EncodedTypeConstraint, EncodeError> {
	let base_type = TypeTag::of_type(&tc.get_type())?.byte();
	Ok(match tc.constraint() {
		None => EncodedTypeConstraint {
			base_type,
			constraint_type: 0,
			constraint_param1: 0,
			constraint_param2: 0,
		},
		Some(Constraint::MaxBytes(max)) => EncodedTypeConstraint {
			base_type,
			constraint_type: 1,
			constraint_param1: max.value(),
			constraint_param2: 0,
		},
		Some(Constraint::PrecisionScale(p, s)) => EncodedTypeConstraint {
			base_type,
			constraint_type: 2,
			constraint_param1: p.value() as u32,
			constraint_param2: s.value() as u32,
		},
		Some(Constraint::Dictionary(dict_id, id_type)) => EncodedTypeConstraint {
			base_type,
			constraint_type: 3,
			constraint_param1: dict_id.to_u64() as u32,
			constraint_param2: TypeTag::of_type(id_type)?.byte() as u32,
		},
		Some(Constraint::SumType(id)) => EncodedTypeConstraint {
			base_type,
			constraint_type: 4,
			constraint_param1: id.to_u64() as u32,
			constraint_param2: 0,
		},
	})
}

pub fn decode_type_constraint(encoded: &EncodedTypeConstraint) -> Result<TypeConstraint, DecodeError> {
	let ty = TypeTag::from_byte(encoded.base_type)?.to_type()?;
	Ok(match encoded.constraint_type {
		1 => TypeConstraint::with_constraint(
			ty,
			Constraint::MaxBytes(MaxBytes::new(encoded.constraint_param1)),
		),
		2 => TypeConstraint::with_constraint(
			ty,
			Constraint::PrecisionScale(
				Precision::new(encoded.constraint_param1 as u8),
				Scale::new(encoded.constraint_param2 as u8),
			),
		),
		3 => TypeConstraint::with_constraint(
			ty,
			Constraint::Dictionary(
				DictionaryId::from(encoded.constraint_param1 as u64),
				TypeTag::from_byte(encoded.constraint_param2 as u8)?.to_type()?,
			),
		),
		4 => TypeConstraint::with_constraint(
			ty,
			Constraint::SumType(SumTypeId::from(encoded.constraint_param1 as u64)),
		),
		_ => TypeConstraint::unconstrained(ty),
	})
}
