// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
	dictionary::DictionaryId,
	digest::Digest,
	sumtype::SumTypeId,
	value_type::ValueType,
};

use crate::{
	error::{DecodeError, EncodeError},
	tag::{TypeTag, ValueKind, peel_options},
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
	let ty = tc.get_type();
	let base_type = TypeTag::of_type(&ty)?.byte();
	if let (
		ValueType::Digest {
			inner,
			accuracy,
		},
		_,
	) = peel_options(&ty)
	{
		if let Some(constraint) = tc.constraint() {
			return Err(EncodeError::UnsupportedType(format!(
				"{ty} cannot carry the {constraint:?} constraint"
			)));
		}
		Digest::new((**inner).clone(), *accuracy)
			.map_err(|error| EncodeError::UnsupportedType(error.to_string()))?;
		return Ok(EncodedTypeConstraint {
			base_type,
			constraint_type: 5,
			constraint_param1: *accuracy,
			constraint_param2: u32::from(TypeTag::of_type(inner)?.byte()),
		});
	}
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
	let tag = TypeTag::from_byte(encoded.base_type)?;
	if encoded.constraint_type == 5 {
		return decode_digest_type(tag, encoded).map(TypeConstraint::unconstrained);
	}
	let ty = tag.to_type()?;
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

fn decode_digest_type(tag: TypeTag, encoded: &EncodedTypeConstraint) -> Result<ValueType, DecodeError> {
	if tag.kind() != Some(ValueKind::Digest) {
		return Err(DecodeError::InvalidData(format!(
			"digest constraint on base type tag 0x{:02X}, which is not a digest",
			encoded.base_type
		)));
	}
	let inner_byte = u8::try_from(encoded.constraint_param2).map_err(|_| {
		DecodeError::InvalidData(format!(
			"digest inner type tag {} does not fit in a type tag byte",
			encoded.constraint_param2
		))
	})?;
	let inner = TypeTag::from_byte(inner_byte)?.to_type()?;
	Digest::new(inner.clone(), encoded.constraint_param1)
		.map_err(|error| DecodeError::InvalidData(format!("invalid digest type: {error}")))?;
	let base = ValueType::Digest {
		inner: Box::new(inner),
		accuracy: encoded.constraint_param1,
	};
	Ok((0..tag.depth()).fold(base, |ty, _| ValueType::Option(Box::new(ty))))
}
