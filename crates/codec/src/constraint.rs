// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::constraint::FFITypeConstraint;
use reifydb_value::value::{
	constraint::{
		Constraint, TypeConstraint, bytes::MaxBytes, dimension::Dimension, precision::Precision, scale::Scale,
	},
	dictionary::DictionaryId,
	sumtype::SumTypeId,
};

use crate::{
	error::{DecodeError, EncodeError},
	tag::TypeTag,
};

pub fn type_constraint_to_ffi(tc: &TypeConstraint) -> Result<FFITypeConstraint, EncodeError> {
	let base_type = TypeTag::of_type(&tc.get_type())?.byte();
	Ok(match tc.constraint() {
		None => FFITypeConstraint {
			base_type,
			constraint_type: 0,
			constraint_param1: 0,
			constraint_param2: 0,
		},
		Some(Constraint::MaxBytes(max)) => FFITypeConstraint {
			base_type,
			constraint_type: 1,
			constraint_param1: max.value(),
			constraint_param2: 0,
		},
		Some(Constraint::PrecisionScale(p, s)) => FFITypeConstraint {
			base_type,
			constraint_type: 2,
			constraint_param1: p.value() as u32,
			constraint_param2: s.value() as u32,
		},
		Some(Constraint::Dictionary(dict_id, id_type)) => FFITypeConstraint {
			base_type,
			constraint_type: 3,
			constraint_param1: dict_id.to_u64() as u32,
			constraint_param2: TypeTag::of_type(id_type)?.byte() as u32,
		},
		Some(Constraint::SumType(id)) => FFITypeConstraint {
			base_type,
			constraint_type: 4,
			constraint_param1: id.to_u64() as u32,
			constraint_param2: 0,
		},
		Some(Constraint::Dimension(dims)) => FFITypeConstraint {
			base_type,
			constraint_type: 5,
			constraint_param1: dims.value(),
			constraint_param2: 0,
		},
	})
}

pub fn type_constraint_from_ffi(ffi: &FFITypeConstraint) -> Result<TypeConstraint, DecodeError> {
	let ty = TypeTag::from_byte(ffi.base_type)?.to_type()?;
	Ok(match ffi.constraint_type {
		1 => TypeConstraint::with_constraint(ty, Constraint::MaxBytes(MaxBytes::new(ffi.constraint_param1))),
		2 => TypeConstraint::with_constraint(
			ty,
			Constraint::PrecisionScale(
				Precision::new(ffi.constraint_param1 as u8),
				Scale::new(ffi.constraint_param2 as u8),
			),
		),
		3 => TypeConstraint::with_constraint(
			ty,
			Constraint::Dictionary(
				DictionaryId::from(ffi.constraint_param1 as u64),
				TypeTag::from_byte(ffi.constraint_param2 as u8)?.to_type()?,
			),
		),
		4 => TypeConstraint::with_constraint(
			ty,
			Constraint::SumType(SumTypeId::from(ffi.constraint_param1 as u64)),
		),
		5 => {
			let dims = Dimension::try_new(ffi.constraint_param1).map_err(|e| {
				DecodeError::InvalidData(format!("invalid vector dimension in type constraint: {e}"))
			})?;
			TypeConstraint::with_constraint(ty, Constraint::Dimension(dims))
		}
		_ => TypeConstraint::unconstrained(ty),
	})
}
