// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::DECIMAL128_MAX_PRECISION,
	decimal::{Decimal, unscaled::digits},
	int::Int,
	uint::Uint,
	value_type::ValueType,
};

use crate::{error::DecodeError, tag::ValueKind};

pub(crate) const NARROW: usize = 16;
pub(crate) const WIDE: usize = 32;

pub(crate) fn width(precision: Precision) -> usize {
	if precision.value() <= DECIMAL128_MAX_PRECISION {
		NARROW
	} else {
		WIDE
	}
}

pub(crate) fn int_unscaled(value: &Int, precision: Precision) -> Option<i256> {
	(value.digits() <= precision.value()).then(|| value.to_i256())
}

pub(crate) fn uint_unscaled(value: &Uint, precision: Precision) -> Option<i256> {
	(value.digits() <= precision.value()).then(|| value.to_i256())
}

pub(crate) fn decimal_unscaled(value: &Decimal, precision: Precision, scale: Scale) -> Option<i256> {
	value.fits(precision.value(), scale.value()).map(|fitted| fitted.unscaled())
}

pub(crate) fn write_le(value: i256, slot: &mut [u8]) {
	match slot.len() {
		NARROW => slot.copy_from_slice(
			&value.to_i128().expect("a value within precision 38 fits a 16 byte slot").to_le_bytes(),
		),
		WIDE => slot.copy_from_slice(&value.to_le_bytes()),
		other => unreachable!("an unscaled slot is 16 or 32 bytes, not {other}"),
	}
}

pub(crate) fn extend_le(value: i256, width: usize, buf: &mut Vec<u8>) {
	let start = buf.len();
	buf.resize(start + width, 0);
	write_le(value, &mut buf[start..]);
}

pub(crate) fn read_le(slot: &[u8]) -> i256 {
	match slot.len() {
		NARROW => {
			let mut bytes = [0u8; NARROW];
			bytes.copy_from_slice(slot);
			i256::from_i128(i128::from_le_bytes(bytes))
		}
		WIDE => {
			let mut bytes = [0u8; WIDE];
			bytes.copy_from_slice(slot);
			i256::from_le_bytes(bytes)
		}
		other => unreachable!("an unscaled slot is 16 or 32 bytes, not {other}"),
	}
}

pub(crate) fn params(ty: &ValueType) -> Option<(Precision, Scale)> {
	Some((ty.precision()?, ty.scale()?))
}

pub(crate) fn decode_params(precision: u8, scale: u8) -> Result<(Precision, Scale), DecodeError> {
	let precision = Precision::try_new(precision)
		.map_err(|error| DecodeError::InvalidData(format!("invalid precision {precision}: {error}")))?;
	let scale = Scale::try_new_with_precision(scale, precision)
		.map_err(|error| DecodeError::InvalidData(format!("invalid scale {scale}: {error}")))?;
	Ok((precision, scale))
}

pub(crate) fn family_type(kind: ValueKind, precision: Precision, scale: Scale) -> Result<ValueType, DecodeError> {
	match kind {
		ValueKind::Decimal => Ok(ValueType::decimal(precision, scale)),
		ValueKind::Int | ValueKind::Uint if scale.value() != 0 => Err(DecodeError::InvalidData(format!(
			"{kind:?} carries scale {} but must have scale 0",
			scale.value()
		))),
		ValueKind::Int => Ok(ValueType::int(precision)),
		ValueKind::Uint => Ok(ValueType::uint(precision)),
		other => Err(DecodeError::InvalidData(format!("{other:?} carries no precision or scale"))),
	}
}

pub(crate) fn check_unscaled(kind: ValueKind, value: i256, precision: Precision) -> Result<i256, DecodeError> {
	if digits(value) > precision.value() {
		return Err(DecodeError::InvalidData(format!(
			"{kind:?} value {value} has more digits than precision {}",
			precision.value()
		)));
	}
	if kind == ValueKind::Uint && value.is_negative() {
		return Err(DecodeError::InvalidData(format!("Uint value {value} is negative")));
	}
	Ok(value)
}
