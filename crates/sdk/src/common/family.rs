// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
use reifydb_codec::{
	extern_c::cells::{decode_decimal_cell, encode_decimal_cell},
	tag::ValueKind,
};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::{DECIMAL128_MAX_PRECISION, DecimalArray},
	decimal::Decimal,
	value_type::ValueType,
};

use crate::error::SdkError;

pub trait FamilyValue: Sized {
	const KIND: ValueKind;

	fn encode_cell(&self, precision: Precision, scale: Scale, buf: &mut Vec<u8>) -> Result<(), SdkError>;

	fn decode_cell(cell: &[u8], scale: Scale) -> Result<Self, SdkError>;

	fn unscaled_digits(&self) -> (i256, u8);
}

impl FamilyValue for Decimal {
	const KIND: ValueKind = ValueKind::Decimal;

	fn encode_cell(&self, precision: Precision, scale: Scale, buf: &mut Vec<u8>) -> Result<(), SdkError> {
		encode_decimal_cell(self, precision, scale, buf).map_err(|e| SdkError::InvalidInput(e.to_string()))
	}

	fn decode_cell(cell: &[u8], scale: Scale) -> Result<Self, SdkError> {
		decode_decimal_cell(cell, scale).map_err(|e| SdkError::InvalidInput(e.to_string()))
	}

	fn unscaled_digits(&self) -> (i256, u8) {
		(self.unscaled(), self.digits())
	}
}

pub fn is_family(kind: ValueKind) -> bool {
	matches!(kind, ValueKind::Decimal)
}

pub fn family_params(kind: ValueKind, precision: u8, scale: u8) -> Option<(Precision, Scale)> {
	if !is_family(kind) {
		return None;
	}
	let precision = Precision::try_new(precision).ok()?;
	let scale = Scale::try_new_with_precision(scale, precision).ok()?;
	Some((precision, scale))
}

pub fn default_family_params(kind: ValueKind) -> Option<(Precision, Scale)> {
	let ty = match kind {
		ValueKind::Decimal => ValueType::DECIMAL,
		_ => return None,
	};
	Some((ty.precision()?, ty.scale()?))
}

pub fn cell_width(precision: Precision) -> usize {
	if precision.value() <= DECIMAL128_MAX_PRECISION {
		16
	} else {
		32
	}
}

pub fn column_params(data: &ColumnBuffer) -> (u8, u8) {
	match data {
		ColumnBuffer::Decimal(array) => (array.precision().value(), array.scale().value()),
		_ => (0, 0),
	}
}

pub fn decode_family_column(
	kind: ValueKind,
	precision: Precision,
	scale: Scale,
	data: &[u8],
	row_count: usize,
) -> Result<ColumnBuffer, SdkError> {
	let width = cell_width(precision);
	let needed = row_count
		.checked_mul(width)
		.ok_or_else(|| SdkError::InvalidInput(format!("{kind:?} column of {row_count} rows is too large")))?;
	if data.len() < needed {
		return Err(SdkError::InvalidInput(format!(
			"{kind:?} column sent {} data bytes for {row_count} rows of {width} bytes each",
			data.len()
		)));
	}
	let cells = data[..needed].chunks_exact(width);
	let unscaled = match kind {
		ValueKind::Decimal => unscaled_cells::<Decimal>(cells, precision, scale)?,
		other => {
			return Err(SdkError::InvalidInput(format!("{other:?} is not an int, uint or decimal column")));
		}
	};
	let array = DecimalArray::from_unscaled(precision, scale, unscaled);
	Ok(match kind {
		_ => ColumnBuffer::Decimal(array),
	})
}

fn unscaled_cells<'a, T: FamilyValue>(
	cells: impl Iterator<Item = &'a [u8]>,
	precision: Precision,
	scale: Scale,
) -> Result<Vec<i256>, SdkError> {
	cells.enumerate()
		.map(|(row, cell)| {
			let value = T::decode_cell(cell, scale).map_err(|e| {
				SdkError::InvalidInput(format!("{:?} cell {row} does not decode: {e}", T::KIND))
			})?;
			let (unscaled, digits) = value.unscaled_digits();
			if digits > precision.value() {
				return Err(SdkError::InvalidInput(format!(
					"{:?} cell {row} has {digits} digits, more than precision {}",
					T::KIND,
					precision.value()
				)));
			}
			Ok(unscaled)
		})
		.collect()
}
