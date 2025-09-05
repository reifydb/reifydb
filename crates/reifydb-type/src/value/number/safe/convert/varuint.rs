// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Apply VarUint conversion macros for all target types
impl_safe_convert_varuint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varuint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varuint_to_float!(f32, f64);

// Direct implementations for VarUint conversions
impl SafeConvert<VarInt> for VarUint {
	fn checked_convert(self) -> Option<VarInt> {
		Some(VarInt(self.0))
	}

	fn saturating_convert(self) -> VarInt {
		VarInt(self.0)
	}

	fn wrapping_convert(self) -> VarInt {
		VarInt(self.0)
	}
}

impl SafeConvert<Decimal> for VarUint {
	fn checked_convert(self) -> Option<Decimal> {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.ok()
	}

	fn saturating_convert(self) -> Decimal {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.unwrap_or_else(|_| {
				Decimal::from_i64(0, 38, 0).unwrap()
			})
	}

	fn wrapping_convert(self) -> Decimal {
		self.saturating_convert()
	}
}

#[cfg(test)]
mod tests {
	// VarUint conversion tests would go here if needed
	// The original file doesn't have specific tests for VarUint conversions
}
