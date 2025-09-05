// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Apply VarInt conversion macros for all target types
impl_safe_convert_varint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varint_to_float!(f32, f64);

// Direct implementations for VarInt conversions
impl SafeConvert<VarUint> for VarInt {
	fn checked_convert(self) -> Option<VarUint> {
		if self.0 >= BigInt::from(0) {
			Some(VarUint(self.0))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if self.0 >= BigInt::from(0) {
			VarUint(self.0)
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		VarUint(self.0.abs())
	}
}

impl SafeConvert<Decimal> for VarInt {
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
	// VarInt conversion tests would go here if needed
	// The original file doesn't have specific tests for VarInt conversions
}
