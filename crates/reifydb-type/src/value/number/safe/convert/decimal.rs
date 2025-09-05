// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Apply Decimal conversion macros for all target types
impl_safe_convert_decimal_to_int!(
	i8, i16, i32, i64, i128, u8, u16, u32, u64, u128
);
impl_safe_convert_decimal_to_float!(f32, f64);

// Direct implementations for Decimal conversions
impl SafeConvert<VarInt> for Decimal {
	fn checked_convert(self) -> Option<VarInt> {
		if let Some(big_int) = self.inner().to_bigint() {
			Some(VarInt(big_int))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarInt {
		self.checked_convert().unwrap_or(VarInt::zero())
	}

	fn wrapping_convert(self) -> VarInt {
		self.saturating_convert()
	}
}

impl SafeConvert<VarUint> for Decimal {
	fn checked_convert(self) -> Option<VarUint> {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				Some(VarUint(big_int))
			} else {
				None
			}
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				VarUint(big_int)
			} else {
				VarUint::zero()
			}
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		if let Some(big_int) = self.inner().to_bigint() {
			VarUint(big_int.abs())
		} else {
			VarUint::zero()
		}
	}
}

#[cfg(test)]
mod tests {
	// Decimal conversion tests would go here if needed
	// The original file doesn't have specific tests for Decimal conversions
}
