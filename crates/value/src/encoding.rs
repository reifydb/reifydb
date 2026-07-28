// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use uuid::Uuid;

use crate::value::{
	date::Date,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	partition::Partition,
	row_number::RowNumber,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

pub trait LeBytes: Sized {
	type Bytes: AsRef<[u8]> + AsMut<[u8]> + Default + Copy;

	const ENCODED_SIZE: usize = size_of::<Self::Bytes>();

	fn to_le_bytes(&self) -> Self::Bytes;

	fn from_le_bytes(bytes: Self::Bytes) -> Self;

	#[inline]
	fn write_le(&self, dst: &mut [u8]) {
		dst[..Self::ENCODED_SIZE].copy_from_slice(self.to_le_bytes().as_ref());
	}

	#[inline]
	fn read_le(src: &[u8]) -> Self {
		let mut buf = Self::Bytes::default();
		buf.as_mut().copy_from_slice(&src[..Self::ENCODED_SIZE]);
		Self::from_le_bytes(buf)
	}
}

macro_rules! le_bytes_for_primitive {
	($($ty:ty),* $(,)?) => {
		$(
			impl LeBytes for $ty {
				type Bytes = [u8; size_of::<$ty>()];

				#[inline]
				fn to_le_bytes(&self) -> Self::Bytes {
					<$ty>::to_le_bytes(*self)
				}

				#[inline]
				fn from_le_bytes(bytes: Self::Bytes) -> Self {
					<$ty>::from_le_bytes(bytes)
				}
			}
		)*
	};
}

le_bytes_for_primitive!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl LeBytes for bool {
	type Bytes = [u8; 1];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		[*self as u8]
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		bytes[0] != 0
	}
}

impl LeBytes for DateTime {
	type Bytes = [u8; 8];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		self.to_nanos().to_le_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		DateTime::from_nanos(u64::from_le_bytes(bytes))
	}
}

impl LeBytes for Date {
	type Bytes = [u8; 4];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		self.to_days_since_epoch().to_le_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		Date::from_days_since_epoch(i32::from_le_bytes(bytes)).expect("stored date must be valid")
	}
}

impl LeBytes for Time {
	type Bytes = [u8; 8];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		self.to_nanos_since_midnight().to_le_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		Time::from_nanos_since_midnight(u64::from_le_bytes(bytes)).expect("stored time must be valid")
	}
}

impl LeBytes for Duration {
	type Bytes = [u8; 16];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		let mut out = [0u8; 16];
		out[0..4].copy_from_slice(&self.get_months().to_le_bytes());
		out[4..8].copy_from_slice(&self.get_days().to_le_bytes());
		out[8..16].copy_from_slice(&self.get_nanos().to_le_bytes());
		out
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		let months = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
		let days = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
		let nanos = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
		Duration::new(months, days, nanos).expect("stored duration must be valid")
	}
}

impl LeBytes for RowNumber {
	type Bytes = [u8; 8];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		self.0.to_le_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		RowNumber(u64::from_le_bytes(bytes))
	}
}

impl LeBytes for Partition {
	type Bytes = [u8; 16];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		self.0.to_le_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		Partition(u128::from_le_bytes(bytes))
	}
}

impl LeBytes for Uuid4 {
	type Bytes = [u8; 16];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		*self.0.as_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		Uuid4(Uuid::from_bytes(bytes))
	}
}

impl LeBytes for Uuid7 {
	type Bytes = [u8; 16];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		*self.0.as_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		Uuid7(Uuid::from_bytes(bytes))
	}
}

impl LeBytes for IdentityId {
	type Bytes = [u8; 16];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		*self.0.0.as_bytes()
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		IdentityId(Uuid7(Uuid::from_bytes(bytes)))
	}
}

pub trait RowField: LeBytes {
	const VALUE_TYPE: ValueType;
}

macro_rules! row_field {
	($($ty:ty => $variant:ident),* $(,)?) => {
		$(
			impl RowField for $ty {
				const VALUE_TYPE: ValueType = ValueType::$variant;
			}
		)*
	};
}

row_field!(
	bool => Boolean,
	f32 => Float4,
	f64 => Float8,
	i8 => Int1,
	i16 => Int2,
	i32 => Int4,
	i64 => Int8,
	i128 => Int16,
	u8 => Uint1,
	u16 => Uint2,
	u32 => Uint4,
	u64 => Uint8,
	u128 => Uint16,
	Date => Date,
	DateTime => DateTime,
	Time => Time,
	Duration => Duration,
	Uuid4 => Uuid4,
	Uuid7 => Uuid7,
	IdentityId => IdentityId,
);

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_row_field_declares_the_value_type_its_own_bytes_fill() {
		// Intent: RowField is what lets `set::<T>` choose the slot type without the caller naming
		// it, so a T mapped to the wrong ValueType writes T's bytes into a slot sized for
		// something else. That corruption is silent in release, because the slot-type check only
		// runs under reifydb_assertions - which makes pinning the two against each other here the
		// only thing standing between a one-character mapping typo and a wrong-width row write.
		// Mutation: map u32 to Uint8, or Date to DateTime, and the widths stop agreeing.
		fn agree<T: RowField>() {
			assert_eq!(
				T::VALUE_TYPE.size(),
				T::ENCODED_SIZE,
				"{:?} declares a {}-byte slot but its LeBytes form is {} bytes",
				T::VALUE_TYPE,
				T::VALUE_TYPE.size(),
				T::ENCODED_SIZE
			);
		}

		agree::<bool>();
		agree::<f32>();
		agree::<f64>();
		agree::<i8>();
		agree::<i16>();
		agree::<i32>();
		agree::<i64>();
		agree::<i128>();
		agree::<u8>();
		agree::<u16>();
		agree::<u32>();
		agree::<u64>();
		agree::<u128>();
		agree::<Date>();
		agree::<DateTime>();
		agree::<Time>();
		agree::<Duration>();
		agree::<Uuid4>();
		agree::<Uuid7>();
		agree::<IdentityId>();
	}

	#[test]
	fn every_width_is_the_size_of_its_own_byte_array() {
		// Intent: ENCODED_SIZE is derived, not declared, so the width and the bytes cannot drift
		// apart. This is what makes widening a type a one-line change: every layout that reads
		// ENCODED_SIZE follows the array.
		// Mutation: give the trait a hand-written ENCODED_SIZE per impl and one of them can
		// disagree with its own Bytes without failing to compile.
		assert_eq!(<u8 as LeBytes>::ENCODED_SIZE, 1);
		assert_eq!(<bool as LeBytes>::ENCODED_SIZE, 1);
		assert_eq!(<Date as LeBytes>::ENCODED_SIZE, 4);
		assert_eq!(<f32 as LeBytes>::ENCODED_SIZE, 4);
		assert_eq!(<DateTime as LeBytes>::ENCODED_SIZE, 8);
		assert_eq!(<Time as LeBytes>::ENCODED_SIZE, 8);
		assert_eq!(<RowNumber as LeBytes>::ENCODED_SIZE, 8);
		assert_eq!(<Duration as LeBytes>::ENCODED_SIZE, 16);
		assert_eq!(<Partition as LeBytes>::ENCODED_SIZE, 16);
		assert_eq!(<Uuid4 as LeBytes>::ENCODED_SIZE, 16);
		assert_eq!(<Uuid7 as LeBytes>::ENCODED_SIZE, 16);
		assert_eq!(<IdentityId as LeBytes>::ENCODED_SIZE, 16);
	}

	#[test]
	fn byte_order_is_little_endian_regardless_of_host() {
		// Intent: THE property this trait exists for. A native-endian store reads back correctly
		// on the machine that wrote it and wrong everywhere else, and nothing in a round-trip test
		// can see that - both directions would use the same wrong order. Only a pinned byte
		// pattern catches it, so these assert the bytes rather than the value.
		// Mutation: swap any impl to to_be_bytes, or narrow a slot, and this fails.
		// Limit worth naming: on a little-endian host to_ne_bytes IS to_le_bytes, so that
		// substitution cannot be caught by any runtime assertion here. It surfaces only when
		// built for a big-endian target - which is precisely where the bug would bite, so do not
		// read a green run as proof that no impl reaches for native order.
		assert_eq!(0x0102_0304_0506_0708u64.to_le_bytes(), [8, 7, 6, 5, 4, 3, 2, 1]);
		assert_eq!(
			LeBytes::to_le_bytes(&DateTime::from_nanos(0x0102_0304_0506_0708)),
			[8, 7, 6, 5, 4, 3, 2, 1]
		);
		assert_eq!(LeBytes::to_le_bytes(&RowNumber(0x0102_0304_0506_0708)), [8, 7, 6, 5, 4, 3, 2, 1]);
		assert_eq!(LeBytes::to_le_bytes(&0x0102_0304i32), [4, 3, 2, 1]);
	}

	#[test]
	fn every_implementor_round_trips_through_its_bytes() {
		// Mutation: read one field of Duration at the wrong offset and only that component
		// survives the round trip.
		assert_eq!(bool::from_le_bytes(LeBytes::to_le_bytes(&true)), true);
		assert_eq!(bool::from_le_bytes(LeBytes::to_le_bytes(&false)), false);

		let dt = DateTime::from_nanos(1_700_000_123_456_789);
		assert_eq!(DateTime::from_le_bytes(LeBytes::to_le_bytes(&dt)), dt);

		let duration = Duration::new(13, 7, 1_234_567_890).unwrap();
		assert_eq!(Duration::from_le_bytes(LeBytes::to_le_bytes(&duration)), duration);

		let date = Date::from_days_since_epoch(19_000).unwrap();
		assert_eq!(Date::from_le_bytes(LeBytes::to_le_bytes(&date)), date);

		let time = Time::from_nanos_since_midnight(86_399_999_999_999).unwrap();
		assert_eq!(Time::from_le_bytes(LeBytes::to_le_bytes(&time)), time);

		let partition = Partition(0xdead_beef_cafe_babe_0123_4567_89ab_cdef);
		assert_eq!(Partition::from_le_bytes(LeBytes::to_le_bytes(&partition)), partition);
	}

	#[test]
	fn the_slice_helpers_agree_with_the_array_form() {
		// Intent: write_le/read_le are what the codec calls against a slot at an offset; they must
		// be the same bytes as the array form or the two paths diverge silently.
		// Mutation: have write_le skip the ENCODED_SIZE bound and it writes past its slot.
		let mut buf = [0u8; 32];
		let dt = DateTime::from_nanos(1_700_000_123_456_789);

		dt.write_le(&mut buf[8..]);
		assert_eq!(&buf[8..16], LeBytes::to_le_bytes(&dt).as_ref());
		assert_eq!(DateTime::read_le(&buf[8..]), dt);
		assert!(buf[0..8].iter().all(|b| *b == 0), "write_le must not reach before its slot");
		assert!(buf[16..].iter().all(|b| *b == 0), "write_le must not reach past its slot");
	}
}
