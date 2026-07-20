// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use rkyv::{
	Archive, Deserialize, Place, Serialize,
	rancor::Fallible,
	with::{ArchiveWith, DeserializeWith, SerializeWith},
};

#[derive(Archive, Serialize, Deserialize)]
pub struct BigIntRepr {
	sign: i8,
	magnitude_le: Vec<u8>,
}

impl BigIntRepr {
	fn from_bigint(value: &BigInt) -> Self {
		let (sign, magnitude_le) = value.to_bytes_le();
		let sign = match sign {
			Sign::Minus => -1,
			Sign::NoSign => 0,
			Sign::Plus => 1,
		};
		Self {
			sign,
			magnitude_le,
		}
	}

	fn to_bigint(sign: i8, magnitude_le: &[u8]) -> BigInt {
		let sign = if sign < 0 {
			Sign::Minus
		} else if sign == 0 {
			Sign::NoSign
		} else {
			Sign::Plus
		};
		BigInt::from_bytes_le(sign, magnitude_le)
	}
}

pub struct BigIntBytes;

impl ArchiveWith<BigInt> for BigIntBytes {
	type Archived = ArchivedBigIntRepr;
	type Resolver = (BigIntRepr, BigIntReprResolver);

	fn resolve_with(_: &BigInt, resolver: Self::Resolver, out: Place<Self::Archived>) {
		let (repr, inner) = resolver;
		repr.resolve(inner, out);
	}
}

impl<S: Fallible + ?Sized> SerializeWith<BigInt, S> for BigIntBytes
where
	BigIntRepr: Serialize<S>,
{
	fn serialize_with(field: &BigInt, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
		let repr = BigIntRepr::from_bigint(field);
		let resolver = repr.serialize(serializer)?;
		Ok((repr, resolver))
	}
}

impl<D: Fallible + ?Sized> DeserializeWith<ArchivedBigIntRepr, BigInt, D> for BigIntBytes {
	fn deserialize_with(archived: &ArchivedBigIntRepr, _: &mut D) -> Result<BigInt, D::Error> {
		Ok(BigIntRepr::to_bigint(archived.sign, archived.magnitude_le.as_slice()))
	}
}

#[derive(Archive, Serialize, Deserialize)]
pub struct BigDecimalRepr {
	digits: BigIntRepr,
	scale: i64,
}

pub struct BigDecimalBytes;

impl ArchiveWith<BigDecimal> for BigDecimalBytes {
	type Archived = ArchivedBigDecimalRepr;
	type Resolver = (BigDecimalRepr, BigDecimalReprResolver);

	fn resolve_with(_: &BigDecimal, resolver: Self::Resolver, out: Place<Self::Archived>) {
		let (repr, inner) = resolver;
		repr.resolve(inner, out);
	}
}

impl<S: Fallible + ?Sized> SerializeWith<BigDecimal, S> for BigDecimalBytes
where
	BigDecimalRepr: Serialize<S>,
{
	fn serialize_with(field: &BigDecimal, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
		let (digits, scale) = field.as_bigint_and_exponent();
		let repr = BigDecimalRepr {
			digits: BigIntRepr::from_bigint(&digits),
			scale,
		};
		let resolver = repr.serialize(serializer)?;
		Ok((repr, resolver))
	}
}

impl<D: Fallible + ?Sized> DeserializeWith<ArchivedBigDecimalRepr, BigDecimal, D> for BigDecimalBytes {
	fn deserialize_with(archived: &ArchivedBigDecimalRepr, _: &mut D) -> Result<BigDecimal, D::Error> {
		let digits = BigIntRepr::to_bigint(archived.digits.sign, archived.digits.magnitude_le.as_slice());
		Ok(BigDecimal::new(digits, archived.scale.to_native()))
	}
}

#[cfg(test)]
mod tests {
	use bigdecimal::BigDecimal;
	use num_bigint::BigInt;
	use rkyv::{
		Archive, Deserialize, Serialize, access,
		de::Pool,
		deserialize,
		rancor::{Error, Strategy},
		to_bytes,
		with::{ArchiveWith, DeserializeWith, With},
	};

	use super::{BigDecimalBytes, BigIntBytes};

	#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
	struct Holder {
		#[rkyv(with = BigIntBytes)]
		int: BigInt,
		#[rkyv(with = BigDecimalBytes)]
		dec: BigDecimal,
	}

	fn round_trip(int: BigInt, dec: BigDecimal) {
		// Round-trip through archived bytes must be lossless for
		// sign, magnitude, and scale; a mismatch here silently
		// corrupts every persisted bignum Value.
		let value = Holder {
			int,
			dec,
		};
		let bytes = to_bytes::<Error>(&value).unwrap();
		let archived = access::<ArchivedHolder, Error>(&bytes).unwrap();
		let restored: Holder = deserialize::<Holder, Error>(archived).unwrap();
		assert_eq!(restored, value);
	}

	#[test]
	fn test_bignum_round_trip() {
		round_trip(BigInt::from(0), BigDecimal::from(0));
		round_trip(BigInt::from(-1), BigDecimal::try_from(-1.5).unwrap());
		round_trip(
			BigInt::from(i128::MAX) * BigInt::from(i128::MAX),
			BigDecimal::new(BigInt::from(123456789), -42),
		);
		round_trip(BigInt::from(i128::MIN), BigDecimal::new(BigInt::from(-987654321), 42));
	}

	#[test]
	fn test_corrupted_bytes_rejected() {
		// bytecheck must reject a truncated buffer as an error, not
		// panic or hand back garbage.
		let value = Holder {
			int: BigInt::from(42),
			dec: BigDecimal::from(7),
		};
		let bytes = to_bytes::<Error>(&value).unwrap();
		let truncated = &bytes[..bytes.len() / 2];
		assert!(access::<ArchivedHolder, Error>(truncated).is_err());
	}

	#[test]
	fn test_value_every_arm_round_trips() {
		// Every Value arm must survive archive -> access -> deserialize
		// unchanged; a lossy arm silently corrupts persisted operator
		// state built from that variant. All inputs fixed, no RNG.
		use uuid::Uuid as StdUuid;

		use crate::value::{
			Value,
			blob::Blob,
			date::Date,
			datetime::DateTime,
			decimal::Decimal,
			dictionary::DictionaryEntryId,
			duration::Duration,
			identity::IdentityId,
			int::Int,
			ordered_f32::OrderedF32,
			ordered_f64::OrderedF64,
			time::Time,
			uint::Uint,
			uuid::{Uuid4, Uuid7},
			value_type::ValueType,
		};

		let values = vec![
			Value::None {
				inner: ValueType::Utf8,
			},
			Value::Boolean(true),
			Value::Float4(OrderedF32::try_from(1.5f32).unwrap()),
			Value::Float8(OrderedF64::try_from(-2.25f64).unwrap()),
			Value::Int1(-8),
			Value::Int2(-1_600),
			Value::Int4(-320_000),
			Value::Int8(-64_000_000_000),
			Value::Int16(i128::MIN),
			Value::Utf8("state".to_string()),
			Value::Uint1(8),
			Value::Uint2(1_600),
			Value::Uint4(320_000),
			Value::Uint8(64_000_000_000),
			Value::Uint16(u128::MAX),
			Value::Date(Date::new(2026, 7, 20).unwrap()),
			Value::DateTime(DateTime::new(2026, 7, 20, 12, 34, 56, 789).unwrap()),
			Value::Time(Time::new(23, 59, 59, 1).unwrap()),
			Value::Duration(Duration::new(1, 2, 3).unwrap()),
			Value::IdentityId(IdentityId(Uuid7(StdUuid::from_u128(7)))),
			Value::Uuid4(Uuid4(StdUuid::from_u128(4))),
			Value::Uuid7(Uuid7(StdUuid::from_u128(77))),
			Value::Blob(Blob::new(vec![1, 2, 3])),
			Value::Int(Int::from_i128(i128::MIN)),
			Value::Uint(Uint::from_u128(u128::MAX)),
			Value::Decimal(Decimal(BigDecimal::new(BigInt::from(-12345), 3))),
			Value::Any(Box::new(Value::Boolean(false))),
			Value::DictionaryId(DictionaryEntryId::U16(u128::MAX)),
			Value::Type(ValueType::Record(vec![("k".to_string(), ValueType::Int4)])),
			Value::List(vec![Value::Int4(1), Value::Utf8("x".to_string())]),
			Value::Record(vec![("k".to_string(), Value::Int8(9))]),
			Value::Tuple(vec![
				Value::Boolean(true),
				Value::None {
					inner: ValueType::Any,
				},
			]),
		];

		let bytes = to_bytes::<Error>(&values).unwrap();
		let archived = access::<<Vec<Value> as Archive>::Archived, Error>(&bytes).unwrap();
		let restored = deserialize::<Vec<Value>, Error>(archived).unwrap();
		assert_eq!(restored, values);
	}

	#[test]
	fn test_with_wrapper_direct() {
		let int = BigInt::from(-123456789012345678901234567890i128);
		let bytes = to_bytes::<Error>(With::<_, BigIntBytes>::cast(&int)).unwrap();
		let archived = access::<<BigIntBytes as ArchiveWith<BigInt>>::Archived, Error>(&bytes).unwrap();
		let mut pool = Pool::default();
		let restored =
			BigIntBytes::deserialize_with(archived, Strategy::<Pool, Error>::wrap(&mut pool)).unwrap();
		assert_eq!(restored, int);
	}
}
