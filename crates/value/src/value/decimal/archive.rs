// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bigdecimal::BigDecimal;
use rkyv::{
	Archive, Deserialize, Place, Serialize,
	rancor::Fallible,
	with::{ArchiveWith, DeserializeWith, SerializeWith},
};

use crate::value::int::archive::BigIntRepr;

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
	use rkyv::{Archive, Deserialize, Serialize, access, deserialize, rancor::Error, to_bytes};

	use crate::value::{decimal::archive::BigDecimalBytes, int::archive::BigIntBytes};

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
}
