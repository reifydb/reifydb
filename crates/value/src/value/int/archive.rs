// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::{BigInt, Sign};
use rkyv::{
	Archive, Deserialize, Place, Serialize,
	rancor::Fallible,
	with::{ArchiveWith, DeserializeWith, SerializeWith},
};

#[derive(Archive, Serialize, Deserialize)]
pub struct BigIntRepr {
	pub(crate) sign: i8,
	pub(crate) magnitude_le: Vec<u8>,
}

impl BigIntRepr {
	pub(crate) fn from_bigint(value: &BigInt) -> Self {
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

	pub(crate) fn to_bigint(sign: i8, magnitude_le: &[u8]) -> BigInt {
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

#[cfg(test)]
mod tests {
	use num_bigint::BigInt;
	use rkyv::{
		access,
		de::Pool,
		rancor::{Error, Strategy},
		to_bytes,
		with::{ArchiveWith, DeserializeWith, With},
	};

	use crate::value::int::archive::BigIntBytes;

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
