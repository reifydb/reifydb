// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::value::value_type::ValueType;

impl ValueType {
	pub fn is_scalar(&self) -> bool {
		!matches!(
			self.inner_type(),
			ValueType::Any
				| ValueType::List(_) | ValueType::Record(_)
				| ValueType::Tuple(_) | ValueType::Digest { .. }
		)
	}
}

#[cfg(test)]
mod tests {
	use ValueType::*;

	use crate::value::value_type::ValueType;

	fn opt(inner: ValueType) -> ValueType {
		Option(Box::new(inner))
	}

	fn digest() -> ValueType {
		Digest {
			inner: Box::new(Float8),
			accuracy: 10_000,
		}
	}

	#[test]
	fn every_scalar_type_and_its_option_is_scalar() {
		// A scalar reported as non-scalar makes distinct, sort and group by refuse a valid key column.
		let scalars = [
			Boolean,
			Float4,
			Float8,
			Int1,
			Int2,
			Int4,
			Int8,
			Int16,
			Utf8,
			Uint1,
			Uint2,
			Uint4,
			Uint8,
			Uint16,
			Date,
			DateTime,
			Time,
			Duration,
			IdentityId,
			Uuid4,
			Uuid7,
			Blob,
			ValueType::INT,
			ValueType::UINT,
			ValueType::DECIMAL,
			DictionaryId,
		];
		for ty in scalars {
			assert!(ty.is_scalar(), "{ty:?} must be scalar");
			assert!(opt(ty.clone()).is_scalar(), "Option({ty:?}) must be scalar");
		}
	}

	#[test]
	fn any_list_record_tuple_digest_and_their_options_are_not_scalar() {
		// A composite reported as scalar reaches the key encoder, which cannot key it and fails mid-query.
		let composites = [
			Any,
			List(Box::new(Int4)),
			Record(vec![("a".to_string(), Int4)]),
			Tuple(vec![Int4, Utf8]),
			digest(),
		];
		for ty in composites {
			assert!(!ty.is_scalar(), "{ty:?} must not be scalar");
			assert!(!opt(ty.clone()).is_scalar(), "Option({ty:?}) must not be scalar");
		}
	}
}
