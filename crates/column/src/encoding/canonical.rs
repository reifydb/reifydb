// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{
	data::{Column, canonical::Canonical},
	encoding::EncodingId,
};
use reifydb_value::{Result, value::value_type::ValueType};

use crate::{
	compress::CompressConfig,
	encoding::Encoding,
	persist::{PersistedArray, unexpected_payload},
};

pub struct CanonicalEncoding {
	pub id: EncodingId,
}

impl CanonicalEncoding {
	pub const BOOL: Self = Self {
		id: EncodingId::CANONICAL_BOOL,
	};
	pub const FIXED: Self = Self {
		id: EncodingId::CANONICAL_FIXED,
	};
	pub const VARLEN: Self = Self {
		id: EncodingId::CANONICAL_VARLEN,
	};
	pub const BIGNUM: Self = Self {
		id: EncodingId::CANONICAL_BIGNUM,
	};
}

impl Encoding for CanonicalEncoding {
	fn id(&self) -> EncodingId {
		self.id
	}

	fn try_compress(&self, input: &Canonical, _cfg: &CompressConfig) -> Result<Option<Column>> {
		Ok(Some(Column::from_canonical(input.clone())))
	}

	fn persist(&self, array: &Column) -> Result<PersistedArray> {
		let canonical = array.to_canonical()?;
		Ok(PersistedArray::Canonical {
			buffer: canonical.to_buffer(),
		})
	}

	fn load(&self, persisted: PersistedArray, _ty: &ValueType) -> Result<Column> {
		match persisted {
			PersistedArray::Canonical {
				buffer,
			} => Ok(Column::from_canonical(Canonical::from_buffer(buffer))),
			_ => Err(unexpected_payload(self.id)),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;

	use super::*;
	use crate::encoding::EncodingRegistry;

	#[test]
	fn canonical_fixed_round_trips_via_try_compress_then_canonicalize() {
		let cd = ColumnBuffer::int4([1i32, 2, 3, 4]);
		let canon = Canonical::from_column_buffer(&cd).unwrap();
		let compressed = CanonicalEncoding::FIXED
			.try_compress(&canon, &CompressConfig::default())
			.unwrap()
			.expect("canonical try_compress always wraps");
		assert_eq!(compressed.encoding(), EncodingId::CANONICAL_FIXED);
		let back = compressed.to_canonical().unwrap();
		assert_eq!(back.len(), 4);
	}

	#[test]
	fn every_builtin_encoding_id_resolves_through_the_registry() {
		// An encoding added to the id set but never registered resolves to none here.
		let r = EncodingRegistry::builtins();
		for id in [
			EncodingId::CANONICAL_BOOL,
			EncodingId::CANONICAL_FIXED,
			EncodingId::CANONICAL_VARLEN,
			EncodingId::CANONICAL_BIGNUM,
			EncodingId::CONSTANT,
			EncodingId::ALL_NONE,
			EncodingId::DICT,
			EncodingId::RLE,
			EncodingId::DELTA,
			EncodingId::DELTA_RLE,
			EncodingId::FOR,
			EncodingId::BITPACK,
			EncodingId::SPARSE,
		] {
			assert!(r.get(id).is_some(), "missing encoding {id:?}");
		}
	}
}
