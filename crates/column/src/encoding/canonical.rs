// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, value::Value};

use crate::{
	array::{Array, canonical::CanonicalArray},
	compress::CompressConfig,
	encoding::{Encoding, EncodingId},
	stats::{Stat, StatsSet},
};

// Canonical encoding for one storage family — identity round-trip plus the
// cheap stats that can be derived without decoding (currently `NoneCount`).
// Four constants (`BOOL`/`FIXED`/`VARLEN`/`BIGNUM`) cover the four families;
// the `EncodingRegistry::builtins()` registers each one so the dispatch model
// is exercised end-to-end.
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

	fn try_compress(&self, input: &CanonicalArray, _cfg: &CompressConfig) -> Result<Option<Array>> {
		Ok(Some(Array::from_canonical(input.clone())))
	}

	fn canonicalize(&self, array: &Array) -> Result<CanonicalArray> {
		let arc = array.to_canonical()?;
		Ok((*arc).clone())
	}

	fn derive_stats(&self, array: &Array) -> StatsSet {
		let mut s = StatsSet::new();
		if let Some(nones) = array.nones() {
			s.set(Stat::NoneCount, Value::Uint8(nones.none_count() as u64));
		}
		s
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn canonical_fixed_round_trips_via_try_compress_then_canonicalize() {
		let cd = ColumnData::int4([1i32, 2, 3, 4]);
		let canon = CanonicalArray::from_column_data(&cd).unwrap();
		let compressed = CanonicalEncoding::FIXED
			.try_compress(&canon, &CompressConfig::default())
			.unwrap()
			.expect("canonical try_compress always wraps");
		assert_eq!(compressed.encoding(), EncodingId::CANONICAL_FIXED);
		let back = CanonicalEncoding::FIXED.canonicalize(&compressed).unwrap();
		assert_eq!(back.len(), 4);
	}

	#[test]
	fn derive_stats_includes_none_count_when_nullable() {
		let mut cd = ColumnData::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let canon = CanonicalArray::from_column_data(&cd).unwrap();
		let array = Array::from_canonical(canon);
		let stats = CanonicalEncoding::FIXED.derive_stats(&array);
		assert_eq!(stats.get(Stat::NoneCount), Some(&Value::Uint8(2)));
	}

	#[test]
	fn registry_builtins_registers_four_canonical_encodings() {
		let r = crate::encoding::EncodingRegistry::builtins();
		assert_eq!(r.len(), 4);
		assert!(r.get(EncodingId::CANONICAL_BOOL).is_some());
		assert!(r.get(EncodingId::CANONICAL_FIXED).is_some());
		assert!(r.get(EncodingId::CANONICAL_VARLEN).is_some());
		assert!(r.get(EncodingId::CANONICAL_BIGNUM).is_some());
	}
}
