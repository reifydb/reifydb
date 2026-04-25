// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	array::{Column, canonical::Canonical},
	encoding::EncodingId,
	stats::{Stat, StatsSet},
};
use reifydb_type::{Result, value::Value};

use crate::{compress::CompressConfig, encoding::Encoding};

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

	fn canonicalize(&self, array: &Column) -> Result<Canonical> {
		let arc = array.to_canonical()?;
		Ok((*arc).clone())
	}

	fn derive_stats(&self, array: &Column) -> StatsSet {
		let mut s = StatsSet::new();
		if let Some(nones) = array.nones() {
			s.set(Stat::NoneCount, Value::Uint8(nones.none_count() as u64));
		}
		s
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
		let back = CanonicalEncoding::FIXED.canonicalize(&compressed).unwrap();
		assert_eq!(back.len(), 4);
	}

	#[test]
	fn derive_stats_includes_none_count_when_nullable() {
		let mut cd = ColumnBuffer::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let canon = Canonical::from_column_buffer(&cd).unwrap();
		let array = Column::from_canonical(canon);
		let stats = CanonicalEncoding::FIXED.derive_stats(&array);
		assert_eq!(stats.get(Stat::NoneCount), Some(&Value::Uint8(2)));
	}

	#[test]
	fn registry_builtins_registers_all_canonical_and_compressed_encodings() {
		let r = EncodingRegistry::builtins();
		// 4 canonical + 9 compressed stubs = 13
		assert_eq!(r.len(), 13);
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
