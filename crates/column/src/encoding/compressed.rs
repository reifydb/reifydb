// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{
	data::{Column, canonical::Canonical},
	encoding::EncodingId,
};
use reifydb_value::{Result, value::value_type::ValueType};

use crate::{compress::CompressConfig, encoding::Encoding, persist::PersistedArray};

macro_rules! declare_compressed {
	($ty:ident, $id:ident) => {
		pub struct $ty;

		impl $ty {
			pub const ID: EncodingId = EncodingId::$id;
		}

		impl Encoding for $ty {
			fn id(&self) -> EncodingId {
				Self::ID
			}

			fn try_compress(&self, _input: &Canonical, _cfg: &CompressConfig) -> Result<Option<Column>> {
				Ok(None)
			}

			fn canonicalize(&self, _array: &Column) -> Result<Canonical> {
				todo!(concat!(stringify!($ty), "::canonicalize not yet implemented"))
			}

			fn persist(&self, _array: &Column) -> Result<PersistedArray> {
				todo!(concat!(stringify!($ty), "::persist not yet implemented"))
			}

			fn load(&self, _persisted: PersistedArray, _ty: &ValueType) -> Result<Column> {
				todo!(concat!(stringify!($ty), "::load not yet implemented"))
			}
		}
	};
}

declare_compressed!(DictEncoding, DICT);
declare_compressed!(RleEncoding, RLE);
declare_compressed!(DeltaEncoding, DELTA);
declare_compressed!(DeltaRleEncoding, DELTA_RLE);
declare_compressed!(ForEncoding, FOR);
declare_compressed!(BitPackEncoding, BITPACK);
declare_compressed!(SparseEncoding, SPARSE);
