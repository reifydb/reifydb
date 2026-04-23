// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	array::{Column, canonical::Canonical},
	encoding::EncodingId,
};
use reifydb_type::Result;

use crate::{compress::CompressConfig, encoding::Encoding};

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
		}
	};
}

declare_compressed!(ConstantEncoding, CONSTANT);
declare_compressed!(AllNoneEncoding, ALL_NONE);
declare_compressed!(DictEncoding, DICT);
declare_compressed!(RleEncoding, RLE);
declare_compressed!(DeltaEncoding, DELTA);
declare_compressed!(DeltaRleEncoding, DELTA_RLE);
declare_compressed!(ForEncoding, FOR);
declare_compressed!(BitPackEncoding, BITPACK);
declare_compressed!(SparseEncoding, SPARSE);
