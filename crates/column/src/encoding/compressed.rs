// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Nine compressed-encoding surface types. Each is a zero-size marker whose
// `try_compress` returns `Ok(None)` in v1 ("this encoding doesn't apply") so
// the compressor falls back to canonical. `canonicalize` is `todo!()` because
// v1 never produces these encodings — filling it in is a follow-up PR per
// encoding. Registering them in `EncodingRegistry::builtins()` freezes the
// surface so downstream dispatch and IPC don't need changes when kernels land.

use reifydb_type::Result;

use crate::{
	array::{Array, canonical::CanonicalArray},
	compress::CompressConfig,
	encoding::{Encoding, EncodingId},
};

macro_rules! declare_compressed {
	($ty:ident, $id:ident, $id_str:literal) => {
		pub struct $ty;

		impl $ty {
			pub const ID: EncodingId = EncodingId($id_str);
		}

		impl Encoding for $ty {
			fn id(&self) -> EncodingId {
				Self::ID
			}

			fn try_compress(
				&self,
				_input: &CanonicalArray,
				_cfg: &CompressConfig,
			) -> Result<Option<Array>> {
				Ok(None)
			}

			fn canonicalize(&self, _array: &Array) -> Result<CanonicalArray> {
				todo!(concat!(stringify!($ty), "::canonicalize not yet implemented"))
			}
		}

		impl EncodingId {
			pub const $id: EncodingId = $ty::ID;
		}
	};
}

declare_compressed!(ConstantEncoding, CONSTANT, "column.constant");
declare_compressed!(AllNoneEncoding, ALL_NONE, "column.all_none");
declare_compressed!(DictEncoding, DICT, "column.dict");
declare_compressed!(RleEncoding, RLE, "column.rle");
declare_compressed!(DeltaEncoding, DELTA, "column.delta");
declare_compressed!(DeltaRleEncoding, DELTA_RLE, "column.delta_rle");
declare_compressed!(ForEncoding, FOR, "column.for");
declare_compressed!(BitPackEncoding, BITPACK, "column.bitpack");
declare_compressed!(SparseEncoding, SPARSE, "column.sparse");
