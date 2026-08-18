// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::interface::cdc::Cdc;
use zstd::{decode_all, encode_all};

use crate::error::CdcError;

const ZSTD_LEVEL: i32 = 1;

pub(crate) fn encode(cdc: &Cdc) -> Result<Vec<u8>, CdcError> {
	let raw = to_stdvec(cdc).map_err(|e| CdcError::Codec(format!("postcard encode: {e}")))?;
	encode_all(&raw[..], ZSTD_LEVEL).map_err(|e| CdcError::Codec(format!("zstd encode: {e}")))
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Cdc, CdcError> {
	let raw = decode_all(bytes).map_err(|e| CdcError::Codec(format!("zstd decode: {e}")))?;
	from_bytes(&raw).map_err(|e| CdcError::Codec(format!("postcard decode: {e}")))
}
