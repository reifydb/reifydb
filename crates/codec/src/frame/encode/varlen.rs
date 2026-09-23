// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::frame::data::FrameColumnData;

use super::EncodedColumn;
use crate::frame::{
	encoding::dict::{try_dict_encode_blob, try_dict_encode_utf8},
	format::Encoding,
};

pub(crate) fn try_dict_varlen(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Utf8(c) => {
			let dict = try_dict_encode_utf8(c, 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		FrameColumnData::Blob(c) => {
			let dict = try_dict_encode_blob(c, 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		_ => None,
	}
}
