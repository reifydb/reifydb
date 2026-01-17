// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to Blob type

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
	value::{blob::Blob, r#type::Type},
};

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_blob(data: &ColumnData) -> EvalResult<ColumnData> {
	match data {
		ColumnData::Utf8 {
			container,
			..
		} => {
			let mut out = ColumnData::with_capacity(Type::Blob, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let temp_fragment = Fragment::internal(container[idx].as_str());
					out.push(Blob::from_utf8(temp_fragment));
				} else {
					out.push_undefined()
				}
			}
			Ok(out)
		}
		_ => {
			let source_type = data.get_type();
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: "Blob".to_string(),
			})
		}
	}
}
