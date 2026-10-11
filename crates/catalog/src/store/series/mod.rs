// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub mod shape;
pub mod update;

use reifydb_core::{common::TimeSource, interface::catalog::series::SeriesEncoding};

use crate::{
	Result,
	error::CatalogError,
	store::{series::shape::series, time_source::read_time_source},
};

pub(crate) fn decode_series_time(bytes: &EncodedCatalogRow) -> TimeSource {
	read_time_source(&series::SHAPE, bytes.as_slice(), series::TIME_DOMAIN, series::TS)
}

pub(crate) fn decode_series_encoding(bytes: &EncodedCatalogRow) -> Result<Option<SeriesEncoding>> {
	let config = series::get_encoding_config(bytes);
	match series::try_get_encoding_name(bytes) {
		None if config.is_empty() => Ok(None),
		None => Err(CatalogError::SeriesEncodingConfigCorrupt {
			name: String::new(),
			reason: "config bytes without an encoding name".to_string(),
		}
		.into()),
		Some(name) if config.is_empty() => Err(CatalogError::SeriesEncodingConfigCorrupt {
			name: name.to_string(),
			reason: "encoding name without config bytes".to_string(),
		}
		.into()),
		Some(name) => Ok(Some(SeriesEncoding {
			name: name.to_string(),
			config: config.as_bytes().to_vec(),
		})),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::blob::Blob;

	use super::decode_series_encoding;
	use crate::store::series::shape::series;

	#[test]
	fn a_series_row_with_a_name_but_no_config_is_corrupt() {
		// Defaulting to no encoding would read encoded blocks as plain ones.
		let mut row = series::allocate();
		series::set_encoding_name(&mut row, "delta");
		series::set_encoding_config(&mut row, &Blob::from(Vec::new()));
		let diagnostic = decode_series_encoding(&row.freeze()).unwrap_err().diagnostic();
		assert_eq!(diagnostic.code, "CA_105");
		assert!(diagnostic.message.contains("delta"), "{}", diagnostic.message);
	}

	#[test]
	fn a_series_row_with_config_but_no_name_is_corrupt() {
		// Treating orphan config bytes as no encoding hides a broken catalog row.
		let mut row = series::allocate();
		series::set_encoding_config(&mut row, &Blob::from(vec![0u8]));
		let diagnostic = decode_series_encoding(&row.freeze()).unwrap_err().diagnostic();
		assert_eq!(diagnostic.code, "CA_105");
	}

	#[test]
	fn a_series_without_encoding_reads_none_and_empty_blob() {
		// Writing postcard of an empty list here would make every plain series look corrupt.
		let mut row = series::allocate();
		series::set_encoding_config(&mut row, &Blob::from(Vec::new()));
		let row = row.freeze();
		assert!(series::get_encoding_config(&row).is_empty());
		assert_eq!(decode_series_encoding(&row).unwrap(), None);
	}
}
