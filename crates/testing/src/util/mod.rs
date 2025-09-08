// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

pub mod wait;

use std::{error::Error, ops::Bound};

use reifydb_core::{CowVec, util::encoding::binary::decode_binary};

/// Parses an binary key range, using Rust range syntax.
pub fn parse_key_range(
	s: &str,
) -> Result<(Bound<CowVec<u8>>, Bound<CowVec<u8>>), Box<dyn Error>> {
	let mut bound = (
		Bound::<CowVec<u8>>::Unbounded,
		Bound::<CowVec<u8>>::Unbounded,
	);

	if let Some(dot_pos) = s.find("..") {
		let start_part = &s[..dot_pos];
		let end_part = &s[dot_pos + 2..];

		// Parse start bound
		if !start_part.is_empty() {
			bound.0 = Bound::Included(decode_binary(start_part));
		}

		// Parse end bound - check for inclusive marker "="
		if let Some(end_str) = end_part.strip_prefix('=') {
			if !end_str.is_empty() {
				bound.1 =
					Bound::Included(decode_binary(end_str));
			}
		} else if !end_part.is_empty() {
			bound.1 = Bound::Excluded(decode_binary(end_part));
		}

		Ok(bound)
	} else {
		Err(format!("invalid range {s}").into())
	}
}
