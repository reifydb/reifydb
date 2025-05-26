// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use base::encoding::binary::decode_binary;
use regex::Regex;
use std::error::Error;
use std::ops::{Bound, RangeBounds};

/// Parses an binary key range, using Rust range syntax.
pub fn parse_key_range(s: &str) -> Result<impl RangeBounds<Vec<u8>> + Clone, Box<dyn Error>> {
    let mut bound = (Bound::<Vec<u8>>::Unbounded, Bound::<Vec<u8>>::Unbounded);
    let re = Regex::new(r"^(\S+)?\.\.(=)?(\S+)?").expect("invalid regex");
    let groups = re.captures(s).ok_or_else(|| format!("invalid range {s}"))?;
    if let Some(start) = groups.get(1) {
        bound.0 = Bound::Included(decode_binary(start.as_str()));
    }
    if let Some(end) = groups.get(3) {
        let end = decode_binary(end.as_str());
        if groups.get(2).is_some() {
            bound.1 = Bound::Included(end)
        } else {
            bound.1 = Bound::Excluded(end)
        }
    }
    Ok(bound)
}
