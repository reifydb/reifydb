// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::env;

use getrandom::fill;

pub fn dst_seed() -> u64 {
	let seed = match env::var("REIFYDB_DST_SEED") {
		Ok(value) => value.parse().expect("REIFYDB_DST_SEED must be a valid u64"),
		Err(_) => {
			let mut buf = [0u8; 8];
			fill(&mut buf).expect("getrandom failed");
			u64::from_le_bytes(buf)
		}
	};
	println!("REIFYDB_DST_SEED={seed}");
	seed
}
