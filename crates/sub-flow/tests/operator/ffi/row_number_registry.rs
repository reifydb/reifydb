// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The operator row-number registry must map the SAME logical key to the SAME
// RowNumber on every apply, reporting is_new=false on reuse. If it did not
// survive across applies, windowed operators would re-emit each window as a
// fresh Insert with a new row number (the FFI vs native divergence the parity
// work surfaced).

use super::Harness;
use crate::common::{RowNumberProbe, row_ints, trigger};

#[test]
fn row_number_registry_persists_across_applies() {
	let mut harness = Harness::<RowNumberProbe>::builder().build().expect("harness build");

	harness.apply(trigger()).expect("apply 1");
	harness.apply(trigger()).expect("apply 2");

	assert_eq!(row_ints(&harness[0]), vec![1, 1], "first apply freshly allocates row 1");
	assert_eq!(row_ints(&harness[1]), vec![1, 0], "second apply reuses row 1 (is_new=0)");
}
