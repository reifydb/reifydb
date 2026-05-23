// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// The operator-facing store range must return every row in range, even past the
// 1024 storage pagination batch_size. A backend that capped the scan (e.g.
// `.take(1024)`) would return fewer than the seeded rows and fail.

use reifydb_core::{interface::catalog::shape::ShapeId, key::row::RowKeyRange};

use super::Harness;
use crate::common::{NoopOperator, STORE_ROW_COUNT, STORE_TABLE, store_seed};

#[test]
fn range_returns_all_rows_past_pagination_batch() {
	let mut harness = Harness::<NoopOperator>::builder().build().expect("harness build");

	harness.seed_store(&store_seed());

	let range = RowKeyRange::scan_range(ShapeId::table(STORE_TABLE), None);
	let rows = harness.store_range(range.start.as_ref(), range.end.as_ref());

	assert_eq!(
		rows.len(),
		STORE_ROW_COUNT as usize,
		"range must return all rows; the 1024 batch_size must not truncate"
	);
}
