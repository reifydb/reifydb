// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The `#time` contract as seen by an operator.
//!
//! These pin the foundation the windowed drivers are being moved onto: an operator's only
//! way to ask "when did this row happen" is `RowView::row_time()`, and what it returns is
//! whatever the source stamped. Every assertion here is about the substrate, not about any
//! particular operator.

use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::operator::view::{ColumnsView, RowView, native::NativeColumnsView};
use reifydb_testing_sdk::builders::TestRowBuilder;
use reifydb_value::value::{Value, datetime::DateTime, value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	])
}

fn row(rn: u64, group: &str, price: f64) -> TestRowBuilder {
	TestRowBuilder::new(rn).with_values(vec![Value::Utf8(group.into()), Value::float8(price)]).with_shape(shape())
}

#[test]
fn a_stamped_row_reports_its_stamp_as_row_time() {
	// The window coordinate is being moved onto #time, so a fixture that stamps a time must be
	// able to prove the operator sees exactly that value. If this drifts, every windowed test
	// downstream is asserting against a coordinate nobody controls.
	let at = DateTime::from_millis(1_753_020_833_000);
	let built = row(1, "BTC", 10.0).with_time(at).build();

	let columns = Columns::from_row(&built);
	let view = NativeColumnsView::new(&columns);
	let seen = view.row(0).expect("row 0").row_time();

	assert_eq!(seen, Some(at), "row_time must report the stamp the row was built with");
}

#[test]
fn an_unstamped_row_reads_as_the_epoch_not_as_absent() {
	// This is the trap that makes the migration dangerous. TestRowBuilder leaves #time at the
	// epoch when nothing stamps it, and the epoch is a LEGAL coordinate rather than a missing
	// one. So a windowed fixture that forgets to stamp does not fail loudly: every row buckets
	// at zero and the suite reports plausible single-bucket output. Pinning the behaviour here
	// means the next person meets it as a documented fact instead of as a green test that
	// checks nothing.
	let built = row(1, "BTC", 10.0).build();

	let columns = Columns::from_row(&built);
	let view = NativeColumnsView::new(&columns);
	let seen = view.row(0).expect("row 0").row_time();

	assert_eq!(
		seen,
		Some(DateTime::default()),
		"an unstamped row must read as the epoch; if this ever becomes none, the windowed \
		 drivers get a missing-value path they do not have today"
	);
	assert!(seen.expect("row time").is_epoch(), "the epoch stamp is what makes the trap silent");
}

#[test]
fn distinct_stamps_survive_independently_across_rows_in_one_batch() {
	// #time is a per-row sidecar, not a per-batch scalar. A window that buckets a batch has to
	// see each row's own time or every multi-window batch collapses into one bucket - which is
	// indistinguishable from the freeze this whole change is fixing.
	let first = DateTime::from_millis(1_753_020_833_000);
	let second = DateTime::from_millis(1_753_020_953_000);

	let mut columns = Columns::from_row(&row(1, "BTC", 10.0).with_time(first).build());
	let later = Columns::from_row(&row(2, "BTC", 20.0).with_time(second).build());
	columns.append(later).expect("append");

	let view = NativeColumnsView::new(&columns);
	assert_eq!(view.row_count(), 2, "precondition: both rows are in one batch");
	assert_eq!(view.row(0).expect("row 0").row_time(), Some(first));
	assert_eq!(
		view.row(1).expect("row 1").row_time(),
		Some(second),
		"the second row keeps its own stamp; a shared batch stamp would bucket both together"
	);
}
