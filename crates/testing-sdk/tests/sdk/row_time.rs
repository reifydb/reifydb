// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The `#time` contract as seen by an operator.
//!
//! These pin the foundation the windowed drivers are being moved onto: an operator's only
//! way to ask "when did this row happen" is `RowView::row_time()`, and what it returns is
//! whatever the source stamped. Every assertion here is about the substrate, not about any
//! particular operator.

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::flow::operator::view::{ColumnsView, RowView, bridge::BridgeColumnsView};
use reifydb_testing_sdk::builders::TestOperatorRowBuilder;
use reifydb_value::value::{Value, datetime::DateTime, value_type::ValueType};

fn fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	]
}

fn row(rn: u64, group: &str, price: f64) -> TestOperatorRowBuilder {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::float8(price)])
		.with_fields(fields())
}

#[test]
fn a_stamped_row_reports_its_stamp_as_row_time() {
	// The window coordinate is being moved onto #time, so a fixture that stamps a time must be
	// able to prove the operator sees exactly that value. If this drifts, every windowed test
	// downstream is asserting against a coordinate nobody controls.
	let at = DateTime::from_millis(1_753_020_833_000);
	let built = row(1, "BTC", 10.0).with_time(at).build();

	let columns = Columns::from_row(&built);
	let view = BridgeColumnsView::new(&columns);
	let seen = view.row(0).expect("row 0").row_time();

	assert_eq!(seen, Some(at), "row_time must report the stamp the row was built with");
}

#[test]
fn an_unstamped_row_reads_as_absent_not_as_the_epoch() {
	// The epoch is a legal coordinate, so reporting it for an unstamped row makes a forgotten
	// stamp indistinguishable from a row genuinely dated 1970: every row buckets at zero and a
	// windowed fixture reports plausible single-bucket output instead of failing. Reporting none
	// makes the driver skip the row.
	let built = row(1, "BTC", 10.0).build();

	let columns = Columns::from_row(&built);
	let view = BridgeColumnsView::new(&columns);

	assert_eq!(view.row(0).expect("row 0").row_time(), None, "an unstamped row must report no #time at all");
}

#[test]
fn a_row_stamped_at_the_epoch_is_present_not_absent() {
	// The epoch must stay a usable coordinate. If absence collapses onto it, a row legitimately
	// dated 1970 silently stops reaching any window.
	let built = row(1, "BTC", 10.0).with_time(DateTime::default()).build();

	let columns = Columns::from_row(&built);
	let view = BridgeColumnsView::new(&columns);
	let seen = view.row(0).expect("row 0").row_time();

	assert_eq!(seen, Some(DateTime::default()), "an explicit epoch stamp must survive as a value");
	assert!(seen.expect("row time").is_epoch());
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

	let view = BridgeColumnsView::new(&columns);
	assert_eq!(view.row_count(), 2, "precondition: both rows are in one batch");
	assert_eq!(view.row(0).expect("row 0").row_time(), Some(first));
	assert_eq!(
		view.row(1).expect("row 1").row_time(),
		Some(second),
		"the second row keeps its own stamp; a shared batch stamp would bucket both together"
	);
}
