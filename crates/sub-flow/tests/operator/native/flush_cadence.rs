// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// flush_state must be deferred to the explicit flush, not run inside apply. The
// probe writes its state only in flush_state, so the value must be invisible
// after apply and visible only after flush. A backend that flushed per-apply
// would make the value visible too early and fail this.

use super::Harness;
use crate::common::{FLUSH_PROBE_VALUE, FlushProbe, flush_probe_key, trigger};

#[test]
fn state_is_visible_only_after_flush() {
	let mut harness = Harness::<FlushProbe>::builder().build().expect("harness build");

	harness.apply_without_flush(trigger()).expect("apply");
	assert_eq!(harness.state_value::<i64>(&flush_probe_key()), None, "state must not be visible before flush");

	harness.flush().expect("flush");
	assert_eq!(
		harness.state_value::<i64>(&flush_probe_key()),
		Some(FLUSH_PROBE_VALUE),
		"state visible after flush"
	);
}
