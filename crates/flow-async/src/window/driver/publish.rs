// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::window::driver::hooks::WindowEmits;

pub fn publish<Row>(
	emits: &mut WindowEmits<Row>,
	row_number: RowNumber,
	pre_time: DateTime,
	post_time: DateTime,
	pre: Option<Row>,
	post: Option<Row>,
) {
	match (pre, post) {
		(None, Some(post)) => emits.inserts.push((row_number, post_time, post)),
		(Some(pre), Some(post)) => emits.updates.push((row_number, pre_time, post_time, pre, post)),
		(Some(pre), None) => emits.removes.push((row_number, pre_time, pre)),
		(None, None) => {}
	}
}
