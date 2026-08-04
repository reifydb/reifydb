// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use once_cell::sync::Lazy;
use reifydb_codec::encoded::{
	row::{EncodedRow, EncodedRowBuilder},
	shape::{RowShape, RowShapeField},
};
use reifydb_core::interface::catalog::queue::{QueueDeduplicate, QueueDispatch};
use reifydb_value::value::{duration::Duration, value_type::ValueType};

pub(crate) mod queue {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const PARTITIONS: usize = 3;
	pub(crate) const ORDERED_BY: usize = 4;
	pub(crate) const RETENTION_DONE: usize = 5;
	pub(crate) const RETRY_ATTEMPTS: usize = 6;
	pub(crate) const RETRY_BACKOFF: usize = 7;
	pub(crate) const UNDERLYING: usize = 8;
	pub(crate) const DEDUPLICATE_BY: usize = 9;
	pub(crate) const DEDUPLICATE_TTL: usize = 10;
	pub(crate) const DISPATCH: usize = 11;
	pub(crate) const TS: usize = 12;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("partitions", ValueType::Uint2),
			RowShapeField::unconstrained("ordered_by", ValueType::Utf8),
			RowShapeField::unconstrained("retention_done", ValueType::Duration),
			RowShapeField::unconstrained("retry_attempts", ValueType::Uint4),
			RowShapeField::unconstrained("retry_backoff", ValueType::Duration),
			RowShapeField::unconstrained("underlying", ValueType::Uint1),
			RowShapeField::unconstrained("deduplicate_by", ValueType::Utf8),
			RowShapeField::unconstrained("deduplicate_ttl", ValueType::Duration),
			RowShapeField::unconstrained("dispatch", ValueType::Uint1),
			RowShapeField::unconstrained("ts", ValueType::Utf8),
		])
	});
}

pub(crate) fn decode_dispatch(row: &EncodedRow) -> QueueDispatch {
	let partitions = queue::SHAPE.get::<u16>(row, queue::PARTITIONS);
	let ordered_by = match queue::SHAPE.get_utf8(row, queue::ORDERED_BY) {
		"" => None,
		column => Some(column.to_string()),
	};
	QueueDispatch::Fifo {
		partitions,
		ordered_by,
	}
}

pub(crate) fn encode_dispatch(row: &mut EncodedRowBuilder, dispatch: &QueueDispatch) {
	queue::SHAPE.set::<u8>(row, queue::DISPATCH, dispatch.tag());
	queue::SHAPE.set::<u16>(row, queue::PARTITIONS, dispatch.partitions());
	queue::SHAPE.set_utf8(row, queue::ORDERED_BY, dispatch.ordered_by().unwrap_or(""));
}

pub(crate) fn decode_deduplicate(row: &EncodedRow) -> Option<QueueDeduplicate> {
	let by = queue::SHAPE.get_utf8(row, queue::DEDUPLICATE_BY);
	if by.is_empty() {
		return None;
	}
	Some(QueueDeduplicate {
		by: by.split(',').map(|column| column.to_string()).collect(),
		ttl: queue::SHAPE.get::<Duration>(row, queue::DEDUPLICATE_TTL),
	})
}

pub(crate) fn encode_deduplicate(row: &mut EncodedRowBuilder, deduplicate: Option<&QueueDeduplicate>) {
	match deduplicate {
		Some(deduplicate) => {
			queue::SHAPE.set_utf8(row, queue::DEDUPLICATE_BY, deduplicate.by.join(","));
			queue::SHAPE.set::<Duration>(row, queue::DEDUPLICATE_TTL, deduplicate.ttl);
		}
		None => {
			queue::SHAPE.set_utf8(row, queue::DEDUPLICATE_BY, "");
			queue::SHAPE.set::<Duration>(row, queue::DEDUPLICATE_TTL, Duration::zero());
		}
	}
}

pub(crate) mod queue_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
		])
	});
}
