// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::{EncodedBytes, EncodedRowBuilder};
use reifydb_core::interface::catalog::queue::{QueueDeduplicate, QueueDispatch};
use reifydb_macro::catalog_shape;
use reifydb_value::value::duration::Duration;

catalog_shape! {
	pub(crate) queue {
		id: u64,
		namespace: u64,
		name: utf8,
		partitions: u16,
		ordered_by: utf8,
		retention_done: Duration?,
		retry_attempts: u32,
		retry_backoff: Duration,
		underlying: u8,
		deduplicate_by: utf8,
		deduplicate_ttl: Duration,
		dispatch: u8,
		ts: utf8,
		time_domain: u8,
	}

	pub(crate) queue_namespace {
		id: u64,
		name: utf8,
	}
}

pub(crate) fn decode_dispatch(bytes: &EncodedBytes) -> QueueDispatch {
	let partitions = queue::get_partitions(bytes);
	let ordered_by = match queue::get_ordered_by(bytes) {
		"" => None,
		column => Some(column.to_string()),
	};
	QueueDispatch::Fifo {
		partitions,
		ordered_by,
	}
}

pub(crate) fn encode_dispatch(row: &mut EncodedRowBuilder, dispatch: &QueueDispatch) {
	queue::set_dispatch(row, dispatch.tag());
	queue::set_partitions(row, dispatch.partitions());
	queue::set_ordered_by(row, dispatch.ordered_by().unwrap_or(""));
}

pub(crate) fn decode_deduplicate(bytes: &EncodedBytes) -> Option<QueueDeduplicate> {
	let by = queue::get_deduplicate_by(bytes);
	if by.is_empty() {
		return None;
	}
	Some(QueueDeduplicate {
		by: by.split(',').map(|column| column.to_string()).collect(),
		ttl: queue::get_deduplicate_ttl(bytes),
	})
}

pub(crate) fn encode_deduplicate(row: &mut EncodedRowBuilder, deduplicate: Option<&QueueDeduplicate>) {
	match deduplicate {
		Some(deduplicate) => {
			queue::set_deduplicate_by(row, deduplicate.by.join(","));
			queue::set_deduplicate_ttl(row, deduplicate.ttl);
		}
		None => {
			queue::set_deduplicate_by(row, "");
			queue::set_deduplicate_ttl(row, Duration::zero());
		}
	}
}
