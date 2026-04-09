// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate};
use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, r#type::Type},
};

pub const REQUEST_HISTORY_CAPACITY: u64 = 10_000;
pub const STATEMENT_STATS_CAPACITY: u64 = 5_000;

fn col(name: &str, ty: Type) -> RingBufferColumnToCreate {
	RingBufferColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

pub fn request_history(namespace: NamespaceId) -> RingBufferToCreate {
	RingBufferToCreate {
		name: Fragment::internal("request_history"),
		namespace,
		columns: vec![
			col("timestamp", Type::DateTime),
			col("operation", Type::Utf8),
			col("fingerprint", Type::Utf8),
			col("total_duration_us", Type::Int8),
			col("compute_duration_us", Type::Int8),
			col("success", Type::Boolean),
			col("statement_count", Type::Int8),
			col("normalized_rql", Type::Utf8),
		],
		capacity: REQUEST_HISTORY_CAPACITY,
		partition_by: vec![],
	}
}

pub fn statement_stats(namespace: NamespaceId) -> RingBufferToCreate {
	RingBufferToCreate {
		name: Fragment::internal("statement_stats"),
		namespace,
		columns: vec![
			col("snapshot_timestamp", Type::DateTime),
			col("fingerprint", Type::Utf8),
			col("normalized_rql", Type::Utf8),
			col("calls", Type::Int8),
			col("total_duration_us", Type::Int8),
			col("mean_duration_us", Type::Int8),
			col("max_duration_us", Type::Int8),
			col("min_duration_us", Type::Int8),
			col("total_rows", Type::Int8),
			col("errors", Type::Int8),
		],
		capacity: STATEMENT_STATS_CAPACITY,
		partition_by: vec![],
	}
}
