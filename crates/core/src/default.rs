// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const TESTING: bool = cfg!(feature = "testing");

pub mod query {
	use reifydb_value::byte_size::ByteSize;

	pub const ORACLE_WINDOW_SIZE: u64 = 500;
	pub const ORACLE_WINDOW_SIZE_TESTING: u64 = 16;

	pub const ROW_BATCH_SIZE: u16 = 128;
	pub const ROW_BATCH_SIZE_TESTING: u16 = 4;

	pub const MEMORY_LIMIT: ByteSize = ByteSize::from_gib(1);
	pub const MEMORY_LIMIT_TESTING: ByteSize = ByteSize::from_mib(1);
}

pub mod retention {
	use reifydb_value::value::duration::Duration;

	pub const EVICT_INTERVAL: Duration = Duration::from_seconds_const(60);
	pub const EVICT_INTERVAL_TESTING: Duration = Duration::from_seconds_const(60);

	pub const EVICT_BATCH_SIZE: u64 = 1024;
	pub const EVICT_BATCH_SIZE_TESTING: u64 = 4;

	pub const EVICT_MAX_BATCHES_PER_TICK: u64 = 8;
	pub const EVICT_MAX_BATCHES_PER_TICK_TESTING: u64 = 2;

	pub const STARTUP_GRACE: Duration = Duration::from_seconds_const(300);
	pub const STARTUP_GRACE_TESTING: Duration = Duration::from_seconds_const(300);

	pub const MAX_HORIZON_FLOOR: Duration = Duration::from_hours_const(7 * 24);
	pub const MAX_HORIZON_FLOOR_TESTING: Duration = Duration::from_hours_const(7 * 24);

	pub const EPOCH_BUCKET_INTERVAL: Duration = Duration::from_seconds_const(60);
	pub const EPOCH_BUCKET_INTERVAL_TESTING: Duration = Duration::from_seconds_const(60);

	pub const HISTORICAL_GC_BATCH_SIZE: u64 = 50_000;
	pub const HISTORICAL_GC_BATCH_SIZE_TESTING: u64 = 16;

	pub const HISTORICAL_GC_INTERVAL: Duration = Duration::from_seconds_const(30);
	pub const HISTORICAL_GC_INTERVAL_TESTING: Duration = Duration::from_seconds_const(30);
}

pub mod cdc {
	use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

	pub const TTL: Option<Duration> = None;
	pub const TTL_TESTING: Option<Duration> = None;

	pub const TTL_SCAN_INTERVAL: Duration = Duration::from_seconds_const(30);
	pub const TTL_SCAN_INTERVAL_TESTING: Duration = Duration::from_seconds_const(30);

	pub const TTL_SCAN_BATCH_SIZE: u64 = 8192;
	pub const TTL_SCAN_BATCH_SIZE_TESTING: u64 = 16;

	pub const WAL_AUTOCHECKPOINT_PAGES: u64 = 1_000_000;
	pub const WAL_AUTOCHECKPOINT_PAGES_TESTING: u64 = 64;

	pub const COMMIT_BUFFER: ByteSize = ByteSize::from_mib(256);
	pub const COMMIT_BUFFER_TESTING: ByteSize = ByteSize::from_kib(64);

	pub const BLOCK_CUT: ByteSize = ByteSize::from_mib(4);
	pub const BLOCK_CUT_TESTING: ByteSize = ByteSize::from_kib(4);

	pub const READ_BUFFER: ByteSize = ByteSize::from_mib(256);
	pub const READ_BUFFER_TESTING: ByteSize = ByteSize::from_kib(64);

	pub const CONSUME_WAIT_TIMEOUT: Duration = Duration::from_seconds_const(30);
	pub const CONSUME_WAIT_TIMEOUT_TESTING: Duration = Duration::from_seconds_const(30);
}

pub mod store {
	use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

	pub const MULTI_POINT_BUFFER_SHARD: ByteSize = ByteSize::from_mib(4);
	pub const MULTI_POINT_BUFFER_SHARD_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const MULTI_RANGE_BUFFER_SHARD: ByteSize = ByteSize::from_mib(4);
	pub const MULTI_RANGE_BUFFER_SHARD_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const MULTI_POINT_BUFFER_SHARDS: u16 = 16;
	pub const MULTI_POINT_BUFFER_SHARDS_TESTING: u16 = 2;

	pub const MULTI_RANGE_BUFFER_SHARDS: u16 = 16;
	pub const MULTI_RANGE_BUFFER_SHARDS_TESTING: u16 = 2;

	pub const MULTI_FLUSH_INTERVAL: Duration = Duration::from_seconds_const(60);
	pub const MULTI_FLUSH_INTERVAL_TESTING: Duration = Duration::from_seconds_const(60);

	pub const MULTI_FLUSH_BUDGET: ByteSize = ByteSize::from_mib(4);
	pub const MULTI_FLUSH_BUDGET_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const MULTI_WAL_AUTOCHECKPOINT_PAGES: u64 = 50_000;
	pub const MULTI_WAL_AUTOCHECKPOINT_PAGES_TESTING: u64 = 64;

	pub const OPERATOR_POINT_BUFFER_SHARD: ByteSize = ByteSize::from_kib(512);
	pub const OPERATOR_POINT_BUFFER_SHARD_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const OPERATOR_RANGE_BUFFER_SHARD: ByteSize = ByteSize::from_mib(2);
	pub const OPERATOR_RANGE_BUFFER_SHARD_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const OPERATOR_POINT_BUFFER_SHARDS: u16 = 16;
	pub const OPERATOR_POINT_BUFFER_SHARDS_TESTING: u16 = 2;

	pub const OPERATOR_RANGE_BUFFER_SHARDS: u16 = 16;
	pub const OPERATOR_RANGE_BUFFER_SHARDS_TESTING: u16 = 2;

	pub const OPERATOR_RESIDENT_BUDGET: ByteSize = ByteSize::from_mib(100);
	pub const OPERATOR_RESIDENT_BUDGET_TESTING: ByteSize = ByteSize::from_kib(64);

	pub const OPERATOR_FLUSH_SLICE: ByteSize = ByteSize::from_mib(4);
	pub const OPERATOR_FLUSH_SLICE_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const OPERATOR_WAL_AUTOCHECKPOINT_PAGES: u64 = 1_000_000;
	pub const OPERATOR_WAL_AUTOCHECKPOINT_PAGES_TESTING: u64 = 64;
}

pub mod flow {
	use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

	pub const TICK: Duration = Duration::from_seconds_const(1);
	pub const TICK_TESTING: Duration = Duration::from_seconds_const(1);

	pub const SAMPLE_INTERVAL: Duration = Duration::from_seconds_const(60);
	pub const SAMPLE_INTERVAL_TESTING: Duration = Duration::from_seconds_const(60);

	pub const BACKLOG_MEMORY_LIMIT: ByteSize = ByteSize::from_mib(64);
	pub const BACKLOG_MEMORY_LIMIT_TESTING: ByteSize = ByteSize::from_kib(64);

	pub const PULL_BATCH: ByteSize = ByteSize::from_mib(8);
	pub const PULL_BATCH_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const LOAD_BATCH: ByteSize = ByteSize::from_mib(8);
	pub const LOAD_BATCH_TESTING: ByteSize = ByteSize::from_kib(16);

	pub const JOIN_PROBE_BLOCK_SIZE: u64 = 1024;
	pub const JOIN_PROBE_BLOCK_SIZE_TESTING: u64 = 16;
}

pub mod threads {
	pub const ASYNC: u16 = 1;
	pub const ASYNC_TESTING: u16 = 1;

	pub const COORDINATION: u16 = 2;
	pub const COORDINATION_TESTING: u16 = 1;

	pub const FLOW: u16 = 2;
	pub const FLOW_TESTING: u16 = 2;

	pub const TASK: u16 = 2;
	pub const TASK_TESTING: u16 = 1;

	pub const COMPUTE: u16 = 2;
	pub const COMPUTE_TESTING: u16 = 1;

	pub const SUBSCRIPTION_WORKER: u16 = 0;
	pub const SUBSCRIPTION_WORKER_TESTING: u16 = 0;
}

pub mod metrics {
	use reifydb_value::value::duration::Duration;

	pub const FLUSH_INTERVAL: Duration = Duration::from_seconds_const(10);
	pub const FLUSH_INTERVAL_TESTING: Duration = Duration::from_seconds_const(10);

	pub const SAMPLE_INTERVAL: Duration = Duration::from_seconds_const(10);
	pub const SAMPLE_INTERVAL_TESTING: Duration = Duration::from_seconds_const(10);

	pub const SNAPSHOT_INTERVAL: Option<Duration> = None;
	pub const SNAPSHOT_INTERVAL_TESTING: Option<Duration> = None;
}

pub mod queue {
	use reifydb_value::value::duration::Duration;

	pub const LEASE_REAP_INTERVAL: Duration = Duration::from_seconds_const(5);
	pub const LEASE_REAP_INTERVAL_TESTING: Duration = Duration::from_seconds_const(5);

	pub const LEASE_REAP_BATCH_SIZE: u64 = 1024;
	pub const LEASE_REAP_BATCH_SIZE_TESTING: u64 = 16;

	pub const RETENTION_INTERVAL: Duration = Duration::from_seconds_const(60);
	pub const RETENTION_INTERVAL_TESTING: Duration = Duration::from_seconds_const(60);

	pub const RETENTION_BATCH_SIZE: u64 = 1024;
	pub const RETENTION_BATCH_SIZE_TESTING: u64 = 16;
}
