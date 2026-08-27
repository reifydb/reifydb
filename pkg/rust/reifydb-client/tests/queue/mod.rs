// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod grpc;
mod http;
mod ws;

pub const QUEUE: &str = "CREATE QUEUE app::jobs { id: int4 } WITH { fifo: { partitions: 1 } }";

pub fn row_count(frames: &[reifydb_client::Frame]) -> usize {
	frames.iter().map(|frame| frame.row_count()).sum()
}
