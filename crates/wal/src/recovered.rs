// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_store_log::segment::Stop;

use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
	pub head: Option<Lsn>,
	pub durable: Option<Lsn>,
	pub start: Option<Lsn>,
	pub floors: Vec<(String, Option<Lsn>)>,
	pub stop: Stop,
}
