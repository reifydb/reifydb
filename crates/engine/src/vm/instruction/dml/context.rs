// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::{namespace::Namespace, ringbuffer::RingBuffer, series::Series, table::Table};
use reifydb_type::fragment::Fragment;

use crate::vm::{services::Services, stack::SymbolTable};

/// Namespace + table + diagnostic fragment - identifies the destination of a
/// table DML operation. The fragment is the source-text identifier used in
/// error reporting (e.g., primary key violations).
pub(super) struct TableTarget<'a> {
	pub namespace: &'a Namespace,
	pub table: &'a Table,
	pub fragment: &'a Fragment,
}

/// Namespace + ringbuffer - identifies the destination of a ring buffer DML
/// operation.
pub(super) struct RingBufferTarget<'a> {
	pub namespace: &'a Namespace,
	pub ringbuffer: &'a RingBuffer,
}

/// Namespace + series - identifies the destination of a series DML operation.
pub(super) struct SeriesTarget<'a> {
	pub namespace: &'a Namespace,
	pub series: &'a Series,
}

/// Services + symbols - the execution-side context that DML helpers need for
/// catalog lookups and write-policy enforcement.
pub(super) struct WriteExecCtx<'a> {
	pub services: &'a Arc<Services>,
	pub symbols: &'a SymbolTable,
}
