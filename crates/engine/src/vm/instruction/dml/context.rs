// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::{namespace::Namespace, ringbuffer::RingBuffer, series::Series, table::Table};
use reifydb_type::fragment::Fragment;

use crate::vm::{services::Services, stack::SymbolTable};

pub(super) struct TableTarget<'a> {
	pub namespace: &'a Namespace,
	pub table: &'a Table,
	pub fragment: &'a Fragment,
}

pub(super) struct RingBufferTarget<'a> {
	pub namespace: &'a Namespace,
	pub ringbuffer: &'a RingBuffer,
}

pub(super) struct SeriesTarget<'a> {
	pub namespace: &'a Namespace,
	pub series: &'a Series,
}

pub(super) struct WriteExecCtx<'a> {
	pub services: &'a Arc<Services>,
	pub symbols: &'a SymbolTable,
}
