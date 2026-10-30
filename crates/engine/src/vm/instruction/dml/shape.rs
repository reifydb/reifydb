// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::shape::RowShape;
use reifydb_core::interface::catalog::{queue::Queue, ringbuffer::RingBuffer, series::Series};
use reifydb_transaction::transaction::Transaction;

use crate::Result;

pub fn get_or_create_ringbuffer_shape(
	catalog: &Catalog,
	ringbuffer: &RingBuffer,
	txn: &mut Transaction<'_>,
) -> Result<RowShape> {
	catalog.get_or_create_ringbuffer_shape(txn, ringbuffer)
}

pub fn get_or_create_queue_shape(catalog: &Catalog, queue: &Queue, txn: &mut Transaction<'_>) -> Result<RowShape> {
	catalog.get_or_create_queue_shape(txn, queue)
}

pub fn get_or_create_series_shape(catalog: &Catalog, series: &Series, txn: &mut Transaction<'_>) -> Result<RowShape> {
	catalog.get_or_create_series_shape(txn, series)
}

pub fn find_series_shape(catalog: &Catalog, series: &Series, txn: &mut Transaction<'_>) -> Result<Option<RowShape>> {
	catalog.find_series_shape(txn, series)
}
