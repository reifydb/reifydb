// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod backfill;
pub(crate) mod committer;
pub(crate) mod merge;
pub(crate) mod quiescence;
pub(crate) mod slice;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
use reifydb_core::actors::pending::Pending;
#[cfg(test)]
use reifydb_flow_async::transaction::substrate::operator_section;
#[cfg(test)]
use reifydb_store_operator::{store::OperatorStore, types::classify_slice};
#[cfg(test)]
use reifydb_wal::lsn::Lsn;

#[cfg(test)]
static LSN: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn next_lsn() -> Lsn {
	Lsn::new(LSN.fetch_add(1, Ordering::Relaxed) + 1)
}

#[cfg(test)]
pub(crate) fn persist(store: &OperatorStore, pending: &Pending) {
	let (slice, sizes) = operator_section(store, pending, &[], &[]).unwrap();
	store.apply_batch_with_checkpoints(&classify_slice(&slice, &sizes), &[], &[], Some(next_lsn())).unwrap();
}
