// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::transaction::*;

/// TransactionManagerRx is a read-only transaction manager.
///
/// It is created by calling [`TransactionManager::read`],
/// the read transaction will automatically notify the transaction manager when it
/// is dropped. So, the end user doesn't need to call any cleanup function, but must
/// hold this struct in their final read transaction implementation.
pub struct TransactionManagerRx<K, V, C, P> {
    pub(in crate::mvcc::transaction) db: TransactionManager<K, V, C, P>,
    pub(in crate::mvcc::transaction) version: u64,
}

impl<K, V, C, P> TransactionManagerRx<K, V, C, P> {
    /// Returns the version of this read transaction.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

impl<K, V, C, P> Drop for TransactionManagerRx<K, V, C, P> {
    fn drop(&mut self) {
        self.db.inner.done_read(self.version);
    }
}
