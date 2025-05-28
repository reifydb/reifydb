// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;

/// Rtm is a read-only transaction manager.
///
/// It is created by calling [`Tm::read`],
/// the read transaction will automatically notify the transaction manager when it
/// is dropped. So, the end user doesn't need to call any cleanup function, but must
/// hold this struct in their final read transaction implementation.
pub struct Rtm<K, V, C, P> {
  pub(super) db: Tm<K, V, C, P>,
  pub(super) read_ts: u64,
}

impl<K, V, C, P> Rtm<K, V, C, P> {
  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }
}

impl<K, V, C, P> Drop for Rtm<K, V, C, P> {
  fn drop(&mut self) {
    self.db.inner.done_read(self.read_ts);
  }
}
