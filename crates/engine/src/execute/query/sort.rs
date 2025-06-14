// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use reifydb_frame::SortKey;
use reifydb_storage::{Storage, UnversionedStorage, VersionedStorage};

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn sort(&mut self, sort_keys: &[SortKey]) -> crate::Result<()> {
        self.frame.sort(sort_keys)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
