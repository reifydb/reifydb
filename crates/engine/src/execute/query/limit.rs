// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use reifydb_storage::VersionedStorage;

impl<VS: VersionedStorage> Executor<VS> {
    pub(crate) fn limit(&mut self, limit: usize) -> crate::Result<()> {
        self.frame.limit(limit)?;
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
