// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;

impl Executor {
    pub(crate) fn limit(&mut self, limit: usize) -> crate::Result<()> {
        self.frame.limit(limit)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn implement() {
        todo!()
    }
}
