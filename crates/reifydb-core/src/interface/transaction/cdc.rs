// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{CdcEvent, CdcStorage};
use crate::{Result, Version};
use std::ops::Bound;

pub trait CdcTransaction: Send + Sync + Clone + 'static {
    fn get(&self, version: Version) -> Result<Vec<CdcEvent>>;

    fn range(
        &self,
        start: Bound<Version>,
        end: Bound<Version>,
    ) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>>;

    fn scan(&self) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>>;

    fn count(&self, version: Version) -> Result<usize>;
}

/// CDC transaction wrapper for storage that implements CdcQuery
#[derive(Clone)]
pub struct StandardCdcTransaction<S : CdcStorage> {
    storage: S,
}

impl<S : CdcStorage> StandardCdcTransaction<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S: CdcStorage> CdcTransaction for StandardCdcTransaction<S> {
    fn get(&self, version: Version) -> Result<Vec<CdcEvent>> {
        self.storage.get(version)
    }

    fn range(
        &self,
        start: Bound<Version>,
        end: Bound<Version>,
    ) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>> {
        Ok(Box::new(self.storage.range(start, end)?))
    }

    fn scan(&self) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>> {
        Ok(Box::new(self.storage.scan()?))
    }

    fn count(&self, version: Version) -> Result<usize> {
        self.storage.count(version)
    }
}
