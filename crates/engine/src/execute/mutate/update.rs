// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::Executor;
use crate::frame::Frame;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::plan::physical::UpdatePlan;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn update(
        &mut self,
        _tx: &mut impl Tx<VS, US>,
        _plan: UpdatePlan,
    ) -> crate::Result<Frame> {
        // TODO: Implement update logic
        // For now, return a placeholder frame indicating update is not yet implemented
        todo!("UPDATE execution not yet implemented")
    }
}