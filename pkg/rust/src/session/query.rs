// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::RqlParams;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::Frame;
use reifydb_engine::Engine;

pub struct QuerySession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) engine: Engine<VT, UT>,
    pub(crate) principal: Principal,
}

impl<VT, UT> QuerySession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{

    pub(crate) fn new(engine: Engine<VT, UT>, principal: Principal) -> Self {
        Self { engine, principal }
    }

    pub fn query(&self, rql: &str, params: impl Into<RqlParams>) -> crate::Result<Vec<Frame>> {
        let params = params.into();
        let substituted_rql = params.substitute(rql)?;
        self.engine.query_as(&self.principal, &substituted_rql)
    }
}
