// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Frame;
use reifydb_core::interface::{
    Engine as _, Params, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_engine::Engine;
use std::marker::PhantomData;

pub struct OnCreateContext<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub engine: Engine<VT, UT>,
    _phantom: PhantomData<(VT, UT)>,
}

impl<'a, VT, UT> OnCreateContext<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(engine: Engine<VT, UT>) -> Self {
        Self { engine, _phantom: PhantomData }
    }

    /// Execute a transactional query as the specified principal
    pub fn command_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: impl Into<Params>,
    ) -> Result<Vec<Frame>, reifydb_core::Error> {
        self.engine.command_as(principal, rql, params.into())
    }

    /// Execute a transactional query as root user
    pub fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_core::Error> {
        let principal = Principal::System { id: 0, name: "root".to_string() };
        self.engine.command_as(&principal, rql, params.into())
    }

    /// Execute a read-only query as the specified principal
    pub fn query_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: impl Into<Params>,
    ) -> Result<Vec<Frame>, reifydb_core::Error> {
        self.engine.query_as(principal, rql, params.into())
    }

    /// Execute a read-only query as root user
    pub fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_core::Error> {
        let principal = Principal::root();
        self.engine.query_as(&principal, rql, params.into())
    }
}
