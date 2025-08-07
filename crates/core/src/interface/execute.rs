// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Frame;
use crate::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, Params, Principal, UnversionedTransaction,
    VersionedTransaction,
};

pub struct Command<'a> {
    pub rql: &'a str,
    pub params: Params,
    pub principal: &'a Principal,
}

pub struct Query<'a> {
    pub rql: &'a str,
    pub params: Params,
    pub principal: &'a Principal,
}

pub trait ExecuteCommand<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn execute_command<'a>(
        &'a self,
        atx: &mut ActiveCommandTransaction<VT, UT>,
        cmd: Command<'a>,
    ) -> crate::Result<Vec<Frame>>;
}

pub trait ExecuteQuery<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn execute_query<'a>(
        &'a self,
        atx: &mut ActiveQueryTransaction<VT, UT>,
        qry: Query<'a>,
    ) -> crate::Result<Vec<Frame>>;
}
