// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;
mod command;
mod params;
mod query;

pub use builder::{CommandSessionBuilder, QuerySessionBuilder};
pub use command::CommandSession;
pub use params::{RqlParams, RqlValue};
pub use query::QuerySession;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};

pub trait IntoCommandSession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_command_session(self) -> crate::Result<CommandSession<VT, UT>>;
}

pub trait IntoQuerySession<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn into_query_session(self) -> crate::Result<QuerySession<VT, UT>>;
}
