// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Frame,
	interface::{
		CommandTransaction, Identity, Params, QueryTransaction, WithEventBus, interceptor::WithInterceptors,
	},
};

#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

pub trait Execute<CT: CommandTransaction + WithInterceptors<CT> + WithEventBus, QT: QueryTransaction>:
	ExecuteCommand<CT> + ExecuteQuery<QT>
{
}

pub trait ExecuteCommand<CT: CommandTransaction + WithInterceptors<CT> + WithEventBus> {
	fn execute_command(&self, txn: &mut CT, cmd: Command<'_>) -> crate::Result<Vec<Frame>>;
}

pub trait ExecuteQuery<QT: QueryTransaction> {
	fn execute_query(&self, txn: &mut QT, qry: Query<'_>) -> crate::Result<Vec<Frame>>;
}
