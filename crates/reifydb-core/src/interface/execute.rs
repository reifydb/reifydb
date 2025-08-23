// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::interceptor::WithInterceptors;
use crate::interface::{CommandTransaction, QueryTransaction, WithHooks};
use crate::{
	interface::{Identity, Params},
	Frame,
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

pub trait Execute<CT: CommandTransaction + WithInterceptors<CT> + WithHooks>:
	ExecuteCommand<CT> + ExecuteQuery
{
}

pub trait ExecuteCommand<
	CT: CommandTransaction + WithInterceptors<CT> + WithHooks,
>
{
	fn execute_command(
		&self,
		txn: &mut CT,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>>;
}

pub trait ExecuteQuery {
	fn execute_query(
		&self,
		txn: &mut impl QueryTransaction,
		qry: Query<'_>,
	) -> crate::Result<Vec<Frame>>;
}
