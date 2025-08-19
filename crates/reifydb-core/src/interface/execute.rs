// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Frame,
	interface::{
		CommandTransaction, Identity, Params, QueryTransaction,
		Transaction,
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

pub trait Execute<T: Transaction>: ExecuteCommand<T> + ExecuteQuery<T> {}

pub trait ExecuteCommand<T: Transaction> {
	fn execute_command<'a>(
		&'a self,
		txn: &mut CommandTransaction<T>,
		cmd: Command<'a>,
	) -> crate::Result<Vec<Frame>>;
}

pub trait ExecuteQuery<T: Transaction> {
	fn execute_query<'a>(
		&'a self,
		txn: &mut QueryTransaction<T>,
		qry: Query<'a>,
	) -> crate::Result<Vec<Frame>>;
}
