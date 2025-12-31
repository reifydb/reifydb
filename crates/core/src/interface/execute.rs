// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;

use crate::{
	Frame,
	interface::{
		CommandTransaction, Identity, Params, QueryTransaction, WithEventBus, interceptor::WithInterceptors,
	},
	value::column::Columns,
};

/// A batch of columnar data returned from query execution
#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

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

#[async_trait]
pub trait ExecuteCommand<CT: CommandTransaction + WithInterceptors<CT> + WithEventBus> {
	async fn execute_command(&self, txn: &mut CT, cmd: Command<'_>) -> crate::Result<Vec<Frame>>;
}

#[async_trait]
pub trait ExecuteQuery<QT: QueryTransaction> {
	async fn execute_query(&self, txn: &mut QT, qry: Query<'_>) -> crate::Result<Vec<Frame>>;
}
