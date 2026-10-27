// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use async_trait::async_trait;
use reifydb_value::{error::Error, params::Params, value::frame::frame::Frame};

use crate::{
	AdminResult, ChangePayload, CommandResult, LoginResult, QueryResult, WireFormat, subscription::SubscribeItem,
};

#[async_trait]
pub trait Subscription: Send {
	fn subscription_id(&self) -> &str;
	async fn recv(&mut self) -> Option<ChangePayload>;
}

#[async_trait]
pub trait ReifyClient: Send {
	fn wire_format(&self) -> WireFormat;
	fn is_authenticated(&self) -> bool;

	async fn authenticate(&mut self, token: &str) -> Result<(), Error>;
	async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error>;
	async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error>;
	async fn logout(&mut self) -> Result<(), Error>;

	async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error>;
	async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error>;
	async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error>;
	async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error>;
	async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error>;
	async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error>;
	async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error>;
	async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error>;

	async fn subscribe<'a>(&self, items: &[SubscribeItem<'a>]) -> Result<Vec<Box<dyn Subscription>>, Error>;
	async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), Error>;
}
