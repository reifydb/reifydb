// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use async_trait::async_trait;
use reifydb_value::{error::ValueError, params::Params, value::frame::frame::Frame};

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

	async fn authenticate(&mut self, token: &str) -> Result<(), ValueError>;
	async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, ValueError>;
	async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, ValueError>;
	async fn logout(&mut self) -> Result<(), ValueError>;

	async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, ValueError>;
	async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, ValueError>;
	async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, ValueError>;
	async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, ValueError>;
	async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, ValueError>;
	async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, ValueError>;
	async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, ValueError>;
	async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, ValueError>;

	async fn subscribe<'a>(&self, items: &[SubscribeItem<'a>]) -> Result<Vec<Box<dyn Subscription>>, ValueError>;
	async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), ValueError>;
}
