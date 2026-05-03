// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::actor::{context::Context, system::ActorConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
	Continue,

	Yield,

	Park,

	Stop,
}

pub trait Actor: Send + Sync + 'static {
	type State: Send + 'static;

	type Message: Send + 'static;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State;

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive;

	#[allow(unused_variables)]
	fn idle(&self, ctx: &Context<Self::Message>) -> Directive {
		Directive::Park
	}

	fn post_stop(&self) {}

	fn config(&self) -> ActorConfig {
		ActorConfig::default()
	}
}
