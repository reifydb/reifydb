// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod coordinator;
pub(crate) mod dispatcher;
#[allow(dead_code, unused_variables)]
mod engine;
pub mod ffi;
pub(crate) mod lag;
#[allow(dead_code, unused_variables)]
pub(crate) mod operator;
pub(crate) mod registry;
pub(crate) mod routing;
pub mod subsystem;
pub(crate) mod tracker;
pub mod transaction;

pub use builder::FlowBuilder;
pub use config::FlowRuntimeConfig;
pub use consumer::IndependentFlowConsumer;
pub use engine::*;
pub use lag::FlowLags;
pub use operator::{
	Operator, stateful,
	transform::{TransformOperator, TransformOperatorFactory, extract},
};
pub use reifydb_core::Result;
// Re-export FlowLagRow and FlowLagsProvider trait from core for convenience
pub use reifydb_core::interface::{FlowLagRow, FlowLagsProvider};
pub use subsystem::{FlowSubsystem, FlowSubsystemFactory};
pub use transaction::{FlowTransaction, FlowTransactionMetrics};
