// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod convert;
pub(crate) mod coordinator;
mod engine;
pub mod ffi;
pub(crate) mod flow;
pub(crate) mod lag;
#[allow(dead_code)]
mod operator;
pub(crate) mod provider;
pub(crate) mod registry;
pub mod subsystem;
pub(crate) mod tracker;
pub mod transaction;

pub use builder::FlowBuilder;
pub use config::FlowRuntimeConfig;
pub use engine::*;
pub use lag::FlowLagsV2;
pub use operator::{
	Operator, stateful,
	transform::{TransformOperator, TransformOperatorFactory, extract},
};
pub use registry::FlowConsumerRegistry;
pub use reifydb_core::Result;
// Re-export FlowLagRow and FlowLagsProvider trait from core for convenience
pub use reifydb_core::interface::{FlowLagRow, FlowLagsProvider};
pub use subsystem::{FlowSubsystem, FlowSubsystemFactory};
pub use transaction::FlowTransaction;
