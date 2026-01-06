// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod convert;
mod engine;
pub mod ffi;
pub(crate) mod lag;
pub(crate) mod r#loop;
#[allow(dead_code)]
mod operator;
pub mod subsystem;
pub(crate) mod tracker;
pub mod transaction;

pub use builder::FlowBuilder;
pub use config::FlowRuntimeConfig;
pub use engine::*;
pub use lag::FlowLags;
pub use operator::{
	Operator, stateful,
	transform::{TransformOperator, TransformOperatorFactory, extract},
};
// Re-export FlowLagRow and FlowLagsProvider trait from core for convenience
pub use reifydb_core::interface::FlowLagRow;
pub use reifydb_core::{Result, interface::FlowLagsProvider};
pub use subsystem::{FlowSubsystem, FlowSubsystemFactory};
pub use transaction::FlowTransaction;
