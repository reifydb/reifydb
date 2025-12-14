// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
mod consumer;
#[allow(dead_code, unused_variables)]
mod engine;
pub mod ffi;
#[allow(dead_code, unused_variables)]
mod operator;
pub mod subsystem;
pub mod transaction;
pub mod worker;

pub use builder::FlowBuilder;
pub use engine::*;
pub use operator::{
	Operator, stateful,
	transform::{TransformOperator, TransformOperatorFactory, extract},
};
pub use reifydb_core::Result;
pub use subsystem::{FlowSubsystem, FlowSubsystemFactory};
pub use transaction::{FlowTransaction, FlowTransactionMetrics};
