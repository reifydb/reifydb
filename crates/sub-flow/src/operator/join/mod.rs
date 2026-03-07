// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Internal modules
pub mod column;
pub mod operator;
pub mod state;
pub mod store;
pub mod strategy;

// All types are accessed directly from their submodules:
// - crate::operator::join::operator::JoinOperator
// - crate::operator::join::state::{JoinSide, JoinSideEntry, JoinState}
// - crate::operator::join::store::Store
// - crate::operator::join::strategy::JoinStrategy
