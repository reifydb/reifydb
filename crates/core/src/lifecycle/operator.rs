// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::{
	interface::catalog::{config::GetConfig, flow::OperatorId},
	row::OperatorRetention,
};

pub trait ListOperatorRetention: Clone + Send + Sync + 'static {
	fn list_operator_retention(&self) -> Vec<(OperatorId, OperatorRetention)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}
