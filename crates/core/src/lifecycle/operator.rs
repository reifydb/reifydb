// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::{
	interface::catalog::{config::GetConfig, flow::OperatorId},
	row::OperatorHorizon,
};

pub trait ListOperatorHorizon: Clone + Send + Sync + 'static {
	fn list_operator_horizon(&self) -> Vec<(OperatorId, OperatorHorizon)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}
