// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::{
	interface::catalog::{config::GetConfig, flow::OperatorId},
	row::OperatorSettings,
};

pub trait ListOperatorSettings: Clone + Send + Sync + 'static {
	fn list_operator_settings(&self) -> Vec<(OperatorId, OperatorSettings)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}
