// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{interceptor::InterceptorProvider, interface::Transaction};
use reifydb_engine::StandardEngine;

use super::Subsystem;

/// Factory trait for creating subsystems with a two-phase initialization
pub trait SubsystemFactory<T: Transaction>: InterceptorProvider<T> {
	fn create(
		self: Box<Self>,
		engine: StandardEngine<T>,
	) -> Box<dyn Subsystem>;
}
