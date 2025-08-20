// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{subsystem::SubsystemFactory, Transaction};
use reifydb_sub_logging::LoggingBuilder;

pub trait WithSubsystem<T: Transaction>: Sized {
	fn with_logging<F>(self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static;

	fn with_subsystem(self, factory: Box<dyn SubsystemFactory<T>>) -> Self;
}
