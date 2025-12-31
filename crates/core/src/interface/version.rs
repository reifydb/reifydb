// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt;

/// Type of system component
#[derive(Clone, Debug)]
pub enum ComponentType {
	/// Main database package
	Package,
	/// Core library module
	Module,
	/// Runtime subsystem
	Subsystem,
	/// Build information
	Build,
}

impl fmt::Display for ComponentType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ComponentType::Package => write!(f, "package"),
			ComponentType::Module => write!(f, "module"),
			ComponentType::Subsystem => write!(f, "subsystem"),
			ComponentType::Build => write!(f, "build"),
		}
	}
}

/// Represents version information for a system component
#[derive(Clone, Debug)]
pub struct SystemVersion {
	/// Name of the component
	pub name: String,
	/// Version string (semantic version, git hash, etc.)
	pub version: String,
	/// Human-readable description of the component
	pub description: String,
	/// Type of component
	pub r#type: ComponentType,
}

/// Trait for components that provide version information
pub trait HasVersion {
	/// Returns the version information for this component
	fn version(&self) -> SystemVersion;
}
