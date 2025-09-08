// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt;

/// Type of system component
#[derive(Clone, Debug)]
pub enum ComponentKind {
	/// Main database package
	Package,
	/// Core library module
	Module,
	/// Runtime subsystem
	Subsystem,
	/// Build-time information
	Build,
}

impl fmt::Display for ComponentKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ComponentKind::Package => write!(f, "package"),
			ComponentKind::Module => write!(f, "module"),
			ComponentKind::Subsystem => write!(f, "subsystem"),
			ComponentKind::Build => write!(f, "build"),
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
	pub kind: ComponentKind,
}

/// Trait for components that provide version information
pub trait HasVersion {
	/// Returns the version information for this component
	fn version(&self) -> SystemVersion;
}
