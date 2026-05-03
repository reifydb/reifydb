// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

#[derive(Clone, Debug)]
pub enum ComponentType {
	Package,

	Module,

	Subsystem,

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

#[derive(Clone, Debug)]
pub struct SystemVersion {
	pub name: String,

	pub version: String,

	pub description: String,

	pub r#type: ComponentType,
}

pub trait HasVersion {
	fn version(&self) -> SystemVersion;
}
