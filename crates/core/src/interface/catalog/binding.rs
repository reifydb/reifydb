// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{BindingId, NamespaceId, ProcedureId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HttpMethod {
	Get,
	Post,
	Put,
	Patch,
	Delete,
}

impl HttpMethod {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Get => "GET",
			Self::Post => "POST",
			Self::Put => "PUT",
			Self::Patch => "PATCH",
			Self::Delete => "DELETE",
		}
	}

	pub fn parse(s: &str) -> Option<Self> {
		match s {
			"GET" => Some(Self::Get),
			"POST" => Some(Self::Post),
			"PUT" => Some(Self::Put),
			"PATCH" => Some(Self::Patch),
			"DELETE" => Some(Self::Delete),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BindingFormat {
	Json,
	Frames,
	Rbcf,
}

impl BindingFormat {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Json => "json",
			Self::Frames => "frames",
			Self::Rbcf => "rbcf",
		}
	}

	pub fn parse(s: &str) -> Option<Self> {
		match s {
			"json" => Some(Self::Json),
			"frames" => Some(Self::Frames),
			"rbcf" => Some(Self::Rbcf),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindingProtocol {
	Http {
		method: HttpMethod,
		path: String,
	},
	Grpc {
		name: String,
	},
	Ws {
		name: String,
	},
}

impl BindingProtocol {
	pub fn protocol_str(&self) -> &'static str {
		match self {
			Self::Http {
				..
			} => "http",
			Self::Grpc {
				..
			} => "grpc",
			Self::Ws {
				..
			} => "ws",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
	pub id: BindingId,
	pub namespace: NamespaceId,
	pub name: String,
	pub procedure_id: ProcedureId,
	pub protocol: BindingProtocol,
	pub format: BindingFormat,
	pub enabled: bool,
}

impl Binding {
	pub fn id(&self) -> BindingId {
		self.id
	}

	pub fn namespace(&self) -> NamespaceId {
		self.namespace
	}

	pub fn procedure_id(&self) -> ProcedureId {
		self.procedure_id
	}
}
