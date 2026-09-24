// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{
	error::Error as StdError,
	fmt::{self, Display, Formatter},
	io,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{from_slice, to_vec};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Register {
	pub protocol_version: u32,
	pub token: String,
	pub fingerprint: Option<String>,
	pub version: String,
	#[serde(default)]
	pub external_access: ExternalAccess,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAccess {
	#[default]
	Off,
	Query,
	Command,
	Admin,
}

impl ExternalAccess {
	pub fn as_str(self) -> &'static str {
		match self {
			ExternalAccess::Off => "off",
			ExternalAccess::Query => "query",
			ExternalAccess::Command => "command",
			ExternalAccess::Admin => "admin",
		}
	}

	pub fn parse(value: &str) -> Option<ExternalAccess> {
		match value {
			"off" => Some(ExternalAccess::Off),
			"query" => Some(ExternalAccess::Query),
			"command" => Some(ExternalAccess::Command),
			"admin" => Some(ExternalAccess::Admin),
			_ => None,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Reply {
	Registered {
		fingerprint: String,
		protocol_version: u32,
	},
	Refused {
		reason: Refusal,
	},
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Refusal {
	UnknownToken,
	UnsupportedVersion {
		supported: u32,
	},
	FingerprintInUse,
	Unavailable,
}

impl Refusal {
	pub fn is_final(&self) -> bool {
		!matches!(self, Refusal::Unavailable)
	}
}

#[derive(Debug)]
pub enum ProtocolError {
	Io(io::Error),
	TooLarge(usize),
	Malformed(String),
}

impl Display for ProtocolError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ProtocolError::Io(e) => write!(f, "io: {e}"),
			ProtocolError::TooLarge(len) => write!(f, "frame of {len} bytes exceeds {MAX_FRAME}"),
			ProtocolError::Malformed(why) => write!(f, "malformed message: {why}"),
		}
	}
}

impl StdError for ProtocolError {}

impl From<io::Error> for ProtocolError {
	fn from(value: io::Error) -> Self {
		ProtocolError::Io(value)
	}
}

pub async fn write_message<W, T>(writer: &mut W, message: &T) -> Result<(), ProtocolError>
where
	W: AsyncWrite + Unpin,
	T: Serialize,
{
	let body = to_vec(message).map_err(|e| ProtocolError::Malformed(e.to_string()))?;
	if body.len() > MAX_FRAME {
		return Err(ProtocolError::TooLarge(body.len()));
	}
	writer.write_u32(body.len() as u32).await?;
	writer.write_all(&body).await?;
	writer.flush().await?;
	Ok(())
}

pub async fn read_message<R, T>(reader: &mut R) -> Result<T, ProtocolError>
where
	R: AsyncRead + Unpin,
	T: DeserializeOwned,
{
	let len = reader.read_u32().await? as usize;
	if len > MAX_FRAME {
		return Err(ProtocolError::TooLarge(len));
	}
	let mut body = vec![0u8; len];
	reader.read_exact(&mut body).await?;
	from_slice(&body).map_err(|e| ProtocolError::Malformed(e.to_string()))
}
