// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{RwLock, mpsc},
};

use reifydb_client::GrpcClient;
use reifydb_runtime::SharedRuntime;
use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
	value::frame::frame::Frame,
};

pub struct RemoteRegistry {
	connections: RwLock<HashMap<String, GrpcClient>>,
	runtime: SharedRuntime,
}

impl RemoteRegistry {
	pub fn new(runtime: SharedRuntime) -> Self {
		Self {
			connections: RwLock::new(HashMap::new()),
			runtime,
		}
	}

	pub fn forward_query(&self, address: &str, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
		let client = self.get_or_connect(address)?;
		let params_opt = match &params {
			Params::None => None,
			_ => Some(params),
		};

		let rql = rql.to_string();
		let (tx, rx) = mpsc::sync_channel(1);

		self.runtime.spawn(async move {
			let result = client.query(&rql, params_opt).await.map(|r| r.frames);
			let _ = tx.send(result);
		});

		rx.recv()
			.map_err(|_| {
				Error(Diagnostic {
					code: "REMOTE_002".to_string(),
					message: "remote query channel closed".to_string(),
					..Default::default()
				})
			})?
			.map_err(Into::into)
	}

	fn get_or_connect(&self, address: &str) -> Result<GrpcClient, Error> {
		// Fast path: check read lock
		{
			let cache = self.connections.read().unwrap();
			if let Some(client) = cache.get(address) {
				return Ok(client.clone());
			}
		}

		// Slow path: connect via spawn + channel
		let address_owned = address.to_string();
		let (tx, rx) = mpsc::sync_channel(1);

		self.runtime.spawn(async move {
			let result = GrpcClient::connect(&address_owned).await;
			let _ = tx.send(result);
		});

		let client = rx.recv().map_err(|_| {
			Error(Diagnostic {
				code: "REMOTE_002".to_string(),
				message: "remote connect channel closed".to_string(),
				..Default::default()
			})
		})??;
		{
			let mut cache = self.connections.write().unwrap();
			cache.entry(address.to_string()).or_insert(client.clone());
		}
		Ok(client)
	}
}

/// Check if an error represents a remote namespace query (REMOTE_001).
pub fn is_remote_query(err: &Error) -> bool {
	err.0.code == "REMOTE_001"
}

/// Extract the remote gRPC address from a REMOTE_001 error diagnostic.
pub fn extract_remote_address(err: &Error) -> Option<String> {
	err.0.notes.iter().find_map(|n| n.strip_prefix("Remote gRPC address: ")).map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
	use reifydb_type::{error::Diagnostic, fragment::Fragment};

	use super::*;

	fn make_remote_error(address: &str) -> Error {
		Error(Diagnostic {
			code: "REMOTE_001".to_string(),
			message: format!(
				"Remote namespace 'remote_ns': source 'users' is on remote instance at {}",
				address
			),
			notes: vec![
				"Namespace 'remote_ns' is configured as a remote namespace".to_string(),
				format!("Remote gRPC address: {}", address),
			],
			fragment: Fragment::None,
			..Default::default()
		})
	}

	#[test]
	fn test_is_remote_query_true() {
		let err = make_remote_error("http://localhost:50051");
		assert!(is_remote_query(&err));
	}

	#[test]
	fn test_is_remote_query_false() {
		let err = Error(Diagnostic {
			code: "CATALOG_001".to_string(),
			message: "Table not found".to_string(),
			fragment: Fragment::None,
			..Default::default()
		});
		assert!(!is_remote_query(&err));
	}

	#[test]
	fn test_extract_remote_address() {
		let err = make_remote_error("http://localhost:50051");
		assert_eq!(extract_remote_address(&err), Some("http://localhost:50051".to_string()));
	}

	#[test]
	fn test_extract_remote_address_missing() {
		let err = Error(Diagnostic {
			code: "REMOTE_001".to_string(),
			message: "Some error".to_string(),
			notes: vec![],
			fragment: Fragment::None,
			..Default::default()
		});
		assert_eq!(extract_remote_address(&err), None);
	}
}
