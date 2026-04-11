// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use std::sync::mpsc;

#[cfg(not(reifydb_single_threaded))]
use reifydb_client::{Encoding, GrpcClient};
#[cfg(not(reifydb_single_threaded))]
use reifydb_runtime::SharedRuntime;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::error::Diagnostic;
use reifydb_type::error::Error;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::{params::Params, value::frame::frame::Frame};

#[cfg(not(reifydb_single_threaded))]
pub struct RemoteRegistry {
	runtime: SharedRuntime,
}

#[cfg(not(reifydb_single_threaded))]
impl RemoteRegistry {
	pub fn new(runtime: SharedRuntime) -> Self {
		Self {
			runtime,
		}
	}

	pub fn forward_query(
		&self,
		address: &str,
		rql: &str,
		params: Params,
		token: Option<&str>,
	) -> Result<Vec<Frame>, Error> {
		let client = self.connect(address, token)?;

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

		rx.recv().map_err(|_| {
			Error(Box::new(Diagnostic {
				code: "REMOTE_002".to_string(),
				message: "remote query channel closed".to_string(),
				..Default::default()
			}))
		})?
	}

	fn connect(&self, address: &str, token: Option<&str>) -> Result<GrpcClient, Error> {
		let address_owned = address.to_string();
		let (tx, rx) = mpsc::sync_channel(1);

		self.runtime.spawn(async move {
			let result = GrpcClient::connect(&address_owned, Encoding::Json).await;
			let _ = tx.send(result);
		});

		let mut client = rx.recv().map_err(|_| {
			Error(Box::new(Diagnostic {
				code: "REMOTE_002".to_string(),
				message: "remote connect channel closed".to_string(),
				..Default::default()
			}))
		})??;
		if let Some(token) = token {
			client.authenticate(token);
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

/// Extract the remote service token from a REMOTE_001 error diagnostic.
pub fn extract_remote_token(err: &Error) -> Option<String> {
	err.0.notes.iter().find_map(|n| n.strip_prefix("Remote token: ")).map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
	use reifydb_type::{error::Diagnostic, fragment::Fragment};

	use super::*;

	fn make_remote_error(address: &str) -> Error {
		Error(Box::new(Diagnostic {
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
		}))
	}

	#[test]
	fn test_is_remote_query_true() {
		let err = make_remote_error("http://localhost:50051");
		assert!(is_remote_query(&err));
	}

	#[test]
	fn test_is_remote_query_false() {
		let err = Error(Box::new(Diagnostic {
			code: "CATALOG_001".to_string(),
			message: "Table not found".to_string(),
			fragment: Fragment::None,
			..Default::default()
		}));
		assert!(!is_remote_query(&err));
	}

	#[test]
	fn test_extract_remote_address() {
		let err = make_remote_error("http://localhost:50051");
		assert_eq!(extract_remote_address(&err), Some("http://localhost:50051".to_string()));
	}

	#[test]
	fn test_extract_remote_address_missing() {
		let err = Error(Box::new(Diagnostic {
			code: "REMOTE_001".to_string(),
			message: "Some error".to_string(),
			notes: vec![],
			fragment: Fragment::None,
			..Default::default()
		}));
		assert_eq!(extract_remote_address(&err), None);
	}

	#[test]
	fn test_extract_remote_token() {
		let err = Error(Box::new(Diagnostic {
			code: "REMOTE_001".to_string(),
			message: "Remote namespace".to_string(),
			notes: vec![
				"Namespace 'test' is configured as a remote namespace".to_string(),
				"Remote gRPC address: http://localhost:50051".to_string(),
				"Remote token: my-secret".to_string(),
			],
			fragment: Fragment::None,
			..Default::default()
		}));
		assert_eq!(extract_remote_token(&err), Some("my-secret".to_string()));
	}

	#[test]
	fn test_extract_remote_token_missing() {
		let err = make_remote_error("http://localhost:50051");
		assert_eq!(extract_remote_token(&err), None);
	}
}
