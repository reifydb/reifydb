// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use std::{
	collections::HashMap,
	sync::{Mutex, mpsc},
};

#[cfg(not(reifydb_single_threaded))]
use reifydb_client::{GrpcClient, WireFormat};
#[cfg(not(reifydb_single_threaded))]
use reifydb_runtime::SharedRuntime;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::error::Diagnostic;
use reifydb_type::error::Error;
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::{params::Params, value::frame::frame::Frame};

#[cfg(not(reifydb_single_threaded))]
type CacheKey = (String, Option<String>);

#[cfg(not(reifydb_single_threaded))]
pub struct RemoteRegistry {
	runtime: SharedRuntime,
	clients: Mutex<HashMap<CacheKey, GrpcClient>>,
}

#[cfg(not(reifydb_single_threaded))]
impl RemoteRegistry {
	pub fn new(runtime: SharedRuntime) -> Self {
		Self {
			runtime,
			clients: Mutex::new(HashMap::new()),
		}
	}

	pub fn forward_query(
		&self,
		address: &str,
		rql: &str,
		params: Params,
		token: Option<&str>,
	) -> Result<Vec<Frame>, Error> {
		let params_opt = match &params {
			Params::None => None,
			_ => Some(params),
		};

		let client = self.get_or_connect(address, token)?;
		match self.run_query(&client, rql, params_opt.clone()) {
			Ok(frames) => Ok(frames),
			Err(e) if is_transport_error(&e) => {
				self.evict(address, token);
				let client = self.get_or_connect(address, token)?;
				self.run_query(&client, rql, params_opt)
			}
			Err(e) => Err(e),
		}
	}

	fn run_query(&self, client: &GrpcClient, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		let client = client.clone();
		let rql = rql.to_string();
		let (tx, rx) = mpsc::sync_channel(1);

		self.runtime.spawn(async move {
			let result = client.query(&rql, params).await;
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

	fn get_or_connect(&self, address: &str, token: Option<&str>) -> Result<GrpcClient, Error> {
		let key = cache_key(address, token);
		if let Some(c) = self.clients.lock().unwrap().get(&key) {
			return Ok(c.clone());
		}
		let client = self.connect(address, token)?;
		self.clients.lock().unwrap().entry(key).or_insert_with(|| client.clone());
		Ok(client)
	}

	fn evict(&self, address: &str, token: Option<&str>) {
		self.clients.lock().unwrap().remove(&cache_key(address, token));
	}

	#[cfg(test)]
	fn cache_len(&self) -> usize {
		self.clients.lock().unwrap().len()
	}

	fn connect(&self, address: &str, token: Option<&str>) -> Result<GrpcClient, Error> {
		let address_owned = address.to_string();
		let (tx, rx) = mpsc::sync_channel(1);

		self.runtime.spawn(async move {
			let result = GrpcClient::connect(&address_owned, WireFormat::Proto).await;
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

#[cfg(not(reifydb_single_threaded))]
fn cache_key(address: &str, token: Option<&str>) -> CacheKey {
	(address.to_string(), token.map(str::to_string))
}

/// Transport-level gRPC errors mean the cached channel may be dead.
/// `status_to_error` (reifydb-client) tags these with a `GRPC_` code prefix when
/// the Status message isn't a JSON-encoded application `Diagnostic`.
#[cfg(not(reifydb_single_threaded))]
fn is_transport_error(err: &Error) -> bool {
	err.0.code.starts_with("GRPC_")
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
	use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
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

	#[test]
	fn test_is_transport_error() {
		let grpc_err = Error(Box::new(Diagnostic {
			code: "GRPC_Unavailable".to_string(),
			message: "channel closed".to_string(),
			..Default::default()
		}));
		assert!(is_transport_error(&grpc_err));

		let app_err = Error(Box::new(Diagnostic {
			code: "CATALOG_001".to_string(),
			message: "Table not found".to_string(),
			..Default::default()
		}));
		assert!(!is_transport_error(&app_err));
	}

	#[test]
	fn test_cache_key_distinguishes_tokens() {
		assert_ne!(cache_key("addr", Some("a")), cache_key("addr", Some("b")));
		assert_ne!(cache_key("addr", None), cache_key("addr", Some("a")));
		assert_eq!(cache_key("addr", Some("a")), cache_key("addr", Some("a")));
	}

	#[test]
	fn test_connect_failure_does_not_pollute_cache() {
		let runtime = SharedRuntime::from_config(SharedRuntimeConfig::default());
		let registry = RemoteRegistry::new(runtime);

		// 127.0.0.1:1 is reserved; connect must fail fast.
		let err = registry.forward_query("http://127.0.0.1:1", "FROM x", Params::None, None).unwrap_err();
		assert!(err.0.code.starts_with("GRPC_") || err.0.code == "REMOTE_002");
		assert_eq!(registry.cache_len(), 0);
	}

	#[test]
	fn test_evict_missing_key_is_noop() {
		let runtime = SharedRuntime::from_config(SharedRuntimeConfig::default());
		let registry = RemoteRegistry::new(runtime);
		registry.evict("http://127.0.0.1:1", None);
		registry.evict("http://127.0.0.1:1", Some("tok"));
		assert_eq!(registry.cache_len(), 0);
	}
}
