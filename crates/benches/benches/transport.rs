// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Display, Formatter, Result as FmtResult};

use reifydb::engine::{engine::StandardEngine, session::Session};
use reifydb_client::{GrpcClient, HttpClient, QueueClaimRequest, WireFormat, WsClient};
use reifydb_testing_scenario::query::OperationKind;
use reifydb_value::{
	params::Params,
	value::{duration::Duration as ValueDuration, frame::frame::Frame, identity::IdentityId},
};
use tokio::runtime::{Builder, Runtime};

pub const DEFAULT_HTTP_PORT: u16 = 18190;
pub const DEFAULT_WS_PORT: u16 = 18191;
pub const DEFAULT_GRPC_PORT: u16 = 18192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
	Embedded,
	Http,
	Ws,
	Grpc,
}

pub const ALL_TRANSPORTS: [Transport; 4] = [Transport::Embedded, Transport::Http, Transport::Ws, Transport::Grpc];

impl Transport {
	pub fn label(self) -> &'static str {
		match self {
			Transport::Embedded => "embedded",
			Transport::Http => "http",
			Transport::Ws => "ws",
			Transport::Grpc => "grpc",
		}
	}

	pub fn parse(raw: &str) -> Option<Self> {
		ALL_TRANSPORTS.into_iter().find(|transport| transport.label() == raw.trim())
	}

	pub fn is_wire(self) -> bool {
		self != Transport::Embedded
	}

	pub fn port(self) -> Option<u16> {
		match self {
			Transport::Embedded => None,
			Transport::Http => Some(DEFAULT_HTTP_PORT),
			Transport::Ws => Some(DEFAULT_WS_PORT),
			Transport::Grpc => Some(DEFAULT_GRPC_PORT),
		}
	}

	pub fn url(self) -> Option<String> {
		let port = self.port()?;
		let scheme = match self {
			Transport::Ws => "ws",
			_ => "http",
		};
		Some(format!("{}://127.0.0.1:{}", scheme, port))
	}
}

impl Display for Transport {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		f.write_str(self.label())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
	Anonymous,
	Root,
	System,
}

pub const ALL_IDENTITIES: [Identity; 3] = [Identity::Anonymous, Identity::Root, Identity::System];

impl Identity {
	pub fn label(self) -> &'static str {
		match self {
			Identity::Anonymous => "anonymous",
			Identity::Root => "root",
			Identity::System => "system",
		}
	}

	pub fn parse(raw: &str) -> Option<Self> {
		ALL_IDENTITIES.into_iter().find(|identity| identity.label() == raw.trim())
	}

	pub fn id(self) -> IdentityId {
		match self {
			Identity::Anonymous => IdentityId::anonymous(),
			Identity::Root => IdentityId::root(),
			Identity::System => IdentityId::system(),
		}
	}

	pub fn is_privileged(self) -> bool {
		self.id().is_privileged()
	}
}

impl Display for Identity {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		f.write_str(self.label())
	}
}

pub struct Timing {
	pub compile_ns: u64,
	pub execute_ns: u64,
}

pub enum Driver {
	Embedded {
		engine: StandardEngine,
		identity: IdentityId,
		session: Session,
	},
	Wire {
		runtime: Runtime,
		client: WireClient,
	},
}

pub enum WireClient {
	Http(Box<HttpClient>),
	Ws(Box<WsClient>),
	Grpc(Box<GrpcClient>),
}

impl Driver {
	pub fn connect(
		transport: Transport,
		engine: &StandardEngine,
		format: WireFormat,
		identity: Identity,
		token: Option<&str>,
	) -> Self {
		let Some(url) = transport.url() else {
			return Driver::Embedded {
				engine: engine.clone(),
				identity: identity.id(),
				session: Session::trusted(engine.clone(), identity.id()),
			};
		};

		let runtime = Builder::new_current_thread().enable_all().build().expect("worker tokio runtime builds");

		let client = runtime.block_on(async {
			match transport {
				Transport::Http => {
					let mut client =
						HttpClient::connect(&url, format).await.expect("http client connects");
					if let Some(token) = token {
						client.authenticate(token);
					}
					WireClient::Http(Box::new(client))
				}
				Transport::Ws => {
					let mut client =
						WsClient::connect(&url, format).await.expect("ws client connects");
					if let Some(token) = token {
						client.authenticate(token).await.expect("ws client authenticates");
					}
					WireClient::Ws(Box::new(client))
				}
				Transport::Grpc => {
					let mut client =
						GrpcClient::connect(&url, format).await.expect("grpc client connects");
					if let Some(token) = token {
						client.authenticate(token);
					}
					WireClient::Grpc(Box::new(client))
				}
				Transport::Embedded => unreachable!("embedded transport has no url"),
			}
		});

		Driver::Wire {
			runtime,
			client,
		}
	}

	#[allow(dead_code)]
	pub fn command_frames(&self, rql: &str) -> Vec<Frame> {
		match self {
			Driver::Embedded {
				engine,
				identity,
				..
			} => {
				let result = engine.command_as(*identity, rql, Params::None);
				if let Some(error) = result.error {
					panic!("command `{}` failed: {}", rql, error);
				}
				result.frames
			}
			Driver::Wire {
				runtime,
				client,
			} => runtime.block_on(async {
				let outcome = match client {
					WireClient::Http(client) => client.command(rql, None).await,
					WireClient::Ws(client) => client.command(rql, None).await,
					WireClient::Grpc(client) => client.command(rql, None).await,
				};

				match outcome {
					Ok(frames) => frames,
					Err(error) => panic!("command `{}` failed: {}", rql, error),
				}
			}),
		}
	}

	#[allow(dead_code)]
	pub fn claim_frames(
		&self,
		queue: &str,
		worker: &str,
		max_n: u32,
		lease_ttl: ValueDuration,
		wait_for: ValueDuration,
	) -> Vec<Frame> {
		match self {
			Driver::Embedded {
				session,
				..
			} => {
				let result = session.claim_wait(queue, worker, max_n, lease_ttl, wait_for);
				if let Some(error) = result.error {
					panic!("claim on `{}` failed: {}", queue, error);
				}
				result.frames
			}
			Driver::Wire {
				runtime,
				client,
			} => runtime.block_on(async {
				let request = QueueClaimRequest {
					queue: queue.to_string(),
					worker: worker.to_string(),
					max_n: Some(max_n),
					lease_ttl: Some(duration_literal(lease_ttl)),
					wait_for: wait_for.is_positive().then(|| duration_literal(wait_for)),
				};

				let outcome = match client {
					WireClient::Http(client) => client.queue_claim(request).await,
					WireClient::Ws(client) => client.queue_claim(request).await,
					WireClient::Grpc(client) => client.queue_claim(request).await,
				};

				match outcome {
					Ok(frames) => frames,
					Err(error) => panic!("claim on `{}` failed: {}", queue, error),
				}
			}),
		}
	}

	pub fn execute(&self, kind: OperationKind, rql: &str) -> Option<Timing> {
		match self {
			Driver::Embedded {
				engine,
				identity,
				..
			} => {
				let result = match kind {
					OperationKind::Query => engine.query_as(*identity, rql, Params::None),
					OperationKind::Command => engine.command_as(*identity, rql, Params::None),
					OperationKind::Admin => engine.admin_as(*identity, rql, Params::None),
				};

				if let Some(error) = result.error {
					panic!("query `{}` failed: {}", rql, error);
				}

				let mut timing = Timing {
					compile_ns: 0,
					execute_ns: 0,
				};
				for statement in &result.metrics.statements {
					timing.compile_ns += statement.compile_duration.to_std().as_nanos() as u64;
					timing.execute_ns += statement.execute_duration.to_std().as_nanos() as u64;
				}
				Some(timing)
			}
			Driver::Wire {
				runtime,
				client,
			} => {
				runtime.block_on(async {
					let outcome = match (client, kind) {
						(WireClient::Http(client), OperationKind::Query) => {
							client.query(rql, None).await.map(|_| ())
						}
						(WireClient::Http(client), _) => {
							client.command(rql, None).await.map(|_| ())
						}
						(WireClient::Ws(client), OperationKind::Query) => {
							client.query(rql, None).await.map(|_| ())
						}
						(WireClient::Ws(client), _) => {
							client.command(rql, None).await.map(|_| ())
						}
						(WireClient::Grpc(client), OperationKind::Query) => {
							client.query(rql, None).await.map(|_| ())
						}
						(WireClient::Grpc(client), _) => {
							client.command(rql, None).await.map(|_| ())
						}
					};

					if let Err(error) = outcome {
						panic!("query `{}` failed: {}", rql, error);
					}
				});

				None
			}
		}
	}
}

#[allow(dead_code)]
fn duration_literal(value: ValueDuration) -> String {
	format!("{}ms", value.to_std().as_millis())
}
