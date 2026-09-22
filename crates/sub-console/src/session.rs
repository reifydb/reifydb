// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	fs,
	future::poll_fn,
	io,
	path::{Path, PathBuf},
};

use reifydb_console_protocol::{
	ExternalAccess, PROTOCOL_VERSION, ProtocolError, Refusal, Register, Reply, read_message, write_message,
};
use reifydb_sub_server_ws::acceptor::{Access, WsStreamAcceptor};
use reifydb_value::value::duration::Duration;
use socket2::{SockRef, TcpKeepalive};
use tokio::{net::TcpStream, select, sync::watch, time::sleep};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing::{error, info, warn};
use yamux::{Config, Connection, Mode};

use crate::backoff::Backoff;

pub(crate) struct SessionConfig {
	pub(crate) address: String,

	pub(crate) token: String,

	pub(crate) fingerprint_path: Option<PathBuf>,

	pub(crate) external_access: ExternalAccess,
}

#[derive(Debug)]
pub(crate) enum SessionError {
	ReadFingerprint {
		path: PathBuf,
		source: io::Error,
	},
	WriteFingerprint {
		path: PathBuf,
		source: io::Error,
	},
	ConfigureSocket(io::Error),
}

impl Display for SessionError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			SessionError::ReadFingerprint {
				path,
				source,
			} => write!(f, "reading fingerprint {}: {source}", path.display()),
			SessionError::WriteFingerprint {
				path,
				source,
			} => write!(f, "writing fingerprint {}: {source}", path.display()),
			SessionError::ConfigureSocket(source) => write!(f, "configuring tunnel socket: {source}"),
		}
	}
}

enum Attempt {
	Registered(TcpStream, String),
	Refused(Refusal),
	Failed(ProtocolError),
}

enum Served {
	Shutdown,
	Lost,
}

pub(crate) async fn run(
	config: SessionConfig,
	acceptor: WsStreamAcceptor,
	mut shutdown: watch::Receiver<bool>,
) -> Result<(), SessionError> {
	let mut backoff = Backoff::new();
	let mut fingerprint = read_fingerprint(config.fingerprint_path.as_deref())?;
	loop {
		let attempt = select! {
			_ = shutdown.changed() => return Ok(()),
			attempt = register(&config, fingerprint.clone()) => attempt?,
		};
		match attempt {
			Attempt::Registered(tcp, minted) => {
				write_fingerprint(config.fingerprint_path.as_deref(), &minted)?;
				info!(fingerprint = %minted, "registered with the tunnel server");
				fingerprint = Some(minted);
				backoff.reset();
				if let Served::Shutdown = serve(tcp, &acceptor, stream_access(config.external_access), &mut shutdown).await {
					return Ok(());
				}
			}
			Attempt::Refused(reason) if reason.is_final() => {
				error!(?reason, "tunnel server refused registration, console stopped");
				return Ok(());
			}
			Attempt::Refused(reason) => warn!(?reason, "tunnel server refused registration, retrying"),
			Attempt::Failed(e) => warn!(error = %e, "tunnel registration failed, retrying"),
		}
		select! {
			_ = sleep(backoff.next().to_std()) => {}
			_ = shutdown.changed() => return Ok(()),
		}
	}
}

async fn register(config: &SessionConfig, fingerprint: Option<String>) -> Result<Attempt, SessionError> {
	let mut tcp = match TcpStream::connect(config.address.as_str()).await {
		Ok(tcp) => tcp,
		Err(e) => return Ok(Attempt::Failed(e.into())),
	};
	configure_socket(&tcp).map_err(SessionError::ConfigureSocket)?;
	let register = Register {
		protocol_version: PROTOCOL_VERSION,
		token: config.token.clone(),
		fingerprint,
		version: env!("CARGO_PKG_VERSION").to_string(),
		external_access: config.external_access,
	};
	match handshake(&mut tcp, &register).await {
		Ok(Reply::Registered {
			fingerprint,
			..
		}) => Ok(Attempt::Registered(tcp, fingerprint)),
		Ok(Reply::Refused {
			reason,
		}) => Ok(Attempt::Refused(reason)),
		Err(e) => Ok(Attempt::Failed(e)),
	}
}

async fn handshake(tcp: &mut TcpStream, register: &Register) -> Result<Reply, ProtocolError> {
	write_message(tcp, register).await?;
	read_message(tcp).await
}

fn configure_socket(tcp: &TcpStream) -> io::Result<()> {
	tcp.set_nodelay(true)?;
	let keepalive = TcpKeepalive::new()
		.with_time(Duration::from_seconds_const(30).to_std())
		.with_interval(Duration::from_seconds_const(10).to_std())
		.with_retries(3);
	SockRef::from(tcp).set_tcp_keepalive(&keepalive)
}

fn stream_access(external: ExternalAccess) -> Option<Access> {
	match external {
		ExternalAccess::Off => None,
		ExternalAccess::Query => Some(Access::Query),
		ExternalAccess::Command => Some(Access::Command),
		ExternalAccess::Admin => Some(Access::Admin),
	}
}

async fn serve(
	tcp: TcpStream,
	acceptor: &WsStreamAcceptor,
	access: Option<Access>,
	shutdown: &mut watch::Receiver<bool>,
) -> Served {
	let mut connection = Connection::new(tcp.compat(), Config::default(), Mode::Server);
	loop {
		select! {
			_ = shutdown.changed() => return Served::Shutdown,
			inbound = poll_fn(|cx| connection.poll_next_inbound(cx)) => match inbound {
				Some(Ok(stream)) => match access {
					Some(access) => acceptor.accept(stream.compat(), None, access),
					None => drop(stream),
				},
				Some(Err(e)) => {
					warn!(error = %e, "tunnel session failed");
					return Served::Lost;
				}
				None => {
					warn!("tunnel session closed");
					return Served::Lost;
				}
			},
		}
	}
}

fn read_fingerprint(path: Option<&Path>) -> Result<Option<String>, SessionError> {
	let Some(path) = path else {
		return Ok(None);
	};
	match fs::read_to_string(path) {
		Ok(contents) => Ok(Some(contents.trim().to_string())),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
		Err(source) => Err(SessionError::ReadFingerprint {
			path: path.to_path_buf(),
			source,
		}),
	}
}

fn write_fingerprint(path: Option<&Path>, fingerprint: &str) -> Result<(), SessionError> {
	let Some(path) = path else {
		return Ok(());
	};
	save(path, fingerprint).map_err(|source| SessionError::WriteFingerprint {
		path: path.to_path_buf(),
		source,
	})
}

fn save(path: &Path, fingerprint: &str) -> io::Result<()> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}
	fs::write(path, fingerprint)
}
