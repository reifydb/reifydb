// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	net::{IpAddr::V4, Ipv4Addr, SocketAddr},
	ops::Deref,
	sync::Arc,
};

use reifydb_core::{Error, interface::Transaction};
use reifydb_engine::StandardEngine;
use tokio::{net::TcpListener, sync::OnceCell};
use tonic::service::InterceptorLayer;

use crate::grpc::server::{db::DbService, grpc::db_server::DbServer};

pub mod auth;
mod db;

pub(crate) mod grpc {
	tonic::include_proto!("reifydb");
}

const DEFAULT_SOCKET: SocketAddr =
	SocketAddr::new(V4(Ipv4Addr::new(0, 0, 0, 0)), 54321);

#[derive(Debug)]
pub struct GrpcConfig {
	pub socket: Option<SocketAddr>,
}

impl Default for GrpcConfig {
	fn default() -> Self {
		Self {
			socket: Some(DEFAULT_SOCKET),
		}
	}
}

#[derive(Clone)]
pub struct GrpcServer<T: Transaction>(Arc<Inner<T>>);

pub struct Inner<T: Transaction> {
	config: GrpcConfig,
	engine: StandardEngine<T>,
	socket_addr: OnceCell<SocketAddr>,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> Deref for GrpcServer<T> {
	type Target = Inner<T>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<T: Transaction> GrpcServer<T> {
	pub fn new(config: GrpcConfig, engine: StandardEngine<T>) -> Self {
		Self(Arc::new(Inner {
			config,
			engine,
			socket_addr: OnceCell::new(),
			_phantom: std::marker::PhantomData,
		}))
	}

	pub async fn serve(self) -> Result<(), Error> {
		let listener = TcpListener::bind(
			self.config.socket.unwrap_or(DEFAULT_SOCKET),
		)
		.await
		.unwrap();

		self.socket_addr.set(listener.local_addr().unwrap()).unwrap();
		let incoming = tokio_stream::wrappers::TcpListenerStream::new(
			listener,
		);

		tonic::transport::Server::builder()
			.layer(InterceptorLayer::new(auth::AuthInterceptor {}))
			.add_service(db_service(self.engine.clone()))
			.serve_with_incoming(incoming)
			.await
			.unwrap();

		Ok(())
	}

	pub fn socket_addr(&self) -> Option<SocketAddr> {
		self.socket_addr.get().cloned()
	}
}

// FIXME return result
pub fn db_service<T: Transaction>(
	engine: StandardEngine<T>,
) -> DbServer<DbService<T>> {
	DbServer::new(DbService::new(engine))
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
	// pub user_id: String,
	// pub roles: Vec<String>,
	// add more fields like email, tenant_id, etc.
}
