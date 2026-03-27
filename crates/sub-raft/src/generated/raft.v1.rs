// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// Generated from proto/raft.proto — hand-written (no protoc codegen).

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftMessage {
	#[prost(bytes = "vec", tag = "1")]
	pub payload: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct RaftAck {}

pub mod raft_transport_client {
	#![allow(unused_variables, dead_code, missing_docs, clippy::wildcard_imports, clippy::let_unit_value)]
	use tonic::codegen::{http::uri::PathAndQuery, *};

	#[derive(Debug, Clone)]
	pub struct RaftTransportClient<T> {
		inner: tonic::client::Grpc<T>,
	}

	impl RaftTransportClient<tonic::transport::Channel> {
		pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
		where
			D: TryInto<tonic::transport::Endpoint>,
			D::Error: Into<StdError>,
		{
			let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
			Ok(Self::new(conn))
		}
	}

	impl<T> RaftTransportClient<T>
	where
		T: tonic::client::GrpcService<tonic::body::Body>,
		T::Error: Into<StdError>,
		T::ResponseBody: Body<Data = tonic::codegen::Bytes> + std::marker::Send + 'static,
		<T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
	{
		pub fn new(inner: T) -> Self {
			let inner = tonic::client::Grpc::new(inner);
			Self {
				inner,
			}
		}

		pub async fn send(
			&mut self,
			request: impl tonic::IntoRequest<super::RaftMessage>,
		) -> std::result::Result<tonic::Response<super::RaftAck>, tonic::Status> {
			self.inner
				.ready()
				.await
				.map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;
			let codec = tonic_prost::ProstCodec::default();
			let path = PathAndQuery::from_static("/reifydb.raft.v1.RaftTransport/Send");
			let mut req = request.into_request();
			req.extensions_mut().insert(GrpcMethod::new("reifydb.raft.v1.RaftTransport", "Send"));
			self.inner.unary(req, path, codec).await
		}
	}
}

pub mod raft_transport_server {
	#![allow(unused_variables, dead_code, missing_docs, clippy::wildcard_imports, clippy::let_unit_value)]
	use tonic::codegen::*;

	#[async_trait]
	pub trait RaftTransport: std::marker::Send + std::marker::Sync + 'static {
		async fn send(
			&self,
			request: tonic::Request<super::RaftMessage>,
		) -> std::result::Result<tonic::Response<super::RaftAck>, tonic::Status>;
	}

	#[derive(Debug)]
	pub struct RaftTransportServer<T> {
		inner: Arc<T>,
	}

	impl<T> Clone for RaftTransportServer<T> {
		fn clone(&self) -> Self {
			Self {
				inner: self.inner.clone(),
			}
		}
	}

	impl<T: RaftTransport> RaftTransportServer<T> {
		pub fn new(inner: T) -> Self {
			Self {
				inner: Arc::new(inner),
			}
		}
	}

	impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftTransportServer<T>
	where
		T: RaftTransport,
		B: Body + std::marker::Send + 'static,
		B::Error: Into<StdError> + std::marker::Send + 'static,
	{
		type Response = http::Response<tonic::body::Body>;
		type Error = std::convert::Infallible;
		type Future = BoxFuture<Self::Response, Self::Error>;

		fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn call(&mut self, req: http::Request<B>) -> Self::Future {
			match req.uri().path() {
				"/reifydb.raft.v1.RaftTransport/Send" => {
					struct SendSvc<T: RaftTransport>(pub Arc<T>);
					impl<T: RaftTransport> tonic::server::UnaryService<super::RaftMessage> for SendSvc<T> {
						type Response = super::RaftAck;
						type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
						fn call(
							&mut self,
							request: tonic::Request<super::RaftMessage>,
						) -> Self::Future {
							let inner = Arc::clone(&self.0);
							let fut = async move {
								<T as RaftTransport>::send(&inner, request).await
							};
							Box::pin(fut)
						}
					}
					let inner = self.inner.clone();
					let fut = async move {
						let method = SendSvc(inner);
						let codec = tonic_prost::ProstCodec::default();
						let mut grpc = tonic::server::Grpc::new(codec);
						let res = grpc.unary(method, req).await;
						Ok(res)
					};
					Box::pin(fut)
				}
				_ => Box::pin(async move {
					let mut response = http::Response::new(tonic::body::Body::default());
					let headers = response.headers_mut();
					headers.insert(
						tonic::Status::GRPC_STATUS,
						(tonic::Code::Unimplemented as i32).into(),
					);
					headers.insert(http::header::CONTENT_TYPE, tonic::metadata::GRPC_CONTENT_TYPE);
					Ok(response)
				}),
			}
		}
	}

	impl<T: RaftTransport> tonic::server::NamedService for RaftTransportServer<T> {
		const NAME: &'static str = "reifydb.raft.v1.RaftTransport";
	}
}
