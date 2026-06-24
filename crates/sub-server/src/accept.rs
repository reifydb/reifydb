// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	io,
	net::SocketAddr,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};

use libc::{EMFILE, ENFILE};
use reifydb_value::value::duration::Duration;
use tokio::{
	io::{AsyncRead, AsyncWrite, ReadBuf},
	net::{TcpListener, TcpStream},
	sync::{OwnedSemaphorePermit, Semaphore},
	time::sleep,
};
use tracing::warn;

pub const BACKOFF_MIN: Duration = Duration::from_milliseconds_const(5);

pub const BACKOFF_MAX: Duration = Duration::from_milliseconds_const(1000);

pub fn is_fd_exhausted(e: &io::Error) -> bool {
	matches!(e.raw_os_error(), Some(code) if code == EMFILE || code == ENFILE)
}

pub async fn backoff_after_accept_error(e: &io::Error, backoff: &mut Duration, name: &str) {
	if is_fd_exhausted(e) {
		warn!("{name}: accept failed, file descriptors exhausted: {e}; retrying in {backoff:?}");
	} else {
		warn!("{name}: accept error: {e}; retrying in {backoff:?}");
	}
	sleep(backoff.to_std()).await;
	*backoff = (*backoff * 2).min(BACKOFF_MAX);
}

pub async fn accept_admitted(
	listener: &TcpListener,
	semaphore: &Arc<Semaphore>,
	name: &str,
) -> (TcpStream, OwnedSemaphorePermit, SocketAddr) {
	let mut backoff = BACKOFF_MIN;
	loop {
		match listener.accept().await {
			Ok((stream, peer)) => match Arc::clone(semaphore).try_acquire_owned() {
				Ok(permit) => {
					if let Err(e) = stream.set_nodelay(true) {
						warn!("{name}: failed to set TCP_NODELAY: {e}");
					}
					return (stream, permit, peer);
				}
				Err(_) => {
					warn!("{name}: connection limit reached, rejecting {peer}");
					drop(stream);
				}
			},
			Err(e) => backoff_after_accept_error(&e, &mut backoff, name).await,
		}
	}
}

pub struct PermittedStream {
	inner: TcpStream,
	_permit: OwnedSemaphorePermit,
}

impl PermittedStream {
	pub fn new(inner: TcpStream, permit: OwnedSemaphorePermit) -> Self {
		Self {
			inner,
			_permit: permit,
		}
	}
}

impl AsyncRead for PermittedStream {
	fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for PermittedStream {
	fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}

	fn poll_write_vectored(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[io::IoSlice<'_>],
	) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
	}

	fn is_write_vectored(&self) -> bool {
		self.inner.is_write_vectored()
	}
}

#[cfg(test)]
mod tests {
	use std::io::{Error, ErrorKind};

	use libc::ECONNABORTED;

	use super::*;

	#[test]
	fn detects_fd_exhaustion_errors() {
		assert!(is_fd_exhausted(&Error::from_raw_os_error(EMFILE)));
		assert!(is_fd_exhausted(&Error::from_raw_os_error(ENFILE)));
	}

	#[test]
	fn ignores_other_errors() {
		assert!(!is_fd_exhausted(&Error::from_raw_os_error(ECONNABORTED)));
		// An error with no OS code (e.g. a synthesized one) must not be treated as fd exhaustion.
		assert!(!is_fd_exhausted(&Error::new(ErrorKind::Other, "boom")));
	}
}
