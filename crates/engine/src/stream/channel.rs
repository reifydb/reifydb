// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Channel-based stream implementation for query results.
//!
//! Provides backpressure through bounded channels - when the buffer is full,
//! the producer (query executor) will wait, allowing memory usage to be bounded.

use std::{
	pin::Pin,
	sync::atomic::{AtomicU64, Ordering},
	task::{Context, Poll},
};

use futures_util::Stream;
use reifydb_core::{
	Frame,
	stream::{StreamError, StreamResult},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Counter for generating unique stream IDs.
static STREAM_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Unique identifier for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(u64);

impl StreamId {
	/// Generate a new unique stream ID.
	pub fn next() -> Self {
		Self(STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
	}
}

/// Stream implementation backed by a bounded mpsc channel.
///
/// This provides natural backpressure - when the buffer is full,
/// the producer (query executor) will wait, allowing memory
/// to be bounded.
pub struct ChannelFrameStream {
	receiver: mpsc::Receiver<StreamResult<Frame>>,
	cancel_token: CancellationToken,
	cancelled_sent: bool,
}

impl ChannelFrameStream {
	/// Create a new channel-based stream with the given buffer size.
	pub fn new(buffer_size: usize, cancel_token: CancellationToken) -> (FrameSender, Self) {
		let (tx, rx) = mpsc::channel(buffer_size);
		let sender = FrameSender {
			sender: tx,
		};
		let stream = Self {
			receiver: rx,
			cancel_token,
			cancelled_sent: false,
		};
		(sender, stream)
	}
}

impl Stream for ChannelFrameStream {
	type Item = StreamResult<Frame>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		// Check cancellation first
		if self.cancel_token.is_cancelled() && !self.cancelled_sent {
			self.cancelled_sent = true;
			return Poll::Ready(Some(Err(StreamError::Cancelled)));
		}

		Pin::new(&mut self.receiver).poll_recv(cx)
	}
}

/// Sender half for producing frames into the stream.
pub struct FrameSender {
	sender: mpsc::Sender<StreamResult<Frame>>,
}

impl FrameSender {
	/// Send a frame to the stream (waits if buffer is full).
	pub async fn send(&self, frame: StreamResult<Frame>) -> Result<(), StreamError> {
		self.sender.send(frame).await.map_err(|_| StreamError::Disconnected)
	}

	/// Send a frame synchronously (for use in spawn_blocking contexts).
	/// Blocks the current thread if the buffer is full.
	pub fn blocking_send(&self, frame: StreamResult<Frame>) -> Result<(), StreamError> {
		self.sender.blocking_send(frame).map_err(|_| StreamError::Disconnected)
	}

	/// Try to send without waiting (returns error if buffer is full or closed).
	pub fn try_send(&self, frame: StreamResult<Frame>) -> Result<(), StreamError> {
		self.sender.try_send(frame).map_err(|_| StreamError::Disconnected)
	}

	/// Check if there's capacity without blocking.
	pub fn has_capacity(&self) -> bool {
		self.sender.capacity() > 0
	}

	/// Check if the receiver has been dropped.
	pub fn is_closed(&self) -> bool {
		self.sender.is_closed()
	}
}

impl Clone for FrameSender {
	fn clone(&self) -> Self {
		Self {
			sender: self.sender.clone(),
		}
	}
}

/// Handle for controlling a running stream query.
#[derive(Clone)]
pub struct StreamHandle {
	cancel_token: CancellationToken,
	stream_id: StreamId,
}

impl StreamHandle {
	/// Create a new stream handle.
	pub fn new(cancel_token: CancellationToken) -> Self {
		Self {
			cancel_token,
			stream_id: StreamId::next(),
		}
	}

	/// Cancel the running query.
	pub fn cancel(&self) {
		self.cancel_token.cancel();
	}

	/// Check if the query has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.cancel_token.is_cancelled()
	}

	/// Get the stream's unique identifier.
	pub fn id(&self) -> StreamId {
		self.stream_id
	}
}
