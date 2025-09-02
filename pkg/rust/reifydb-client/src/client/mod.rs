// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod message;
mod router;
mod utils;
mod worker;

use std::{
	sync::{Arc, Mutex, mpsc},
	thread::JoinHandle,
};

pub(crate) use message::{InternalMessage, ResponseRoute};
pub(crate) use router::RequestRouter;
pub(crate) use utils::generate_request_id;

use crate::{
	WebSocketClient,
	session::{
		BlockingSession, CallbackSession, ChannelSession,
		ResponseMessage,
	},
};

/// Main client that owns the WebSocket connection
#[derive(Clone)]
pub struct Client {
	inner: Arc<ClientInner>,
}

pub(crate) struct ClientInner {
	pub(crate) command_tx: mpsc::Sender<InternalMessage>,
	pub(crate) router: Arc<Mutex<RequestRouter>>,
	worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
	pub(crate) url: String,
}

impl Client {
	/// Connect to the ReifyDB server
	/// Supports both plain addresses (e.g., "127.0.0.1:8080") and WebSocket
	/// URLs (e.g., "ws://127.0.0.1:8080")
	pub fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
		let (command_tx, command_rx) = mpsc::channel();
		let router = Arc::new(Mutex::new(RequestRouter::new()));

		// Verify connection by creating a test WebSocket client
		let test_client = WebSocketClient::connect(url)?;
		drop(test_client); // Close test connection

		// Start the background worker thread
		let router_clone = router.clone();
		let url_clone = url.to_string();
		let worker_handle = std::thread::spawn(move || {
			worker::worker_thread(
				url_clone,
				command_rx,
				router_clone,
			);
		});

		Ok(Self {
			inner: Arc::new(ClientInner {
				command_tx,
				router,
				worker_handle: Arc::new(Mutex::new(Some(
					worker_handle,
				))),
				url: url.to_string(),
			}),
		})
	}

	/// Create a blocking session
	pub fn blocking_session(
		&self,
		token: Option<String>,
	) -> Result<BlockingSession, reifydb_type::Error> {
		BlockingSession::new(self.inner.clone(), token)
	}

	/// Create a callback-based session
	pub fn callback_session(
		&self,
		token: Option<String>,
	) -> Result<CallbackSession, reifydb_type::Error> {
		CallbackSession::new(self.inner.clone(), token)
	}

	/// Create a channel-based session
	pub fn channel_session(
		&self,
		token: Option<String>,
	) -> Result<
		(ChannelSession, mpsc::Receiver<ResponseMessage>),
		reifydb_type::Error,
	> {
		ChannelSession::new(self.inner.clone(), token)
	}

	/// Close the client connection
	pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
		self.inner.command_tx.send(InternalMessage::Close)?;

		// Wait for worker thread to finish
		if let Ok(mut handle_guard) = self.inner.worker_handle.lock() {
			if let Some(handle) = handle_guard.take() {
				let _ = handle.join();
			}
		}

		Ok(())
	}
}

impl Drop for Client {
	fn drop(&mut self) {
		let _ = self.inner.command_tx.send(InternalMessage::Close);
	}
}
