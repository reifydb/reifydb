// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	io::{Read, Write},
	net::{SocketAddr, TcpStream, ToSocketAddrs},
	sync::{Arc, Mutex, mpsc},
	thread::JoinHandle,
};

use crate::{
	Request, Response, ResponseMessage, WsBlockingSession,
	WsCallbackSession, WsChannelSession,
	ws::{
		message::InternalMessage,
		protocol::{
			build_ws_frame, calculate_accept_key,
			calculate_frame_size, find_header_end,
			generate_websocket_key, parse_ws_frame,
		},
		router::RequestRouter,
		worker,
	},
};

/// WebSocket client implementation
#[derive(Clone)]
pub struct WsClient {
	inner: Arc<ClientInner>,
}

pub(crate) struct ClientInner {
	pub(crate) command_tx: mpsc::Sender<InternalMessage>,
	worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

// ============================================================================
// WsClient Implementation
// ============================================================================

impl WsClient {
	/// Create a new WebSocket client from URL string
	pub fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
		// Parse the URL to get a socket address
		let socket_addr = Self::parse_ws_url(url)?;

		let (command_tx, command_rx) = mpsc::channel();
		let router = Arc::new(Mutex::new(RequestRouter::new()));

		// Verify connection by creating a test WebSocket client
		let test_client = WebSocketClient::connect(socket_addr)?;
		drop(test_client); // Close test connection

		// Start the background worker thread
		let router_clone = router.clone();
		let socket_addr_clone = socket_addr;
		let worker_handle = std::thread::spawn(move || {
			worker::worker_thread_with_addr(
				socket_addr_clone,
				command_rx,
				router_clone,
			);
		});

		Ok(Self {
			inner: Arc::new(ClientInner {
				command_tx,
				worker_handle: Arc::new(Mutex::new(Some(
					worker_handle,
				))),
			}),
		})
	}

	/// Parse a WebSocket URL to extract the socket address
	fn parse_ws_url(
		url: &str,
	) -> Result<SocketAddr, Box<dyn std::error::Error>> {
		let addr_str =
			if url.starts_with("ws://") {
				&url[5..] // Remove "ws://"
			} else if url.starts_with("wss://") {
				return Err("WSS (secure WebSocket) is not yet supported".into());
			} else {
				url
			};

		// Parse the address string to SocketAddr
		// Handle different formats:
		// - [::1]:8080 (already properly formatted)
		// - ::1:8080 (needs brackets added)
		// - localhost:8080 (hostname)
		// - 127.0.0.1:8080 (IPv4)

		if addr_str.starts_with('[') {
			// Already has brackets, parse as-is
			addr_str.to_socket_addrs()?.next().ok_or_else(|| {
				"Failed to resolve address".into()
			})
		} else if addr_str.starts_with("::") {
			// IPv6 address without brackets
			// Find the last colon that's likely the port separator
			// Count colons - if more than 2, it's IPv6
			let colon_count = addr_str.matches(':').count();
			if colon_count > 2 {
				// Definitely IPv6, find the port
				if let Some(port_start) = addr_str.rfind(':') {
					// Check if what follows is a port
					// number
					if addr_str[port_start + 1..]
						.chars()
						.all(|c| c.is_ascii_digit())
					{
						let ipv6_part =
							&addr_str[..port_start];
						let port_part = &addr_str
							[port_start + 1..];
						let formatted = format!(
							"[{}]:{}",
							ipv6_part, port_part
						);
						return formatted
							.to_socket_addrs()?
							.next()
							.ok_or_else(|| {
								"Failed to resolve address".into()
							});
					}
				}
			}
			// Try as-is
			addr_str.to_socket_addrs()?.next().ok_or_else(|| {
				"Failed to resolve address".into()
			})
		} else {
			// Regular address (hostname or IPv4)
			addr_str.to_socket_addrs()?.next().ok_or_else(|| {
				"Failed to resolve address".into()
			})
		}
	}

	/// Create a new WebSocket client
	pub fn new<A: ToSocketAddrs>(
		addr: A,
	) -> Result<Self, Box<dyn std::error::Error>> {
		// Resolve the address to get the first valid SocketAddr
		let socket_addr = addr
			.to_socket_addrs()?
			.next()
			.ok_or("Failed to resolve address")?;

		let (command_tx, command_rx) = mpsc::channel();
		let router = Arc::new(Mutex::new(RequestRouter::new()));

		// Verify connection by creating a test WebSocket client
		let test_client = WebSocketClient::connect(socket_addr)?;
		drop(test_client); // Close test connection

		// Start the background worker thread
		let router_clone = router.clone();
		let socket_addr_clone = socket_addr;
		let worker_handle = std::thread::spawn(move || {
			worker::worker_thread_with_addr(
				socket_addr_clone,
				command_rx,
				router_clone,
			);
		});

		Ok(Self {
			inner: Arc::new(ClientInner {
				command_tx,
				worker_handle: Arc::new(Mutex::new(Some(
					worker_handle,
				))),
			}),
		})
	}

	/// Create a blocking session
	pub fn blocking_session(
		&self,
		token: Option<String>,
	) -> Result<WsBlockingSession, reifydb_type::Error> {
		WsBlockingSession::new(self.inner.clone(), token)
	}

	/// Create a callback-based session
	pub fn callback_session(
		&self,
		token: Option<String>,
	) -> Result<WsCallbackSession, reifydb_type::Error> {
		WsCallbackSession::new(self.inner.clone(), token)
	}

	/// Create a channel-based session
	pub fn channel_session(
		&self,
		token: Option<String>,
	) -> Result<
		(WsChannelSession, mpsc::Receiver<ResponseMessage>),
		reifydb_type::Error,
	> {
		WsChannelSession::new(self.inner.clone(), token)
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

impl Drop for WsClient {
	fn drop(&mut self) {
		let _ = self.inner.command_tx.send(InternalMessage::Close);
	}
}

/// WebSocket client implementation
pub struct WebSocketClient {
	pub(crate) stream: TcpStream,
	read_buffer: Vec<u8>,
	pub(crate) is_connected: bool,
}

impl WebSocketClient {
	/// Create a new WebSocket client and connect to the specified address
	pub fn connect(
		addr: SocketAddr,
	) -> Result<Self, Box<dyn std::error::Error>> {
		// Connect to the socket address
		let stream = TcpStream::connect(addr)?;
		stream.set_nonblocking(true)?;

		let mut client = WebSocketClient {
			stream,
			read_buffer: Vec::with_capacity(4096),
			is_connected: false,
		};

		// Perform WebSocket handshake
		client.handshake()?;

		Ok(client)
	}

	/// Perform WebSocket handshake
	fn handshake(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		// Generate WebSocket key
		let key = generate_websocket_key();

		// Build handshake request
		let request = format!(
			"GET / HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
			key
		);

		// Send handshake
		self.stream.write_all(request.as_bytes())?;
		self.stream.flush()?;

		// Read response with timeout
		let mut response = Vec::new();
		let mut buffer = [0u8; 1024];
		let start = std::time::Instant::now();
		let timeout = std::time::Duration::from_secs(5);

		loop {
			match self.stream.read(&mut buffer) {
				Ok(0) => return Err(
					"Connection closed during handshake"
						.into(),
				),
				Ok(n) => {
					response.extend_from_slice(
						&buffer[..n],
					);

					// Check if we have the complete HTTP
					// response
					if let Some(end_pos) =
						find_header_end(&response)
					{
						response.truncate(end_pos);
						break;
					}
				}
				Err(e) if e.kind()
					== std::io::ErrorKind::WouldBlock =>
				{
					// No data available yet
					if start.elapsed() > timeout {
						return Err(
							"Handshake timeout"
								.into(),
						);
					}
					std::thread::sleep(std::time::Duration::from_millis(10));
					continue;
				}
				Err(e) => return Err(e.into()),
			}
		}

		// Verify handshake response
		let response_str = String::from_utf8_lossy(&response);
		if !response_str.contains("HTTP/1.1 101") {
			return Err(format!(
				"Invalid handshake response: {}",
				response_str
			)
			.into());
		}

		// Verify Sec-WebSocket-Accept
		let expected_accept = calculate_accept_key(&key);
		if !response_str.contains(&format!(
			"Sec-WebSocket-Accept: {}",
			expected_accept
		)) {
			return Err("Invalid Sec-WebSocket-Accept".into());
		}

		self.is_connected = true;
		Ok(())
	}

	/// Send a request over the WebSocket connection
	pub(crate) fn send_request(
		&mut self,
		request: &Request,
	) -> Result<(), Box<dyn std::error::Error>> {
		if !self.is_connected {
			return Err("Not connected".into());
		}

		// Serialize request to JSON
		let json = serde_json::to_string(request)?;
		let payload = json.as_bytes();

		// Build WebSocket frame (text frame, opcode = 1)
		let frame = build_ws_frame(0x01, payload, true);

		// Send frame
		self.stream.write_all(&frame)?;
		self.stream.flush()?;

		Ok(())
	}

	/// Receive a response from the WebSocket connection
	pub fn receive(
		&mut self,
	) -> Result<Option<Response>, Box<dyn std::error::Error>> {
		if !self.is_connected {
			return Err("Not connected".into());
		}

		// Read data into buffer
		let mut buf = vec![0u8; 4096];
		match self.stream.read(&mut buf) {
			Ok(0) => {
				self.is_connected = false;
				return Err("Connection closed".into());
			}
			Ok(n) => {
				self.read_buffer.extend_from_slice(&buf[..n]);
			}
			Err(e) if e.kind()
				== std::io::ErrorKind::WouldBlock =>
			{
				// No data available
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
		}

		// Try to parse WebSocket frame
		if let Some((opcode, payload)) =
			parse_ws_frame(&self.read_buffer)?
		{
			// Remove parsed frame from buffer
			let frame_size = calculate_frame_size(&payload, false);
			self.read_buffer.drain(..frame_size);

			match opcode {
				0x01 | 0x02 => {
					// Text or binary frame
					let response: Response =
						serde_json::from_slice(
							&payload,
						)?;
					return Ok(Some(response));
				}
				0x08 => {
					// Close frame
					self.is_connected = false;
					return Err(
						"Connection closed by server"
							.into(),
					);
				}
				0x09 => {
					// Ping frame - respond with pong
					let pong = build_ws_frame(
						0x0A, &payload, true,
					);
					self.stream.write_all(&pong)?;
					self.stream.flush()?;
				}
				0x0A => {
					// Pong frame - ignore
				}
				_ => {
					// Unknown opcode
					return Err(format!(
						"Unknown opcode: {}",
						opcode
					)
					.into());
				}
			}
		}

		Ok(None)
	}

	/// Close the WebSocket connection
	pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		if self.is_connected {
			// Send close frame
			let close_frame = build_ws_frame(0x08, &[], true);
			self.stream.write_all(&close_frame)?;
			self.stream.flush()?;
			self.is_connected = false;
		}
		Ok(())
	}

	/// Check if the client is connected
	pub fn is_connected(&self) -> bool {
		self.is_connected
	}
}

impl Drop for WebSocketClient {
	fn drop(&mut self) {
		let _ = self.close();
	}
}
