// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::VecDeque,
	io::Write,
	net::SocketAddr,
	sync::mpsc::{self, Receiver},
	time::Instant,
};

use mio::{Interest, Token, net::TcpStream};
use mpsc::TryRecvError;
use reifydb_core::Result;
use reifydb_engine::StandardEngine;
use reifydb_sub_api::SchedulerService;

use crate::protocols::{
	http::HttpState,
	ws::{CommandResponse, ErrorResponse, QueryResponse, Response, WsState},
};

/// Buffer management for connection buffers
const INITIAL_BUFFER_SIZE: usize = 8192;
const MAX_BUFFER_SIZE: usize = 1024 * 1024; // 1MB max
const SHRINK_THRESHOLD: usize = 64 * 1024; // Shrink if buffer grows above 64KB and is mostly empty

/// Connection state for protocol detection and handling
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
	/// Initial state - detecting protocol
	Detecting,
	/// WebSocket protocol
	WebSocket(WsState),
	/// HTTP protocol
	Http(HttpState),
	/// Connection closed
	Closed,
}

/// Type of request being processed
#[derive(Debug, Clone)]
pub enum RequestType {
	HttpCommand,
	HttpQuery,
	WebSocketCommand,
	WebSocketQuery,
}

/// Protocol-specific response ready to be sent
#[derive(Debug)]
pub enum PendingResponse {
	HttpCommand(CommandResponse),
	HttpQuery(QueryResponse),
	HttpCommandError(ErrorResponse),
	HttpQueryError(ErrorResponse),
	WebSocketCommand(CommandResponse),
	WebSocketQuery(QueryResponse),
	WebSocketCommandError(ErrorResponse),
	WebSocketQueryError(ErrorResponse),
}

/// Represents a pending query/command execution
pub struct PendingQuery {
	pub receiver: Receiver<Result<Response>>,
	pub request_type: RequestType,
}

/// Generic connection wrapper that can handle multiple protocols
pub struct Connection {
	stream: TcpStream,
	peer: SocketAddr,
	token: Token,
	state: ConnectionState,
	engine: StandardEngine,
	scheduler: SchedulerService,
	created_at: Instant,
	last_activity: Instant,
	buffer: Vec<u8>,
	// Track pending queries with their response channels
	pending_queries: VecDeque<PendingQuery>,
	// Queue of responses ready to be written
	response_queue: VecDeque<PendingResponse>,
}

impl Connection {
	pub fn new(
		stream: TcpStream,
		peer: SocketAddr,
		token: Token,
		engine: StandardEngine,
		scheduler: SchedulerService,
	) -> Self {
		let now = Instant::now();
		Self {
			stream,
			peer,
			token,
			state: ConnectionState::Detecting,
			engine,
			scheduler,
			created_at: now,
			last_activity: now,
			buffer: Vec::with_capacity(INITIAL_BUFFER_SIZE),
			pending_queries: VecDeque::new(),
			response_queue: VecDeque::new(),
		}
	}

	pub fn stream(&mut self) -> &mut TcpStream {
		&mut self.stream
	}

	pub fn peer(&self) -> SocketAddr {
		self.peer
	}

	pub fn token(&self) -> Token {
		self.token
	}

	pub fn state(&self) -> &ConnectionState {
		&self.state
	}

	pub fn state_mut(&mut self) -> &mut ConnectionState {
		self.last_activity = Instant::now();
		&mut self.state
	}

	pub fn set_state(&mut self, state: ConnectionState) {
		self.state = state;
		self.last_activity = Instant::now();
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	pub fn buffer(&self) -> &[u8] {
		&self.buffer
	}

	pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
		self.last_activity = Instant::now();
		&mut self.buffer
	}

	/// Smart buffer management - shrink if buffer has grown large but is
	/// mostly empty
	pub fn optimize_buffer(&mut self) {
		let buffer_capacity = self.buffer.capacity();
		let buffer_len = self.buffer.len();

		// If buffer capacity is large but usage is low, shrink it
		if buffer_capacity > SHRINK_THRESHOLD && buffer_len < buffer_capacity / 4 {
			// Shrink to 2x current usage or initial size, whichever
			// is larger
			let new_capacity = std::cmp::max(buffer_len * 2, INITIAL_BUFFER_SIZE);

			if new_capacity < buffer_capacity {
				self.buffer.shrink_to(new_capacity);
			}
		}

		// Ensure buffer doesn't grow beyond max size
		if buffer_capacity > MAX_BUFFER_SIZE {
			self.buffer.truncate(MAX_BUFFER_SIZE);
			self.buffer.shrink_to(MAX_BUFFER_SIZE);
		}
	}

	pub fn reset_buffer(&mut self) {
		self.buffer.clear();
		if self.buffer.capacity() > INITIAL_BUFFER_SIZE * 4 {
			self.buffer.shrink_to(INITIAL_BUFFER_SIZE);
		}
	}

	/// Properly close the TCP connection with shutdown
	pub fn shutdown(&mut self) {
		self.cancel_pending_queries();
		let _ = self.stream.flush();
		let _ = self.stream.shutdown(std::net::Shutdown::Both);
		self.state = ConnectionState::Closed;
		self.reset_buffer()
	}

	pub fn interests(&self) -> Interest {
		match &self.state {
			ConnectionState::Detecting => Interest::READABLE,
			ConnectionState::WebSocket(ws_state) => ws_state.interests(),
			ConnectionState::Http(http_state) => http_state.interests(),
			ConnectionState::Closed => Interest::READABLE,
		}
	}

	pub fn age(&self) -> std::time::Duration {
		self.created_at.elapsed()
	}

	pub fn idle_time(&self) -> std::time::Duration {
		self.last_activity.elapsed()
	}

	/// Submit a query for async execution
	pub fn submit_query(&mut self, rx: Receiver<Result<Response>>, request_type: RequestType) {
		self.pending_queries.push_back(PendingQuery {
			receiver: rx,
			request_type,
		});
	}

	/// Check for completed queries and move results to response queue
	pub fn poll_pending_queries(&mut self) -> bool {
		let mut has_responses = false;

		while let Some(pending) = self.pending_queries.pop_front() {
			match pending.receiver.try_recv() {
				Ok(result) => {
					use crate::protocols::ws::ResponsePayload;

					let response = match result {
						Ok(response) => match (response.payload, &pending.request_type) {
							(
								ResponsePayload::Command(cmd_resp),
								RequestType::HttpCommand,
							) => Some(PendingResponse::HttpCommand(cmd_resp)),
							(
								ResponsePayload::Query(query_resp),
								RequestType::HttpQuery,
							) => Some(PendingResponse::HttpQuery(query_resp)),
							(
								ResponsePayload::Command(cmd_resp),
								RequestType::WebSocketCommand,
							) => Some(PendingResponse::WebSocketCommand(cmd_resp)),
							(
								ResponsePayload::Query(query_resp),
								RequestType::WebSocketQuery,
							) => Some(PendingResponse::WebSocketQuery(query_resp)),
							(ResponsePayload::Err(err_resp), RequestType::HttpCommand) => {
								Some(PendingResponse::HttpCommandError(err_resp))
							}
							(ResponsePayload::Err(err_resp), RequestType::HttpQuery) => {
								Some(PendingResponse::HttpQueryError(err_resp))
							}
							(
								ResponsePayload::Err(err_resp),
								RequestType::WebSocketCommand,
							) => Some(PendingResponse::WebSocketCommandError(err_resp)),
							(
								ResponsePayload::Err(err_resp),
								RequestType::WebSocketQuery,
							) => Some(PendingResponse::WebSocketQueryError(err_resp)),
							_ => {
								eprintln!("Mismatched response type and request type");
								None
							}
						},
						Err(e) => {
							// Task execution failed (shouldn't happen as errors are wrapped
							// in Response)
							eprintln!("Task execution failed: {}", e);
							None
						}
					};

					if let Some(resp) = response {
						self.response_queue.push_back(resp);
						has_responses = true;
					}
				}
				Err(TryRecvError::Empty) => {
					// Query still pending, put it back
					self.pending_queries.push_front(pending);
					break; // Preserve order by stopping here
				}
				Err(TryRecvError::Disconnected) => {
					// Channel disconnected, query was cancelled
					// This is expected if worker pool is shutting down
				}
			}
		}

		has_responses
	}

	/// Check if there are responses ready to write
	pub fn has_pending_responses(&self) -> bool {
		!self.response_queue.is_empty()
	}

	/// Get the next response to write
	pub fn next_response(&mut self) -> Option<PendingResponse> {
		self.response_queue.pop_front()
	}

	/// Get the scheduler service
	pub fn scheduler(&self) -> &SchedulerService {
		&self.scheduler
	}

	/// Get the number of pending queries
	pub fn pending_query_count(&self) -> usize {
		self.pending_queries.len()
	}

	/// Cancel all pending queries (for connection close)
	pub fn cancel_pending_queries(&mut self) {
		self.pending_queries.clear();
		self.response_queue.clear();
	}
}
