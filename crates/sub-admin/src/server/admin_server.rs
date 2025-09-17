// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	io::{ErrorKind::WouldBlock, Read, Write},
	net::TcpListener,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

use mio::{
	Events, Interest, Poll, Token,
	net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream},
};
use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use reifydb_network::{HttpRequest, HttpResponse, http::parse_request};
use slab::Slab;

use super::{Route, Router};
use crate::{config::AdminConfig, handlers};

const LISTENER: Token = Token(0);

pub struct AdminServer<T: Transaction> {
	config: AdminConfig,
	engine: StandardEngine<T>,
	running: Arc<AtomicBool>,
	thread_handle: Option<thread::JoinHandle<()>>,
}

impl<T: Transaction> AdminServer<T> {
	pub fn new(config: AdminConfig, engine: StandardEngine<T>) -> Self {
		Self {
			config,
			engine,
			running: Arc::new(AtomicBool::new(false)),
			thread_handle: None,
		}
	}

	pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(());
		}

		println!("Starting admin server on port {}", self.config.port);

		let config = self.config.clone();
		let engine = self.engine.clone();
		let running = self.running.clone();

		let thread_handle = thread::spawn(move || {
			if let Err(e) = Self::run_server(config, engine, running.clone()) {
				eprintln!("Admin server error: {:?}", e);
			}
		});

		self.running.store(true, Ordering::Relaxed);
		self.thread_handle = Some(thread_handle);

		Ok(())
	}

	pub fn stop(&mut self) {
		self.running.store(false, Ordering::Relaxed);

		if let Some(handle) = self.thread_handle.take() {
			let _ = handle.join();
		}
	}

	fn run_server(
		config: AdminConfig,
		engine: StandardEngine<T>,
		running: Arc<AtomicBool>,
	) -> Result<(), Box<dyn std::error::Error>> {
		let listener = TcpListener::bind(&config.address())?;
		listener.set_nonblocking(true)?;

		let mut poll = Poll::new()?;
		let mut events = Events::with_capacity(1024);
		let mut connections: Slab<MioTcpStream> = Slab::new();

		let mut mio_listener = MioTcpListener::from_std(listener);
		poll.registry().register(&mut mio_listener, LISTENER, Interest::READABLE)?;

		println!("Admin server listening on {}", config.address());

		while running.load(Ordering::Relaxed) {
			poll.poll(&mut events, Some(std::time::Duration::from_millis(100)))?;

			for event in events.iter() {
				match event.token() {
					LISTENER => {
						// Accept new connections
						loop {
							match mio_listener.accept() {
								Ok((mut stream, addr)) => {
									println!("Admin connection from: {}", addr);
									let entry = connections.vacant_entry();
									let token = Token(entry.key() + 2); // Offset by 2 to avoid LISTENER and WAKE_TOKEN

									// Register the new connection with the poll
									poll.registry().register(
										&mut stream,
										token,
										Interest::READABLE,
									)?;

									entry.insert(stream);
								}
								Err(ref e) if e.kind() == WouldBlock => {
									break;
								}
								Err(e) => {
									eprintln!("Accept error: {}", e);
									break;
								}
							}
						}
					}
					token if token.0 >= 2 => {
						// Handle existing connection
						// (offset by 2)
						let idx = token.0 - 2;
						if let Some(stream) = connections.get_mut(idx) {
							let mut buffer = vec![0; 8192];
							match stream.read(&mut buffer) {
								Ok(0) => {
									// Connection closed
									poll.registry().deregister(stream)?;
									connections.remove(idx);
								}
								Ok(n) => {
									println!("Received {} bytes", n);
									// Process request
									if let Ok(request) = parse_request(&buffer[..n])
									{
										println!(
											"Request: {} {}",
											request.method, request.path
										);
										let response = Self::handle_request(
											&config, &engine, request,
										);
										match stream
											.write_all(&response.to_bytes())
										{
											Ok(_) => println!(
												"Response sent"
											),
											Err(e) => eprintln!(
												"Failed to send response: {}",
												e
											),
										}
									}
									poll.registry().deregister(stream)?;
									connections.remove(idx);
								}
								Err(ref e) if e.kind() == WouldBlock => {
									// Not ready yet
								}
								Err(e) => {
									eprintln!("Read error: {}", e);
									poll.registry().deregister(stream)?;
									connections.remove(idx);
								}
							}
						}
					}
					_ => {}
				}
			}
		}

		Ok(())
	}

	fn handle_request(config: &AdminConfig, engine: &StandardEngine<T>, request: HttpRequest) -> HttpResponse {
		let route = Router::route(&request.method, &request.path);

		match route {
			Route::Health => handlers::handle_health(engine),
			Route::GetConfig => handlers::handle_get_config(config),
			Route::UpdateConfig => handlers::handle_update_config(config, request),
			Route::Execute => handlers::handle_execute(engine, request),
			Route::Metrics => handlers::handle_metrics(engine),
			Route::Login => handlers::handle_login(config, request),
			Route::Logout => handlers::handle_logout(),
			Route::AuthStatus => handlers::handle_auth_status(config, request),
			Route::ServeIndex => handlers::serve_index(),
			Route::ServeStatic(path) => handlers::serve_static(&path),
			Route::WebSocket => {
				// WebSocket upgrade handled separately
				HttpResponse::bad_request().with_json(r#"{"error":"WebSocket not implemented yet"}"#)
			}
			Route::NotFound => HttpResponse::not_found().with_json(r#"{"error":"Not found"}"#),
		}
	}

	pub fn is_running(&self) -> bool {
		self.running.load(Ordering::Relaxed)
	}
}
