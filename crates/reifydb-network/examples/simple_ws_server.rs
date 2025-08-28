// Simple WebSocket server using mio without tungstenite for connection
// management This version handles the WebSocket frames manually to avoid
// mio/tungstenite conflicts

use std::{
	collections::VecDeque,
	io::{Read, Write},
	net::{SocketAddr, ToSocketAddrs},
	sync::Arc,
	thread,
};

use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use mio::{
	Events, Interest, Token, Waker,
	event::Event,
	net::{TcpListener, TcpStream},
};
use sha1::Sha1;
use slab::Slab;
use socket2::{Domain, Protocol, Socket, Type};

const LISTENER: Token = Token(0);
const WAKE_TOKEN: Token = Token(1);
const TOKEN_BASE: usize = 2;

#[derive(Clone, Debug)]
struct Config {
	bind_addr: String,
	workers: usize,
	reuse_port: bool,
	pin_threads: bool,
	max_outbox_bytes: usize,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8091".to_string(),
			workers: num_cpus::get_physical().max(1),
			reuse_port: true,
			pin_threads: true,
			max_outbox_bytes: 1 << 20,
		}
	}
}

fn main() -> Result<()> {
	let cfg = Config::default();
	eprintln!(
		"Starting simple WS server: addr={} workers={} reuse_port={} pin_threads={}",
		cfg.bind_addr, cfg.workers, cfg.reuse_port, cfg.pin_threads
	);

	let addrs: Vec<SocketAddr> = cfg
		.bind_addr
		.to_socket_addrs()
		.context("invalid bind addr")?
		.collect();
	let addr = *addrs.first().ok_or_else(|| anyhow!("no resolved addr"))?;

	let worker_count = cfg.workers;
	let mut handles = Vec::with_capacity(worker_count);

	for worker_id in 0..worker_count {
		let listener = build_listener(addr, cfg.reuse_port)?;
		listener.set_nonblocking(true)?;
		let cfg_cloned = cfg.clone();

		let handle = thread::Builder::new()
			.name(format!("old_ws-wkr-{worker_id}"))
			.spawn(move || {
				worker_main(
					worker_id,
					worker_count,
					listener,
					cfg_cloned,
				)
			})?;
		handles.push(handle);
	}

	for h in handles {
		let _ = h.join();
	}

	Ok(())
}

fn build_listener(
	addr: SocketAddr,
	reuse_port: bool,
) -> Result<std::net::TcpListener> {
	let domain = if addr.is_ipv4() {
		Domain::IPV4
	} else {
		Domain::IPV6
	};
	let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
	sock.set_reuse_address(true)?;
	if reuse_port {
		let _ = sock.set_reuse_port(true);
	}
	sock.bind(&addr.into())?;
	sock.listen(1024)?;
	Ok(sock.into())
}

fn worker_main(
	worker_id: usize,
	worker_count: usize,
	std_listener: std::net::TcpListener,
	cfg: Config,
) {
	if cfg.pin_threads {
		if let Some(core) = core_affinity::get_core_ids()
			.and_then(|v| v.get(worker_id).cloned())
		{
			core_affinity::set_for_current(core);
			eprintln!(
				"Worker {worker_id}/{worker_count} pinned to core {:?}",
				core.id
			);
		}
	}

	let mut poll = mio::Poll::new().expect("poll");
	let mut events = Events::with_capacity(1024);
	let mut listener = TcpListener::from_std(std_listener);
	poll.registry()
		.register(&mut listener, LISTENER, Interest::READABLE)
		.expect("register listener");

	let waker = Waker::new(poll.registry(), WAKE_TOKEN).expect("waker");
	let _ctrl = Arc::new(waker);

	let mut conns = Slab::<Connection>::new();

	loop {
		if let Err(e) = poll.poll(&mut events, None) {
			if e.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			eprintln!("poll error: {e:?}");
			break;
		}

		for event in events.iter() {
			match event.token() {
				LISTENER => loop {
					match listener.accept() {
                            Ok((stream, peer)) => {
                                if let Err(e) = on_accept(&mut conns, &poll, stream, peer) {
                                    eprintln!("accept error: {e:?}");
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                eprintln!("listener accept fatal: {e:?}");
                                break;
                            }
                        }
				},
				WAKE_TOKEN => {
					// handle control-plane if needed
				}
				token => {
					let key =
						token.0.checked_sub(TOKEN_BASE)
							.unwrap_or(usize::MAX);
					if key == usize::MAX {
						continue;
					}
					if !conns.contains(key) {
						continue;
					}
					if let Err(e) = on_connection_event(
						&mut conns, &poll, key, event,
					) {
						eprintln!(
							"conn {key} event error: {e:?} -> closing"
						);
						close_conn(
							&mut conns, &poll, key,
						);
					}
				}
			}
		}
	}
}

fn on_accept(
	conns: &mut Slab<Connection>,
	poll: &mio::Poll,
	mut stream: TcpStream,
	peer: SocketAddr,
) -> Result<()> {
	stream.set_nodelay(true)?;
	let entry = conns.vacant_entry();
	let key = entry.key();
	let token = Token(TOKEN_BASE + key);

	poll.registry()
		.register(
			&mut stream,
			token,
			Interest::READABLE | Interest::WRITABLE,
		)
		.context("register conn")?;

	let conn = Connection::new(stream, peer, token);
	entry.insert(conn);
	Ok(())
}

fn on_connection_event(
	conns: &mut Slab<Connection>,
	poll: &mio::Poll,
	key: usize,
	event: &Event,
) -> Result<()> {
	let token = Token(TOKEN_BASE + key);
	let (readable, writable) = (event.is_readable(), event.is_writable());
	let conn = conns.get_mut(key).expect("conn exists");

	if readable {
		match conn.readable() {
			Ok(()) => {}
			Err(e) => {
				if e.downcast_ref::<std::io::Error>()
					.map(|ioe| ioe.kind()) != Some(
					std::io::ErrorKind::WouldBlock,
				) {
					return Err(e);
				}
			}
		}
	}

	if writable {
		match conn.writable() {
			Ok(()) => {}
			Err(e) => {
				if e.downcast_ref::<std::io::Error>()
					.map(|ioe| ioe.kind()) != Some(
					std::io::ErrorKind::WouldBlock,
				) {
					return Err(e);
				}
			}
		}
	}

	let interests = conn.interests();
	poll.registry()
		.reregister(&mut conn.stream, token, interests)
		.context("reregister")?;

	Ok(())
}

fn close_conn(conns: &mut Slab<Connection>, poll: &mio::Poll, key: usize) {
	if let Some(mut conn) = conns.try_remove(key) {
		let _ = poll.registry().deregister(&mut conn.stream);
	}
}

// === Connection state machine ===

struct Connection {
	stream: TcpStream,
	peer: SocketAddr,
	token: Token,
	state: ConnState,
	outbox_bytes: usize,
	max_outbox_bytes: usize,
}

enum ConnState {
	Handshake(HandshakeState),
	Active(ActiveState),
	Closed,
}

struct HandshakeState {
	buf: Vec<u8>,
	response: Option<Vec<u8>>,
	written: usize,
}

struct ActiveState {
	sendq: VecDeque<Vec<u8>>, // Raw WebSocket frames to send
}

impl Connection {
	fn new(stream: TcpStream, peer: SocketAddr, token: Token) -> Self {
		Self {
			stream,
			peer,
			token,
			state: ConnState::Handshake(HandshakeState {
				buf: Vec::with_capacity(1024),
				response: None,
				written: 0,
			}),
			outbox_bytes: 0,
			max_outbox_bytes: 1 << 20,
		}
	}

	fn interests(&self) -> Interest {
		match &self.state {
			ConnState::Handshake(hs) => {
				let mut i = Interest::READABLE;
				if hs.response.is_some() {
					i |= Interest::WRITABLE;
				}
				i
			}
			ConnState::Active(ws) => {
				let mut i = Interest::READABLE;
				if !ws.sendq.is_empty() {
					i |= Interest::WRITABLE;
				}
				i
			}
			ConnState::Closed => Interest::READABLE,
		}
	}

	fn readable(&mut self) -> Result<()> {
		match &mut self.state {
			ConnState::Handshake(_) => self.handle_handshake_read(),
			ConnState::Active(_) => self.handle_ws_read(),
			ConnState::Closed => Ok(()),
		}
	}

	fn writable(&mut self) -> Result<()> {
		match &mut self.state {
			ConnState::Handshake(_) => {
				self.handle_handshake_write()
			}
			ConnState::Active(_) => self.handle_ws_write(),
			ConnState::Closed => Ok(()),
		}
	}

	fn handle_handshake_read(&mut self) -> Result<()> {
		let mut buf = [0u8; 2048];
		let mut should_process_handshake = false;

		if let ConnState::Handshake(hs) = &mut self.state {
			loop {
				match self.stream.read(&mut buf) {
                    Ok(0) => return Err(anyhow!("peer closed during handshake")),
                    Ok(n) => {
                        hs.buf.extend_from_slice(&buf[..n]);
                        if find_header_end(&hs.buf).is_some() {
                            should_process_handshake = true;
                            break;
                        }
                        if hs.buf.len() > 16 * 1024 {
                            return Err(anyhow!("handshake header too large"));
                        }
                        if n < buf.len() { break; }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
			}
		}

		if should_process_handshake {
			if let ConnState::Handshake(hs) = &mut self.state {
				if hs.response.is_none() {
					if let Some(hlen) =
						find_header_end(&hs.buf)
					{
						let (resp, _key) =
							build_ws_response(
								&hs.buf[..hlen],
							)?;
						hs.response = Some(resp);
					}
				}
			}
		}
		Ok(())
	}

	fn handle_handshake_write(&mut self) -> Result<()> {
		let mut should_upgrade = false;

		if let ConnState::Handshake(hs) = &mut self.state {
			if let Some(resp) = &hs.response {
				while hs.written < resp.len() {
					match self.stream.write(&resp[hs.written..]) {
                        Ok(0) => return Err(anyhow!("peer closed while writing 101")),
                        Ok(n) => hs.written += n,
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                        Err(e) => return Err(e.into()),
                    }
				}
				should_upgrade = true;
			}
		}

		if should_upgrade {
			self.state = ConnState::Active(ActiveState {
				sendq: VecDeque::new(),
			});
			eprintln!("{} -> WS established", self.peer);
		}
		Ok(())
	}

	fn handle_ws_read(&mut self) -> Result<()> {
		let mut buf = [0u8; 1024];

		loop {
			match self.stream.read(&mut buf) {
                Ok(0) => {
                    self.state = ConnState::Closed;
                    return Ok(());
                }
                Ok(n) => {
                    // Parse WebSocket frame with masking support
                    if n >= 6 { // Minimum for masked frame: 2 bytes header + 4 bytes mask
                        let frame_type = buf[0] & 0x0F;
                        if frame_type == 1 { // Text frame
                            let masked = (buf[1] & 0x80) != 0;
                            let payload_len = (buf[1] & 0x7F) as usize;

                            if payload_len < 126 && masked {
                                let mask_start = 2;
                                let payload_start = mask_start + 4;

                                if n >= payload_start + payload_len {
                                    // Extract mask and payload
                                    let mask = &buf[mask_start..mask_start + 4];
                                    let masked_payload = &buf[payload_start..payload_start + payload_len];

                                    // Unmask payload
                                    let mut unmasked_payload = Vec::with_capacity(payload_len);
                                    for (i, &byte) in masked_payload.iter().enumerate() {
                                        unmasked_payload.push(byte ^ mask[i % 4]);
                                    }

                                    if let Ok(text) = std::str::from_utf8(&unmasked_payload) {
                                        eprintln!("Received: {}", text);
                                        let resp = handle_request(text);
                                        let response_json = serde_json::to_string(&resp)?;
                                        self.enqueue_text_frame(&response_json)?;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.state = ConnState::Closed;
                    return Ok(());
                }
            }
		}
		Ok(())
	}

	fn handle_ws_write(&mut self) -> Result<()> {
		if let ConnState::Active(ws) = &mut self.state {
			while !ws.sendq.is_empty() {
				let frame = ws.sendq.front().unwrap().clone();
				match self.stream.write(&frame) {
                    Ok(n) if n == frame.len() => {
                        ws.sendq.pop_front();
                        self.outbox_bytes = self.outbox_bytes.saturating_sub(frame.len());
                    }
                    Ok(_) => {
                        // Partial write - not handled in this simple example
                        break;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        self.state = ConnState::Closed;
                        return Ok(());
                    }
                }
			}
		}
		Ok(())
	}

	fn enqueue_text_frame(&mut self, text: &str) -> Result<()> {
		let payload = text.as_bytes();
		let payload_len = payload.len();

		if payload_len > 125 {
			return Err(anyhow!(
				"Message too long for simple implementation"
			));
		}

		if self.outbox_bytes + payload_len + 2 > self.max_outbox_bytes {
			self.state = ConnState::Closed;
			return Err(anyhow!("backpressure: outbox overflow"));
		}

		// Build simple WebSocket text frame
		let mut frame = Vec::with_capacity(payload_len + 2);
		frame.push(0x81); // FIN=1, opcode=1 (text)
		frame.push(payload_len as u8); // payload length (< 126)
		frame.extend_from_slice(payload);

		if let ConnState::Active(ws) = &mut self.state {
			ws.sendq.push_back(frame);
			self.outbox_bytes += payload_len + 2;
		}
		Ok(())
	}
}

// === HTTP -> WS handshake helpers ===

fn find_header_end(buf: &[u8]) -> Option<usize> {
	let pat = b"\r\n\r\n";
	buf.windows(4).position(|w| w == pat).map(|i| i + 4)
}

fn build_ws_response(req_bytes: &[u8]) -> Result<(Vec<u8>, String)> {
	let mut headers = [httparse::EMPTY_HEADER; 32];
	let mut req = httparse::Request::new(&mut headers);
	let status =
		req.parse(req_bytes).map_err(|e| anyhow!("httparse: {e}"))?;
	if status.is_partial() {
		return Err(anyhow!("partial HTTP request"));
	}

	if req.method != Some("GET") || req.version != Some(1) {
		return Err(anyhow!("invalid HTTP method/version"));
	}

	let mut key: Option<&[u8]> = None;
	let mut upgrade_ok = false;
	let mut conn_upgrade = false;
	let mut version13 = false;

	for h in req.headers.iter() {
		match h.name.to_ascii_lowercase().as_str() {
			"sec-websocket-key" => key = Some(h.value),
			"upgrade" => {
				if eq_case_insensitive(h.value, b"websocket") {
					upgrade_ok = true;
				}
			}
			"connection" => {
				if bytes_contains_ci(h.value, b"upgrade") {
					conn_upgrade = true;
				}
			}
			"sec-websocket-version" => {
				if eq_case_insensitive(h.value, b"13") {
					version13 = true;
				}
			}
			_ => {}
		}
	}

	if !(upgrade_ok && conn_upgrade && version13) {
		return Err(anyhow!("missing/invalid WS upgrade headers"));
	}
	let key = key.ok_or_else(|| anyhow!("missing Sec-WebSocket-Key"))?;

	let accept = compute_accept_key(key);
	let resp = format!(
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
		accept
	);
	Ok((resp.into_bytes(), accept))
}

fn compute_accept_key(sec_websocket_key: &[u8]) -> String {
	use sha1::digest::Digest;
	const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	let mut sha1 = Sha1::new();
	sha1.update(sec_websocket_key);
	sha1.update(GUID.as_bytes());
	let result = sha1.finalize();
	BASE64.encode(result)
}

fn eq_case_insensitive(a: &[u8], b: &[u8]) -> bool {
	a.eq_ignore_ascii_case(b)
}

fn bytes_contains_ci(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle))
}

// === Application Protocol ===

#[derive(serde::Serialize, serde::Deserialize)]
struct RequestMsg {
	#[serde(default)]
	q: String,
}

#[derive(serde::Serialize)]
struct ResponseMsg {
	ok: bool,
	result: String,
}

fn handle_request(txt: &str) -> ResponseMsg {
	let parsed: Result<RequestMsg> =
		serde_json::from_str(txt).map_err(Into::into);
	match parsed {
		Ok(req) => {
			println!("req {}", req.q);
			let result = execute_query(&req.q);

			ResponseMsg {
				ok: true,
				result,
			}
		}
		Err(_) => ResponseMsg {
			ok: false,
			result: "bad request".into(),
		},
	}
}

fn execute_query(q: &str) -> String {
	let mut h: u64 = 0xcbf29ce484222325;
	for b in q.as_bytes() {
		h ^= *b as u64;
		h = h.wrapping_mul(0x100000001b3);
	}
	format!("hash={h:016x}")
}
