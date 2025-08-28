// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::net::{SocketAddr, TcpListener};

use socket2::{Domain, Protocol, Socket, Type};

pub fn create_listener(
	addr: SocketAddr,
	reuse_port: bool,
) -> Result<TcpListener, Box<dyn std::error::Error>> {
	let domain = match addr {
		SocketAddr::V4(_) => Domain::IPV4,
		SocketAddr::V6(_) => Domain::IPV6,
	};

	let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

	if reuse_port {
		socket.set_reuse_port(true)?;
	}

	socket.set_reuse_address(true)?;
	socket.set_nonblocking(true)?;
	socket.bind(&addr.into())?;
	socket.listen(1024)?;

	Ok(socket.into())
}

pub fn configure_tcp_stream(
	stream: &mut mio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
	stream.set_nodelay(true)?;
	Ok(())
}
