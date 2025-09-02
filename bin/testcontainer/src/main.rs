// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::atomic::{AtomicBool, Ordering},
	time::Duration,
};

use reifydb::{
	FormatStyle, LoggingBuilder, WithSubsystem,
	core::interface::subsystem::logging::LogLevel, server,
	sub_server::ServerConfig,
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| {
		console.color(true)
			.stderr_for_errors(true)
			.format_style(FormatStyle::Timeline)
	})
	.buffer_capacity(20000)
	.batch_size(2000)
	.flush_interval(Duration::from_millis(50))
	.immediate_on_error(true)
	.level(LogLevel::Trace)
}

fn main() {
	// Set up signal handling
	static RUNNING: AtomicBool = AtomicBool::new(true);

	extern "C" fn handle_signal(sig: libc::c_int) {
		let signal_name = match sig {
			libc::SIGINT => "SIGINT (Ctrl+C)",
			libc::SIGTERM => "SIGTERM",
			libc::SIGQUIT => "SIGQUIT",
			libc::SIGHUP => "SIGHUP",
			_ => "Unknown signal",
		};
		println!(
			"\nReceived {}, shutting down gracefully...",
			signal_name
		);
		RUNNING.store(false, Ordering::SeqCst);
	}

	unsafe {
		// Handle common termination signals
		libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t); // Ctrl+C
		libc::signal(
			libc::SIGTERM,
			handle_signal as libc::sighandler_t,
		); // Termination request
		libc::signal(
			libc::SIGQUIT,
			handle_signal as libc::sighandler_t,
		); // Quit signal
		libc::signal(libc::SIGHUP, handle_signal as libc::sighandler_t); // Hangup signal
	}

	let mut db = server::memory_optimistic()
		.with_config(ServerConfig::default())
		.with_logging(logger_configuration)
		.build()
		.unwrap();

	// Start the database
	db.start().unwrap();
	println!("Database started successfully!");
	println!("Press Ctrl+C to stop...");

	// Run until interrupted
	while RUNNING.load(Ordering::SeqCst) {
		std::thread::sleep(Duration::from_millis(100));
	}

	// Stop the database
	println!("Shutting down database...");
	db.stop().unwrap();
	println!("Database stopped successfully!");
}
