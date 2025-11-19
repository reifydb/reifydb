// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem, core::interface::logging::LogLevel::Debug, embedded,
	sub_logging::LoggingBuilder,
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Debug)
}

fn main() {
	let mut db = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_worker(|wp| wp)
		.with_flow(|f| {
			f.operators_dir(
				PathBuf::from_str("/home/ddymke/Workspace/red/testsuite/fixture/target/debug").unwrap(),
			)
		})
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace
	println!("Creating namespace...");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();

	// Create tables - messages with multiple lookups to same table
	println!("Creating tables...");
	db.command_as_root(
		r#"create table test.messages { id: int4, sender_id: int4, receiver_id: int4, channel_id: int4, content: utf8 }"#,
		Params::None,
	)
	.unwrap();
	db.command_as_root(r#"create table test.users { id: int4, username: utf8 }"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.channels { id: int4, channel_name: utf8 }"#, Params::None).unwrap();

	// Insert all data BEFORE creating the view
	println!("Inserting users...");
	db.command_as_root(
		r#"
from [
    {id: 1, username: "alice"},
    {id: 2, username: "bob"},
    {id: 3, username: "charlie"}
] insert test.users
"#,
		Params::None,
	)
	.unwrap();

	println!("Inserting channels...");
	db.command_as_root(
		r#"
from [
    {id: 10, channel_name: "general"},
    {id: 20, channel_name: "random"},
    {id: 30, channel_name: "announcements"}
] insert test.channels
"#,
		Params::None,
	)
	.unwrap();

	println!("Inserting messages...");
	db.command_as_root(
		r#"
from [
    {id: 1, sender_id: 1, receiver_id: 2, channel_id: 10, content: "Hello Bob"},
    {id: 2, sender_id: 2, receiver_id: 1, channel_id: 10, content: "Hi Alice"},
    {id: 3, sender_id: 3, receiver_id: 1, channel_id: 20, content: "Hey there"},
    {id: 4, sender_id: 1, receiver_id: 3, channel_id: 30, content: "Announcement"},
    {id: 5, sender_id: 4, receiver_id: 2, channel_id: 10, content: "Unknown user"}
] insert test.messages
"#,
		Params::None,
	)
	.unwrap();

	// Now create the view with multiple lookups (two to same table)
	println!("\nCreating deferred view with multiple lookups to same table...");
	db.command_as_root(
		r#"
create deferred view test.message_log {
    msg_id: int4,
    sender: utf8,
    receiver: utf8,
    channel: utf8,
    content: utf8
} as {
    from test.messages
    left join { from test.users } sender on sender_id == sender.id
    left join { from test.users } receiver on receiver_id == receiver.id
    left join { from test.channels } channels on channel_id == channels.id
    map {
        msg_id: id,
        sender: username,
        receiver: receiver_username,
        channel: channel_name,
        content: content
    }
}
"#,
		Params::None,
	)
	.unwrap();

	println!("Created deferred view");

	// Wait for view to process data
	sleep(Duration::from_millis(500));

	// Query the deferred view - should have complete info from all lookups
	println!("\n=== Deferred view query (should show all 5 messages with joined data) ===");
	for frame in db.query_as_root(r#"from test.message_log sort msg_id asc"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Expected: msg_id, sender, receiver, channel, content for all 5 messages ===");
	println!("Note: Message 5 should have Undefined sender (user 4 doesn't exist)");
}
