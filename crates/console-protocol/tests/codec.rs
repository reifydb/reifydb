// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::io::ErrorKind;

use reifydb_console_protocol::{
	MAX_FRAME, PROTOCOL_VERSION, ProtocolError, Refusal, Register, Reply, read_message, write_message,
};
use tokio::io::{AsyncWriteExt, duplex};

fn register(fingerprint: Option<&str>) -> Register {
	Register {
		protocol_version: PROTOCOL_VERSION,
		token: "token".to_string(),
		fingerprint: fingerprint.map(str::to_string),
		version: "0.9.3".to_string(),
	}
}

fn every_refusal() -> Vec<Refusal> {
	vec![
		Refusal::UnknownToken,
		Refusal::UnsupportedVersion {
			supported: PROTOCOL_VERSION,
		},
		Refusal::FingerprintInUse,
		Refusal::Unavailable,
	]
}

#[tokio::test]
async fn every_message_round_trips() {
	let (mut instance, mut server) = duplex(MAX_FRAME + 16);

	for sent in [register(None), register(Some("fp-1"))] {
		write_message(&mut instance, &sent).await.unwrap();
		let received: Register = read_message(&mut server).await.unwrap();
		assert_eq!(received, sent);
	}

	let mut replies = vec![Reply::Registered {
		fingerprint: "fp-1".to_string(),
		protocol_version: PROTOCOL_VERSION,
	}];
	replies.extend(every_refusal().into_iter().map(|reason| Reply::Refused {
		reason,
	}));
	for sent in replies {
		write_message(&mut server, &sent).await.unwrap();
		let received: Reply = read_message(&mut instance).await.unwrap();
		assert_eq!(received, sent);
	}
}

#[test]
fn the_wire_shape_is_pinned() {
	// Both ends ship on their own schedule, so the JSON shape must never drift silently.
	let json = serde_json::to_string(&Reply::Refused {
		reason: Refusal::UnknownToken,
	})
	.unwrap();
	assert_eq!(json, r#"{"type":"Refused","reason":{"kind":"UnknownToken"}}"#);
}

#[tokio::test]
async fn an_oversized_length_is_refused_before_the_body() {
	// The length must be checked before the body, otherwise this surfaces as Io, not TooLarge.
	let (mut writer, mut reader) = duplex(MAX_FRAME + 16);
	writer.write_u32((MAX_FRAME + 1) as u32).await.unwrap();
	drop(writer);

	let err = read_message::<_, Register>(&mut reader).await.unwrap_err();
	assert!(matches!(err, ProtocolError::TooLarge(len) if len == MAX_FRAME + 1), "got {err:?}");
}

#[tokio::test]
async fn a_frame_of_exactly_max_frame_is_accepted() {
	// A body of exactly MAX_FRAME bytes must pass, otherwise the cap is off by one.
	let (mut writer, mut reader) = duplex(MAX_FRAME + 16);
	let overhead = serde_json::to_vec(&"").unwrap().len();
	let value = "x".repeat(MAX_FRAME - overhead);
	write_message(&mut writer, &value).await.unwrap();

	let received: String = read_message(&mut reader).await.unwrap();
	assert_eq!(received, value);
}

#[tokio::test]
async fn an_oversized_message_is_never_written() {
	// An oversized frame must never reach the wire, otherwise the peer reads a prefix it must refuse.
	let (mut writer, mut reader) = duplex(MAX_FRAME + 16);
	let value = "x".repeat(MAX_FRAME);

	let err = write_message(&mut writer, &value).await.unwrap_err();
	assert!(matches!(err, ProtocolError::TooLarge(len) if len == MAX_FRAME + 2), "got {err:?}");

	drop(writer);
	let err = read_message::<_, String>(&mut reader).await.unwrap_err();
	assert!(matches!(&err, ProtocolError::Io(e) if e.kind() == ErrorKind::UnexpectedEof), "got {err:?}");
}

#[tokio::test]
async fn a_truncated_body_is_an_io_error() {
	let (mut writer, mut reader) = duplex(MAX_FRAME + 16);
	writer.write_u32(10).await.unwrap();
	writer.write_all(b"abc").await.unwrap();
	drop(writer);

	let err = read_message::<_, Register>(&mut reader).await.unwrap_err();
	assert!(matches!(&err, ProtocolError::Io(e) if e.kind() == ErrorKind::UnexpectedEof), "got {err:?}");
}

#[tokio::test]
async fn garbage_is_malformed() {
	let (mut writer, mut reader) = duplex(MAX_FRAME + 16);
	writer.write_u32(3).await.unwrap();
	writer.write_all(b"abc").await.unwrap();

	let err = read_message::<_, Register>(&mut reader).await.unwrap_err();
	assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

#[tokio::test]
async fn an_unknown_protocol_version_is_refused_cleanly() {
	// A newer instance must still decode, otherwise the server can only drop it without a reason.
	let (mut instance, mut server) = duplex(MAX_FRAME + 16);
	let future = br#"{"protocol_version":2,"token":"token","version":"9.9.9","capabilities":["zstd"]}"#;
	instance.write_u32(future.len() as u32).await.unwrap();
	instance.write_all(future).await.unwrap();

	let received: Register = read_message(&mut server).await.unwrap();
	assert_eq!(received.protocol_version, 2);
	assert_eq!(received.fingerprint, None);

	let refusal = Reply::Refused {
		reason: Refusal::UnsupportedVersion {
			supported: PROTOCOL_VERSION,
		},
	};
	write_message(&mut server, &refusal).await.unwrap();
	let reply: Reply = read_message(&mut instance).await.unwrap();
	assert_eq!(reply, refusal);
}

#[test]
fn only_unavailable_is_retried() {
	for refusal in every_refusal() {
		assert_eq!(refusal.is_final(), refusal != Refusal::Unavailable, "{refusal:?}");
	}
}
