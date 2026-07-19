// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Reconnection behaviour: a dropped socket is transparently re-established (with re-auth and
//! resubscribe) against the same handles, in-flight requests are rejected while disconnected,
//! backoff is exponential, and an exhausted or explicitly-disconnected client stops for good.
//!
//! A `TcpProxy` sits between the client and the server so a disconnect can be injected while the
//! client keeps dialing the same (proxy) URL - the server's ephemeral port never changes.

use std::{future::Future, sync::Arc};

use reifydb_client::{
	BatchItem, BatchPushEvent, ChangeKind, HydrationConfig, ReconnectOptions, SubscriptionConfig, WireFormat,
	WsClientOptions,
};
use reifydb_value::value::duration::Duration;
use tokio::{runtime::Runtime, sync::mpsc, time::timeout};

use super::{create_test_table, recv_with_timeout, unique_table_name};
use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	proxy::TcpProxy,
};

fn with_ws_server<F, Fut>(test: F)
where
	F: FnOnce(u16) -> Fut,
	Fut: Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
	runtime.block_on(test(port));
	cleanup_server(Some(server));
}

/// Hydration disabled so a resubscribe never replays prior rows - only genuinely new inserts
/// produce changes, keeping the post-reconnect assertions unambiguous.
fn no_hydration() -> SubscriptionConfig {
	SubscriptionConfig {
		hydration: HydrationConfig {
			enabled: false,
			max_rows: None,
		},
		throttle: None,
		linger: None,
	}
}

fn options(reconnect: ReconnectOptions) -> WsClientOptions {
	WsClientOptions {
		format: WireFormat::Frames,
		reconnect,
	}
}

fn signal_hook(tx: mpsc::UnboundedSender<()>) -> Arc<dyn Fn() + Send + Sync> {
	Arc::new(move || {
		let _ = tx.send(());
	})
}

async fn fired_within(rx: &mut mpsc::UnboundedReceiver<()>, timeout_ms: u64) -> bool {
	let dur = Duration::from_milliseconds(timeout_ms as i64).unwrap().to_std();
	matches!(timeout(dur, rx.recv()).await, Ok(Some(())))
}

#[test]
fn reconnect_transparently_resubscribes_standalone() {
	with_ws_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 20,
			on_reconnect: Some(signal_hook(recon_tx)),
			..Default::default()
		};

		let mut client = reifydb_client::WsClient::connect_with_options(&proxy.ws_url(), options(reconnect))
			.await
			.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("reconn_std");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let sub_id = client.subscribe(&format!("from test::{}", table), no_hydration()).await.unwrap();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		let first = recv_with_timeout(&mut client, 5000).await.expect("initial change");
		assert_eq!(first.subscription_id, sub_id);

		proxy.kill();
		assert!(fired_within(&mut recon_rx, 5000).await, "client should reconnect after the socket drops");

		client.command(&format!("INSERT test::{} [{{ id: 2 }}]", table), None).await.unwrap();
		let second = recv_with_timeout(&mut client, 5000).await.expect("change after reconnect");
		assert_eq!(second.subscription_id, sub_id, "the stable subscription id survives the reconnect");
		assert_eq!(second.changes[0].kind, ChangeKind::Insert);

		client.close().await.unwrap();
	});
}

#[test]
fn reconnect_transparently_resubscribes_batch() {
	with_ws_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 20,
			on_reconnect: Some(signal_hook(recon_tx)),
			..Default::default()
		};

		let mut client = reifydb_client::WsClient::connect_with_options(&proxy.ws_url(), options(reconnect))
			.await
			.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("reconn_batch");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut batch = client.batch_subscribe(&[BatchItem::new(&query, no_hydration())]).await.unwrap();
		let batch_id = batch.batch_id().to_string();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		let first = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), batch.recv())
			.await
			.expect("no batch event before timeout")
			.expect("batch stream should not close");
		assert!(matches!(first, BatchPushEvent::Change(_)), "expected a batch change");

		proxy.kill();
		assert!(fired_within(&mut recon_rx, 5000).await, "batch client should reconnect");

		client.command(&format!("INSERT test::{} [{{ id: 2 }}]", table), None).await.unwrap();
		let second = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), batch.recv())
			.await
			.expect("no batch event after reconnect before timeout")
			.expect("batch stream should keep yielding after reconnect");
		match second {
			BatchPushEvent::Change(payload) => {
				assert_eq!(payload.batch_id, batch_id, "the stable batch id survives the reconnect");
			}
			other => panic!("expected a batch change after reconnect, got {:?}", other),
		}

		client.close().await.unwrap();
	});
}

#[test]
fn pending_request_rejected_with_connection_lost() {
	with_ws_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 50,
			max_reconnect_attempts: 5,
			..Default::default()
		};

		let mut client = reifydb_client::WsClient::connect_with_options(&proxy.ws_url(), options(reconnect))
			.await
			.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("reconn_reject");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		// Pause so reconnection cannot immediately succeed, then drop the live connection.
		proxy.pause();
		proxy.kill();

		let result = client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await;
		let err = result.expect_err("a request issued across a dropped connection must fail");
		assert_eq!(err.0.code, "CONNECTION_LOST");

		client.close().await.unwrap();
	});
}

#[test]
fn backoff_is_exponential_then_attempts_are_exhausted() {
	with_ws_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let (discon_tx, mut discon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 3,
			on_reconnect: Some(signal_hook(recon_tx)),
			on_disconnect: Some(signal_hook(discon_tx)),
			..Default::default()
		};

		let mut client = reifydb_client::WsClient::connect_with_options(&proxy.ws_url(), options(reconnect))
			.await
			.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("reconn_exhaust");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		client.subscribe(&format!("from test::{}", table), no_hydration()).await.unwrap();

		// Refuse every reconnect so the client exhausts its attempts.
		proxy.pause();
		let start = tokio::time::Instant::now();
		proxy.kill();

		assert!(fired_within(&mut discon_rx, 5000).await, "on_disconnect fires when the socket drops");

		// Once attempts are exhausted the connection task ends and recv() reports end-of-stream.
		let ended = client.recv().await;
		let elapsed = start.elapsed();
		assert!(ended.is_none(), "recv() ends after reconnection is abandoned");

		// Three attempts back off 20 + 40 + 80 ms; linear retries would total only ~60ms.
		let expected = Duration::from_milliseconds(120).unwrap().to_std();
		assert!(elapsed >= expected, "expected exponential backoff (>=120ms), took {:?}", elapsed);
		assert!(!fired_within(&mut recon_rx, 200).await, "on_reconnect must not fire while paused");

		client.close().await.unwrap();
	});
}

#[test]
fn explicit_disconnect_disables_reconnect() {
	with_ws_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 5,
			on_reconnect: Some(signal_hook(recon_tx)),
			..Default::default()
		};

		let mut client = reifydb_client::WsClient::connect_with_options(&proxy.ws_url(), options(reconnect))
			.await
			.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("reconn_disc");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		client.subscribe(&format!("from test::{}", table), no_hydration()).await.unwrap();

		client.disconnect().await;

		let ended = client.recv().await;
		assert!(ended.is_none(), "recv() ends after an explicit disconnect");
		assert!(
			!fired_within(&mut recon_rx, 200).await,
			"an explicitly disconnected client must not reconnect"
		);
	});
}
