// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! gRPC reconnection: each subscription is its own server-streaming RPC that self-heals when
//! its stream drops - re-dialing the same (proxy) URL, re-authenticating and resubscribing
//! under the same stable client id - while a request over a dead, unrecoverable connection
//! surfaces `CONNECTION_LOST`.
//!
//! Reconnection is driven by `recv()`: the replayed rows a hydrated resubscribe produces are
//! what a post-drop `recv()` returns, so the assertions lean on hydration rather than racing an
//! insert against the reconnect window.

use std::{future::Future, sync::Arc};

use reifydb_client::{
	BatchItem, BatchStreamEvent, GrpcClient, GrpcClientOptions, ReconnectOptions, SubscriptionConfig, WireFormat,
};
use reifydb_value::value::duration::Duration;
use tokio::{runtime::Runtime, sync::mpsc, time::timeout};

use super::{create_test_table, recv_with_timeout, unique_table_name};
use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	proxy::TcpProxy,
};

fn with_grpc_server<F, Fut>(test: F)
where
	F: FnOnce(u16) -> Fut,
	Fut: Future<Output = ()>,
{
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();
	runtime.block_on(test(port));
	cleanup_server(Some(server));
}

fn options(reconnect: ReconnectOptions) -> GrpcClientOptions {
	GrpcClientOptions {
		format: WireFormat::Rbcf,
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
	with_grpc_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 20,
			on_reconnect: Some(signal_hook(recon_tx)),
			..Default::default()
		};

		let mut client =
			GrpcClient::connect_with_options(&format!("http://{}", proxy.addr()), options(reconnect))
				.await
				.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("reconn_grpc_std");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let mut sub = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();
		let sub_id = sub.subscription_id().to_string();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		recv_with_timeout(&mut sub, 5000).await.expect("initial change");

		proxy.kill();
		// recv() drives the self-heal; a hydrated resubscribe replays the existing row.
		let after = recv_with_timeout(&mut sub, 5000).await.expect("change after reconnect");
		assert!(!after.changes.is_empty(), "the resubscribe should replay the existing row");
		assert_eq!(sub.subscription_id(), sub_id, "the stable subscription id survives the reconnect");
		assert!(fired_within(&mut recon_rx, 1000).await, "on_reconnect should fire");
	});
}

#[test]
fn reconnect_transparently_resubscribes_batch() {
	with_grpc_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let (recon_tx, mut recon_rx) = mpsc::unbounded_channel();
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 20,
			max_reconnect_attempts: 20,
			on_reconnect: Some(signal_hook(recon_tx)),
			..Default::default()
		};

		let mut client =
			GrpcClient::connect_with_options(&format!("http://{}", proxy.addr()), options(reconnect))
				.await
				.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("reconn_grpc_batch");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let query = format!("from test::{}", table);
		let mut batch =
			client.batch_subscribe(&[BatchItem::new(&query, SubscriptionConfig::default())]).await.unwrap();
		let batch_id = batch.batch_id().to_string();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		let first = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), batch.recv())
			.await
			.expect("no batch event before timeout")
			.expect("batch stream should not close");
		assert!(matches!(first, BatchStreamEvent::Change(_)), "expected a batch change");

		proxy.kill();
		let second = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), batch.recv())
			.await
			.expect("no batch event after reconnect before timeout")
			.expect("batch stream should keep yielding after reconnect");
		match second {
			BatchStreamEvent::Change(env) => {
				assert_eq!(env.batch_id, batch_id, "the stable batch id survives the reconnect");
			}
			other => panic!("expected a batch change after reconnect, got {:?}", other),
		}
		assert_eq!(batch.batch_id(), batch_id);
		assert!(fired_within(&mut recon_rx, 1000).await, "on_reconnect should fire");
	});
}

#[test]
fn request_fails_with_connection_lost_after_disconnect() {
	with_grpc_server(|port| async move {
		let proxy = TcpProxy::start(port).await;
		let reconnect = ReconnectOptions {
			reconnect_delay_ms: 50,
			max_reconnect_attempts: 3,
			..Default::default()
		};

		let mut client =
			GrpcClient::connect_with_options(&format!("http://{}", proxy.addr()), options(reconnect))
				.await
				.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("reconn_grpc_reject");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		// Refuse reconnection, then drop the live connection.
		proxy.pause();
		proxy.kill();

		let result = client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await;
		let err = result.expect_err("a request over an unrecoverable connection must fail");
		assert_eq!(err.0.code, "CONNECTION_LOST");
	});
}

#[test]
fn backoff_is_exponential_then_attempts_are_exhausted() {
	with_grpc_server(|port| async move {
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

		let mut client =
			GrpcClient::connect_with_options(&format!("http://{}", proxy.addr()), options(reconnect))
				.await
				.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("reconn_grpc_exhaust");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let mut sub = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		recv_with_timeout(&mut sub, 5000).await.expect("initial change");

		proxy.pause();
		let start = tokio::time::Instant::now();
		proxy.kill();

		// recv() drives the reconnect attempts; all fail while paused, so the stream ends.
		let ended = sub.recv().await;
		let elapsed = start.elapsed();
		assert!(ended.is_none(), "recv() ends once reconnection is abandoned");
		assert!(fired_within(&mut discon_rx, 1000).await, "on_disconnect fires when the stream drops");

		let expected = Duration::from_milliseconds(120).unwrap().to_std();
		assert!(elapsed >= expected, "expected exponential backoff (>=120ms), took {:?}", elapsed);
		assert!(!fired_within(&mut recon_rx, 200).await, "on_reconnect must not fire while paused");
	});
}
