// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A background reader reconnects each call's stream, so the assertions lean on the rows a hydrated
//! resubscribe replays rather than racing an insert against the reconnect window.

use std::{future::Future, sync::Arc};

use reifydb_client::{
	GrpcClient, GrpcClientOptions, ReconnectOptions, SubscribeItem, SubscriptionConfig, Value, WireFormat,
};
use reifydb_value::value::duration::Duration;
use tokio::{
	runtime::Runtime,
	sync::mpsc,
	time::{sleep, timeout},
};

use super::{create_test_table, find_column, recv_with_timeout, unique_table_name};
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
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);
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
fn reconnect_resubscribes_every_item_of_one_call() {
	// Catches a reconnect that resubscribes only the first item of a call or hands its handles new ids.
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

		let table_a = unique_table_name("reconn_grpc_call_a");
		let table_b = unique_table_name("reconn_grpc_call_b");
		create_test_table(&client, &table_a, &[("id", "int4")]).await.unwrap();
		create_test_table(&client, &table_b, &[("id", "int4")]).await.unwrap();
		let query_a = format!("from test::{}", table_a);
		let query_b = format!("from test::{}", table_b);
		let mut handles = client
			.subscribe(&[
				SubscribeItem::new(&query_a, SubscriptionConfig::default()),
				SubscribeItem::new(&query_b, SubscriptionConfig::default()),
			])
			.await
			.unwrap();
		let id_a = handles[0].subscription_id().to_string();
		let id_b = handles[1].subscription_id().to_string();

		client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table_a), None).await.unwrap();
		client.command(&format!("INSERT test::{} [{{ id: 2 }}]", table_b), None).await.unwrap();
		recv_with_timeout(&mut handles[0], 5000).await.expect("initial change on handle 0");
		recv_with_timeout(&mut handles[1], 5000).await.expect("initial change on handle 1");

		proxy.kill();
		let after_a = recv_with_timeout(&mut handles[0], 5000).await.expect("handle 0 change after reconnect");
		let after_b = recv_with_timeout(&mut handles[1], 5000).await.expect("handle 1 change after reconnect");
		assert!(!after_a.changes.is_empty(), "the resubscribe should replay table A's row");
		assert!(!after_b.changes.is_empty(), "the resubscribe should replay table B's row");
		assert_eq!(
			find_column(&after_a.changes[0].frame, "id").unwrap().get_value(0),
			Value::Int4(1),
			"handle 0 must replay its own table's row"
		);
		assert_eq!(
			find_column(&after_b.changes[0].frame, "id").unwrap().get_value(0),
			Value::Int4(2),
			"handle 1 must replay its own table's row"
		);
		assert_eq!(handles[0].subscription_id(), id_a, "handle 0 keeps its id across the reconnect");
		assert_eq!(handles[1].subscription_id(), id_b, "handle 1 keeps its id across the reconnect");
		assert!(fired_within(&mut recon_rx, 1000).await, "on_reconnect should fire");
	});
}

async fn server_subscriptions(client: &GrpcClient) -> usize {
	client.query("FROM system::subscriptions", None)
		.await
		.unwrap()
		.first()
		.map_or(0, |frame| frame.batch.num_rows())
}

async fn settles_at(client: &GrpcClient, expected: usize) -> bool {
	for _ in 0..250 {
		if server_subscriptions(client).await == expected {
			return true;
		}
		sleep(Duration::from_milliseconds(20).unwrap().to_std()).await;
	}
	false
}

#[test]
fn unsubscribe_after_a_reconnect_ends_the_live_server_subscription() {
	with_grpc_server(|port| async move {
		// Sending the stale first server id after a reconnect hits nothing, and the live stream keeps running.
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
		let table = unique_table_name("reconn_grpc_unsub");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
		let baseline = server_subscriptions(&client).await;
		let sub = client
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

		proxy.kill();
		assert!(fired_within(&mut recon_rx, 5000).await, "the stream must reconnect");
		assert!(settles_at(&client, baseline + 1).await, "exactly the resubscribed stream must be live");

		client.unsubscribe(&[sub.subscription_id()]).await.unwrap();

		assert!(
			settles_at(&client, baseline).await,
			"the unsubscribe must end the subscription that is live now"
		);
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
			.subscribe(&[SubscribeItem::new(
				&format!("from test::{}", table),
				SubscriptionConfig::default(),
			)])
			.await
			.unwrap()
			.remove(0);

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
