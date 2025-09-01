// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
// use std::{
// 	sync::{Arc, Once},
// 	time::Duration,
// };
//
// use criterion::{
// 	black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
// 	Throughput,
// };
// use futures_util::{SinkExt, StreamExt};
// use reifydb::{
// 	core::interface::subsystem::logging::LogLevel,
// 	fix_me_server::ServerConfig, memory, optimistic, Database,
// 	LoggingBuilder, MemoryOptimisticTransaction, ServerBuilder,
// 	WithSubsystem,
// };
// use reifydb_bench::queries;
// use tokio::{net::TcpStream, runtime::Runtime, time::sleep};
// use tokio_tungstenite::{
// 	connect_async,
// 	tungstenite::{Message, Utf8Bytes},
// 	MaybeTlsStream, WebSocketStream,
// };
//
// // Global server instance that gets started once
// static mut GLOBAL_DB: Option<Arc<Database<MemoryOptimisticTransaction>>> =
// None; static INIT: Once = Once::new();
//
// #[allow(static_mut_refs)]
// fn get_or_start_server(
// 	rt: &Runtime,
// ) -> Arc<Database<MemoryOptimisticTransaction>> {
// 	unsafe {
// 		INIT.call_once(|| {
// 			let db = rt.block_on(async {
// 				fn logger_configuration(
// 					logging: LoggingBuilder,
// 				) -> LoggingBuilder {
// 					logging.level(LogLevel::Off)
// 				}
//
// 				let (storage, unversioned, cdc, hooks) =
// 					memory();
// 				let (versioned, _, _, _) = optimistic((
// 					storage.clone(),
// 					unversioned.clone(),
// 					cdc.clone(),
// 					hooks.clone(),
// 				));
//
// 				let mut db: Database<
// 					MemoryOptimisticTransaction,
// 				> = ServerBuilder::new(
// 					versioned.clone(),
// 					unversioned.clone(),
// 					cdc.clone(),
// 					hooks.clone(),
// 				)
// 				.with_config(ServerConfig::default())
// 				.with_logging(logger_configuration)
// 				.build()
// 				.unwrap();
//
// 				db.start().unwrap();
//
// 				sleep(Duration::from_millis(100)).await;
// 				db
// 			});
//
// 			GLOBAL_DB = Some(Arc::new(db));
// 		});
//
// 		GLOBAL_DB.clone().unwrap()
// 	}
// }
//
// async fn connect() -> Result<
// 	WebSocketStream<MaybeTlsStream<TcpStream>>,
// 	Box<dyn std::error::Error>,
// > {
// 	let mut retries = 5;
// 	let mut delay = Duration::from_millis(100);
//
// 	loop {
// 		match connect_async("ws://127.0.0.1:8090").await {
// 			Ok((ws_stream, _)) => return Ok(ws_stream),
// 			Err(_e) if retries > 0 => {
// 				retries -= 1;
// 				sleep(delay).await;
// 				delay *= 2; // Exponential backoff
// 			}
// 			Err(e) => return Err(e.into()),
// 		}
// 	}
// }
//
// async fn send_single_request(
// 	ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
// 	id: u64,
// ) -> Result<(), Box<dyn std::error::Error>> {
// 	let request = serde_json::json!({
// 	    "id": format!("req_{}", id),
// 	    "type": "Query",
// 	    "payload": {
// 		"statements": ["MAP 1"],
// 		"params": null
// 	    }
// 	});
//
// 	let request_str = serde_json::to_string(&request)?;
// 	ws.send(Message::Text(Utf8Bytes::from(request_str))).await?;
//
// 	// Wait for response
// 	if let Some(msg) = ws.next().await {
// 		match msg? {
// 			Message::Text(_text) => {
// 				// Response received successfully
// 			}
// 			Message::Close(_) => {
// 				return Err("Connection closed".into());
// 			}
// 			_ => {
// 				// Ignore other message types
// 			}
// 		}
// 	} else {
// 		return Err("No response received".into());
// 	}
//
// 	Ok(())
// }
//
// async fn close(
// 	mut ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
// ) -> Result<(), Box<dyn std::error::Error>> {
// 	// Send close frame with proper close handshake
// 	let _ = ws.close(None).await;
//
// 	// Wait for close confirmation or timeout
// 	let timeout_duration = Duration::from_millis(100);
// 	match tokio::time::timeout(timeout_duration, ws.next()).await {
// 		Ok(Some(Ok(Message::Close(_)))) => {
// 			// Proper close handshake completed
// 		}
// 		_ => {
// 			// Timeout or other response, close completed
// 		}
// 	}
//
// 	Ok(())
// }
//
// fn websocket_single_request_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
// 	let _db = get_or_start_server(&rt);
//
// 	let rt = Runtime::new().unwrap();
// 	c.bench_function("ws_single_request", |b| {
// 		b.iter(|| {
// 			rt.block_on(async {
// 				let mut ws = connect().await.unwrap();
// 				black_box(
// 					send_single_request(&mut ws, 1)
// 						.await
// 						.unwrap(),
// 				);
// 				let _ = close(ws).await;
// 			})
// 		});
// 	});
// }
//
// fn websocket_sequential_requests_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
// 	let _db = get_or_start_server(&rt);
//
// 	let mut group = c.benchmark_group("ws_sequential_requests");
// 	group.sample_size(50);
// 	group.warm_up_time(Duration::from_secs(3));
// 	group.throughput(Throughput::Elements(1));
//
// 	for request_count in [10, 50, 100, 500].iter() {
// 		group.bench_with_input(
// 			BenchmarkId::new("requests", request_count),
// 			request_count,
// 			|b, &request_count| {
// 				b.iter(|| {
// 					rt.block_on(async {
// 						let mut ws = connect()
// 							.await
// 							.unwrap();
// 						for i in 0..request_count {
// 							black_box(send_single_request(&mut ws, i).await.unwrap());
// 						}
// 						let _ = close(ws).await;
// 					})
// 				});
// 			},
// 		);
// 	}
// 	group.finish();
// }
//
// fn websocket_pipelined_requests_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
// 	let _db = get_or_start_server(&rt);
//
// 	let mut group = c.benchmark_group("ws_pipelined_requests");
// 	group.sample_size(50);
// 	group.warm_up_time(Duration::from_secs(3));
// 	group.throughput(Throughput::Elements(1));
//
// 	for request_count in [10, 50, 100, 500].iter() {
// 		group.bench_with_input(
//             BenchmarkId::new("requests", request_count),
//             request_count,
//             |b, &request_count| {
//                 b.iter(|| {
//                     rt.block_on(async {
//                         let mut ws = connect().await.unwrap();
//
//                         // Send all requests without
//                         // waiting for responses
//                         for i in 0..request_count {
//                             let request = serde_json::json!({
// 							    "id": format!("req_{}", i),
// 							    "type": "Query",
// 							    "payload": {
// 								"statements": ["MAP 1"],
// 								"params": null
// 							    }
// 							});
//
//                             let request_str =
// serde_json::to_string(&request).unwrap();
// ws.send(Message::Text(Utf8Bytes::from(request_str))).await.unwrap();
//                         }
//
//                         // Now collect all responses
//                         for _i in 0..request_count {
//                             if let Some(msg) =
//                                 ws.next().await
//                             {
//                                 match msg.unwrap() {
//                                     Message::Text(_text) => {
//                                         // Response received successfully
//                                         black_box(());
//                                     }
//                                     Message::Close(_) => {
//                                         panic!("Connection closed during
// pipelined benchmark");                                     }
//                                     _ => {
//                                         // Ignore other message types
//                                     }
//                                 }
//                             } else {
//                                 panic!("No response received during pipelined
// benchmark");                             }
//                         }
//
//                         let _ = close(ws).await;
//                     })
//                 });
//             },
//         );
// 	}
// 	group.finish();
// }
//
// fn websocket_concurrent_connections_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
// 	let _db = get_or_start_server(&rt);
//
// 	let mut group = c.benchmark_group("ws_concurrent_connections");
// 	group.sample_size(50);
// 	group.warm_up_time(Duration::from_secs(3));
// 	group.throughput(Throughput::Elements(1));
//
// 	for connection_count in [50, 100, 200].iter() {
// 		group.bench_with_input(
// 			BenchmarkId::new("connections", connection_count),
// 			connection_count,
// 			|b, &connection_count| {
// 				b.iter(|| {
// 					rt.block_on(async {
// 						let mut handles = Vec::new();
//
// 						for i in 0..connection_count {
// 							let handle =
// 								tokio::spawn(
// 									async move {
// 										let mut ws = connect().await.unwrap();
// 										send_single_request(&mut ws, i).await.unwrap();
// 										let _ = close(ws).await;
// 									},
// 								);
// 							handles.push(handle);
// 						}
//
// 						for handle in handles {
// 							black_box(
// 								handle.await
// 									.unwrap(
// 									),
// 							);
// 						}
// 					})
// 				});
// 			},
// 		);
// 	}
// 	group.finish();
// }
//
// fn websocket_query_execution_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
// 	let _db = get_or_start_server(&rt);
//
// 	let mut group = c.benchmark_group("ws_query_execution");
// 	group.sample_size(50);
// 	group.warm_up_time(Duration::from_secs(3));
// 	group.throughput(Throughput::Elements(1));
//
// 	group.bench_function("map_one", |b| {
// 		b.iter(|| {
// 			rt.block_on(async {
// 				let mut ws = connect().await.unwrap();
//
// 				let request = serde_json::json!({
// 				    "id": "map_one",
// 				    "type": "Query",
// 				    "payload": {
// 					"statements": ["MAP {1}"],
// 					"params": null
// 				    }
// 				});
//
// 				let request_str =
// 					serde_json::to_string(&request)
// 						.unwrap();
//
// 				ws.send(Message::Text(Utf8Bytes::from(
// 					request_str,
// 				)))
// 				.await
// 				.unwrap();
//
// 				if let Some(msg) = ws.next().await {
// 					black_box(msg.unwrap());
// 				}
//
// 				let _ = close(ws).await;
// 			})
// 		});
// 	});
//
// 	group.bench_function("multiple_queries_same_connection", |b| {
// 		b.iter(|| {
// 			rt.block_on(async {
// 				let mut ws = connect().await.unwrap();
//
// 				for i in 0..50 {
// 					let request = serde_json::json!({
// 					    "id": format!("query_{}", i),
// 					    "type": "Query",
// 					    "payload": {
// 						"statements": [format!("MAP {}", i)],
// 						"params": null
// 					    }
// 					});
//
// 					let request_str =
// 						serde_json::to_string(&request)
// 							.unwrap();
//
// 					ws.send(Message::Text(
// 						Utf8Bytes::from(request_str),
// 					))
// 					.await
// 					.unwrap();
//
// 					if let Some(msg) = ws.next().await {
// 						black_box(msg.unwrap());
// 					}
// 				}
//
// 				let _ = close(ws).await;
// 			})
// 		});
// 	});
//
// 	group.bench_function("complex_filter", |b| {
// 		b.iter(|| {
// 			rt.block_on(async {
// 				let mut ws = connect().await.unwrap();
//
// 				let query = queries::COMPLEX_FILTER;
//
// 				let request = serde_json::json!({
// 				    "id": "complex_filter",
// 				    "type": "Query",
// 				    "payload": {
// 					"statements": [query],
// 					"params": null
// 				    }
// 				});
//
// 				let request_str =
// 					serde_json::to_string(&request)
// 						.unwrap();
// 				ws.send(Message::Text(Utf8Bytes::from(
// 					request_str,
// 				)))
// 				.await
// 				.unwrap();
//
// 				if let Some(msg) = ws.next().await {
// 					black_box(msg.unwrap());
// 				}
//
// 				let _ = close(ws).await;
// 			})
// 		});
// 	});
//
// 	group.finish();
// }
//
// fn websocket_concurrent_query_benchmark(c: &mut Criterion) {
// 	let rt = Runtime::new().unwrap();
//
// 	// Start server once for all benchmarks
// 	let _db = get_or_start_server(&rt);
//
// 	let mut group = c.benchmark_group("ws_concurrent_queries");
// 	group.sample_size(50);
// 	group.warm_up_time(Duration::from_secs(3));
// 	group.throughput(Throughput::Elements(1));
//
// 	for connection_count in [50, 100, 200].iter() {
// 		group.bench_with_input(
//             BenchmarkId::new("concurrent_connections_queries",
// connection_count),             connection_count,
//             |b, &connection_count| {
//                 b.iter(|| {
//                     rt.block_on(async {
//                         let mut handles = Vec::new();
//
//                         for i in 0..connection_count {
//                             let handle = tokio::spawn(async move {
//                                 let mut ws = connect().await.unwrap();
//
//                                 // Send multiple queries on each connection
//                                 for j in 0..5 {
//                                     let request = serde_json::json!({
//                                         "id": format!("concurrent_{}_{}", i,
// j),                                         "type": "Query",
//                                         "payload": {
//                                             "statements": [format!("MAP {} as
// conn, {} as query", i, j)],
// "params": null                                         }
//                                     });
//
//                                     let request_str =
// serde_json::to_string(&request).unwrap();
// ws.send(Message::Text(Utf8Bytes::from(request_str))).await.unwrap();
//
//                                     if let Some(msg) = ws.next().await {
//                                         let _ = msg.unwrap();
//                                     }
//                                 }
//
//                                 let _ = close(ws).await;
//                             });
//                             handles.push(handle);
//                         }
//
//                         // Wait for all connections to complete
//                         for handle in handles {
//                             black_box(handle.await.unwrap());
//                         }
//                     })
//                 });
//             },
//         );
// 	}
// 	group.finish();
// }
//
// criterion_group!(
// 	websocket_benches,
// 	websocket_single_request_benchmark,
// 	websocket_sequential_requests_benchmark,
// 	websocket_pipelined_requests_benchmark,
// 	websocket_concurrent_connections_benchmark,
// 	websocket_query_execution_benchmark,
// 	websocket_concurrent_query_benchmark
// );
// criterion_main!(websocket_benches);

fn main() {
	// Benchmark code is currently disabled
}
