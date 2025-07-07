// use reifydb::ReifyDB;
// use reifydb::runtime::Runtime;
// use reifydb::server::{DatabaseConfig, ServerConfig};
//
// fn main() {
//     let rt = Runtime::new().unwrap();
//
//     ReifyDB::server()
//         .with_config(ServerConfig {
//             database: DatabaseConfig { socket_addr: "127.0.0.1:54321".parse().ok() },
//         })
//         .on_create(|ctx| async move {
//             ctx.tx("create schema test");
//             ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
//             ctx.tx("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)");
//         })
//         .serve_blocking(rt);
// }

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use futures::{SinkExt, StreamExt};
use reifydb::core::{Kind, Value};
use reifydb::network::websocket::RequestPayload::Auth;
use reifydb::network::websocket::{AuthRequestPayload, AuthResponsePayload, Column, QueryRequestPayload, QueryResponsePayload, Request as WebsocketRequest, Request, RequestPayload, Response as WebsocketResponse, ResponsePayload};
use reifydb::{DB, ReifyDB, memory, serializable};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
    println!("🧠 ReifyDB WebSocket server listening on ws://127.0.0.1:9001");

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let (db, root) = ReifyDB::embedded_with(serializable(memory()));
            db.execute_as(&root, r#"create schema test"#).await.unwrap();
            db.execute_as(
                &root,
                r#"create table test.one(field: int1 policy(saturation undefined), other: int1)"#,
            )
            .await
            .unwrap();
            db.execute_as(&root, r#"create table test.two(field: int1)"#).await.unwrap();
            let _err = db
                .execute_as(
                    &root,
                    r#"insert (-129,2),(2,2),(3,2),(4,2),(5,2) into test.one (field, other)"#,
                )
                .await
                .unwrap();
            let _err =
                db.execute_as(&root, r#"insert (2),(3) into test.two (field)"#).await.unwrap();

            let ws_stream = accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws_stream.split();

            match read.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    dbg!(&text);
                    match serde_json::from_str::<WebsocketRequest>(&text) {
                        Ok(request) => match request.payload {
                            Auth(AuthRequestPayload { token: Some(token) }) => {
                                if validate_token(&token.as_str()) {
                                    println!("✅ Authenticated: {}", token);

                                    let response = WebsocketResponse {
                                        id: request.id,
                                        payload: ResponsePayload::Auth(AuthResponsePayload {}),
                                    };

                                    let msg = serde_json::to_string(&response)
                                        .unwrap();
                                    write
                                        .send(WsMessage::Text(msg))
                                        .await
                                        .unwrap();


                                    // Ready to accept other messages
                                    while let Some(Ok(msg)) = read.next().await {
                                        if let WsMessage::Text(text) = msg {
                                            match serde_json::from_str::<Request>(&text) {
                                                Ok(request) => match request.payload {
                                                    RequestPayload::Query(
                                                        QueryRequestPayload { statements },
                                                    ) => {
                                                        println!(
                                                            "📥 Received query: {}",
                                                            statements.join(",")
                                                        );

                                                        let statement = statements.first().unwrap();
                                                        let mut result = db
                                                            .query_as(&root, statement)
                                                            .await
                                                            .unwrap();

                                                        let frame = result.pop().unwrap();
                                                        let response = WebsocketResponse {
                                                            id: request.id,
                                                            payload: ResponsePayload::Query(
                                                                QueryResponsePayload {
                                                                    columns: frame
                                                                        .columns
                                                                        .into_iter()
                                                                        .map(|c| {
                                                                            Column {
																				name: c.name,
																				kind: Kind::Int2,
																				data: c.values.iter().map(|v| if v == Value::Undefined {
																					"⟪undefined⟫".to_string()
																				} else {
																					v.to_string()
																				}).collect(),
                                                                            }
                                                                        })
                                                                        .collect::<Vec<_>>(),
                                                                },
                                                            ),
                                                        };

                                                        let msg = serde_json::to_string(&response)
                                                            .unwrap();
                                                        write
                                                            .send(WsMessage::Text(msg))
                                                            .await
                                                            .unwrap();
                                                    }
                                                    _ => {}
                                                },
                                                Err(err) => {
                                                    eprintln!("❌ Invalid message: {err}");
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    eprintln!("❌ Invalid token: {token}");
                                    let _ = write.send(WsMessage::Close(None)).await;
                                }
                            }
                            _ => {
                                eprintln!("❌ First message must be auth");
                                let _ = write.send(WsMessage::Close(None)).await;
                            }
                        },
                        Err(err) => {
                            eprintln!("❌ Failed to parse auth message: {err}");
                            let _ = write.send(WsMessage::Close(None)).await;
                        }
                    }
                }
                _ => {
                    eprintln!("❌ No valid first message");
                }
            }
        });
    }
}

fn validate_token(token: &str) -> bool {
    token == "mysecrettoken"
}
