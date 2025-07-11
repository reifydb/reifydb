// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
//
// // #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// // #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// // #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]
//
// use reifydb_cli::cli;
//
// fn main() {
//     let args: Vec<String> = std::env::args().collect();
//     if let Err(err) = cli(args) {
//         println!("{err:?}");
//         std::process::exit(1)
//     }
// }

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::network::ws::client::WsClient;

#[tokio::main]
async fn main() {
    let client = WsClient::connect("ws://127.0.0.1:8090").await.unwrap();

    client.auth(Some("mysecrettoken".into())).await.unwrap();

    let result = client.rx("SELECT cast('test', text); SELECT 2;".into()).await.unwrap();

    println!("✅ Frames: {:?}", result);
}
