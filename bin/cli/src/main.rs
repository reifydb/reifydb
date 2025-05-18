// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]

// fn main() {
//     let (db, root) = ReifyDB::embedded();
//     // returns (db, root)
//     // let session = db.session(root)
//     // session.tx_execute('')
//     // db.tx_execute_as(&root, r#"create schema test"#);
//
//     let session = db.session(root.clone()).unwrap();
//     for result in session.execute("select 2, 3, 4") {
//         println!("{}", result);
//     }
//
//     let session = db.session_read_only(root.clone()).unwrap();
//     for result in session.execute("select 5, 6, 7, 8") {
//         println!("{}", result);
//     }
//
//     // db.tx_execute_as(&root, r#"create schema test"#);
//     // db.tx_execute_as(&root, r#"create table test.arith(id: int2, num: int2)"#);
//     // db.tx_execute_as(&root, r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);
//     //
//     // let result = db
//     //     .rx_execute_as(&root, r#"from test.arith select id + 1, 2 + num + 3, id + num, num + num"#);
//     //
//     // for mut result in result {
//     //     println!("{}", result);
//     // }
// }

use reifydb::Value;
use reifydb::client::Client;
use tonic::codegen::tokio_stream::StreamExt;

use tokio::io::{self, AsyncBufReadExt, BufReader};

use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = env::args().nth(1).expect("Usage: program '<query>'");

    let client = Client {};

    let table = client.query(&query).await;

    // Print column headers
    for col in &table.columns {
        print!("{} ({}) | ", col.name, col.value);
    }
    println!();

    // Print rows
    let mut rows = table.rows;
    while let Some(row) = rows.next().await {
        match row {
            Ok(values) => {
                let formatted: Vec<String> = values
                    .into_iter()
                    .map(|v| match v {
                        Value::Bool(v) => v.to_string(),
                        Value::Int2(v) => v.to_string(),
                        Value::Uint2(v) => v.to_string(),
                        Value::Text(v) => v,
                        _ => "[unsupported]".to_string(),
                    })
                    .collect();
                println!("{}", formatted.join(" | "));
            }
            Err(e) => {
                eprintln!("❌ Row error: {e}");
            }
        }
    }

    Ok(())
}
