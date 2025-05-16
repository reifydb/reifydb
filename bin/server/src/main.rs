use std::sync::Arc;

use dashmap::DashMap;
use reifydb::server::Server;
use reifydb::{DB, memory, mvcc};

type Db = Arc<DashMap<String, String>>;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (server, root) = Server::new(mvcc(memory()));
    server.tx_execute_as(&root, r#"create schema test"#);
    server.tx_execute_as(&root, r#"create table test.arith(id: int2, num: int2)"#);
    server.tx_execute_as(
        &root,
        r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#,
    );

    let result = server
        .rx_execute_as(&root, r#"from test.arith select id + 1, 2 + num + 3, id + num, num + num"#);

    for mut result in result {
        println!("{}", result);
    }

    server.serve().await;

    Ok(())

    // 	let listener = TcpListener::bind("127.0.0.1:6379")?;
    // 	let db = Arc::new(DashMap::new());
    //
    // 	println!("Server listening on 127.0.0.1:6379");
    //
    // 	for stream in listener.incoming() {
    // 		let db = db.clone();
    // 		let stream = stream?;
    //
    // 		thread::spawn(move || handle_client(stream, db));
    // 	}
    //
    // 	Ok(())
    // }
    //
    // fn handle_client(stream: TcpStream, db: Db) {
    // 	let reader = BufReader::new(&stream);
    // 	let mut writer = stream.try_clone().unwrap();
    //
    // 	for line in reader.lines() {
    // 		let line = match line {
    // 			Ok(l) => l,
    // 			Err(_) => break,
    // 		};
    //
    // 		let response = handle_command(&line, &db);
    // 		let _ = writer.write_all(response.as_bytes());
    // 	}
    // }
    //
    // fn handle_command(line: &str, db: &Db) -> String {
    // 	let tokens: Vec<&str> = line.trim().split_whitespace().collect();
    // 	if tokens.is_empty() {
    // 		return "-ERR Empty command\n".to_string();
    // 	}
    //
    // 	match tokens[0].to_uppercase().as_str() {
    // 		"GET" if tokens.len() == 2 => {
    // 			db.get(tokens[1])
    // 				.map(|v| format!("+{}\n", v.value()))
    // 				.unwrap_or_else(|| "$-1\n".to_string())
    // 		}
    //
    // 		"SET" if tokens.len() == 3 => {
    // 			let key = tokens[1].to_string();
    // 			let value = tokens[2].to_string();
    // 			// rayon::spawn_fifo({
    // 			// 	let db = db.clone();
    // 			// 	move || {
    // 					db.insert(key, value);
    // 				// }
    // 			// });
    // 			"+OK\n".to_string()
    // 		}
    //
    // 		"BATCHSET" if tokens.len() >= 3 && tokens.len() % 2 == 1 => {
    // 			let kvs: Vec<(String, String)> = tokens[1..]
    // 				.chunks(2)
    // 				.map(|pair| (pair[0].to_string(), pair[1].to_string()))
    // 				.collect();
    //
    // 			let db = db.clone();
    // 			// rayon::spawn_fifo(move || {
    // 				kvs.into_par_iter().for_each(|(k, v)| {
    // 					db.insert(k, v);
    // 				});
    // 			// });
    //
    // 			"+OK\n".to_string()
    // 		}
    //
    // 		_ => "-ERR Unknown or invalid command\n".to_string(),
    // 	}
}

// use std::sync::Arc;
//
// use dashmap::DashMap;
// use rayon::prelude::*;
// use tokio::{
// 	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
// 	net::{TcpListener, TcpStream},
// };
//
// type Db = Arc<DashMap<String, String>>;
//
// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
// 	let listener = TcpListener::bind("127.0.0.1:6379").await?;
// 	let db = Arc::new(DashMap::new());
//
// 	println!("Async server listening on 127.0.0.1:6379");
//
// 	loop {
// 		let (stream, _) = listener.accept().await?;
// 		let db = db.clone();
// 		tokio::spawn(async move {
// 			if let Err(e) = handle_connection(stream, db).await {
// 				eprintln!("Connection error: {:?}", e);
// 			}
// 		});
// 	}
// }
//
// async fn handle_connection(stream: TcpStream, db: Db) -> anyhow::Result<()> {
// 	let (reader, mut writer) = stream.into_split();
// 	let mut lines = BufReader::new(reader).lines();
//
// 	while let Some(line) = lines.next_line().await? {
// 		let response = handle_command(&line, &db).await;
// 		writer.write_all(response.as_bytes()).await?;
// 	}
//
// 	Ok(())
// }
//
// async fn handle_command(line: &str, db: &Db) -> String {
// 	let tokens: Vec<&str> = line.trim().split_whitespace().collect();
// 	if tokens.is_empty() {
// 		return "-ERR Empty command\n".to_string();
// 	}
//
// 	match tokens[0].to_uppercase().as_str() {
// 		"GET" if tokens.len() == 2 => {
// 			db.get(tokens[1])
// 				.map(|v| format!("+{}\n", v.value()))
// 				.unwrap_or_else(|| "$-1\n".to_string())
// 		}
//
// 		"SET" if tokens.len() == 3 => {
// 			let key = tokens[1].to_string();
// 			let value = tokens[2].to_string();
// 			let db = db.clone();
//
// 			// Rayon in async context
// 			tokio::task::spawn_blocking(move || {
// 				db.insert(key, value);
// 			})
// 				.await
// 				.ok();
//
// 			"+OK\n".to_string()
// 		}
//
// 		"BATCHSET" if tokens.len() >= 3 && tokens.len() % 2 == 1 => {
// 			let kvs: Vec<(String, String)> = tokens[1..]
// 				.chunks(2)
// 				.map(|pair| (pair[0].to_string(), pair[1].to_string()))
// 				.collect();
// 			let db = db.clone();
//
// 			tokio::task::spawn_blocking(move || {
// 				kvs.into_par_iter().for_each(|(k, v)| {
// 					db.insert(k, v);
// 				});
// 			})
// 				.await
// 				.ok();
//
// 			"+OK\n".to_string()
// 		}
//
// 		_ => "-ERR Unknown or invalid command\n".to_string(),
// 	}
// }
