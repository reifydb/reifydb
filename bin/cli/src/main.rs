// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

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

use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    thread,
    time::Instant,
};

// const THREADS: usize = 32;
// const REQUESTS_PER_THREAD: usize = 10_000;

fn main() {
    let start = Instant::now();

    let mut handles = vec![];

    // for i in 0..THREADS {
    let handle = thread::spawn(move || {
        let stream = TcpStream::connect("127.0.0.1:6379").expect("Failed to connect");
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        // for j in 0..REQUESTS_PER_THREAD {
        let key = format!("key_{}_{}", 0, 1);
        let value = format!("val_{}", 1);

        // SET
        // let cmd = format!("SET {} {}\n", key, value);
        // writer.write_all(cmd.as_bytes()).unwrap();

        let mut line = String::new();
        // reader.read_line(&mut line).unwrap();

        // GET
        let cmd = format!("GET {}\n", key);
        writer.write_all(cmd.as_bytes()).unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        println!("{line}");
        assert!(line.starts_with('+') || line.starts_with('$')); // basic check
        // }
    });

    handles.push(handle);
    // }

    for h in handles {
        h.join().unwrap();
    }

    // let duration = start.elapsed();
    // let total_requests = THREADS * REQUESTS_PER_THREAD * 2;
    // let rps = total_requests as f64 / duration.as_secs_f64();
    // 
    // println!("Completed {} requests in {:.2?} ({:.2} req/s)", total_requests, duration, rps);
}

// use tokio::{
//     io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
//     net::TcpStream,
//     time::Instant,
// };
//
// const CLIENTS: usize = 128;
//
// #[tokio::main]
// async fn main() {
//     let start = Instant::now();
//
//     let mut handles = vec![];
//
//     for i in 0..CLIENTS {
//         handles.push(tokio::spawn(run_client(i)));
//     }
//
//     for handle in handles {
//         handle.await.unwrap();
//     }
//
//     let elapsed = start.elapsed();
//     let total_requests = 100_000;
//     let rps = total_requests as f64 / elapsed.as_secs_f64();
//
//     println!(
//         "Completed {} requests in {:.2?} ({:.2} req/s)",
//         total_requests, elapsed, rps
//     );
// }
//
// async fn run_client(client_id: usize) {
//     let stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
//     let (reader, mut writer) = stream.into_split();
//     let mut reader = BufReader::new(reader).lines();
//
//     for i in 0..100_000 / CLIENTS {
//         let key = format!("key_{}_{}", client_id, i);
//         let val = format!("val_{}", i);
//
//         // SET
//         writer
//             .write_all(format!("SET {} {}\n", key, val).as_bytes())
//             .await
//             .unwrap();
//         reader.next_line().await.unwrap().unwrap(); // discard +OK
//
//         // GET
//         writer
//             .write_all(format!("GET {}\n", key).as_bytes())
//             .await
//             .unwrap();
//         let response = reader.next_line().await.unwrap().unwrap();
//         assert!(response.starts_with('+') || response.starts_with('$'));
//     }
// }
