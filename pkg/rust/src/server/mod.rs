// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DB, IntoSessionRx, IntoSessionTx, SessionRx, SessionTx};
use auth::Principal;
use engine::Engine;
use engine::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use rql::ast;
use rql::plan::{plan, plan_mut};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use storage::StorageEngine;
use tokio::task::spawn_blocking;
use transaction::{Rx, TransactionEngine, Tx};

pub struct Server<'a, S: StorageEngine, T: TransactionEngine<'a, S>> {
    engine: Arc<Engine<'a, S, T>>,
}

impl<'a, S: StorageEngine, T: TransactionEngine<'a, S>> Server<'a, S, T> {
    pub fn new(transaction: T) -> (Self, Principal) {
        let principal = Principal::System { id: 1, name: "root".to_string() };

        (Self { engine: Arc::new(Engine::new(transaction)) }, principal)
    }
}

impl<S: StorageEngine + 'static, T: TransactionEngine<'static, S> + 'static> Server<'static, S, T> {
    pub async fn serve(&self) -> std::io::Result<()> {
        let engine = self.engine.clone();
        spawn_blocking(move || {
            let engine = engine.clone();
            let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
            // let db = Arc::new(DashMap::new());

            println!("Server listening on 127.0.0.1:6379");

            let engine = engine.clone();
            for stream in listener.incoming() {
                let engine = engine.clone();
                // let db = db.clone();
                let stream = stream.unwrap();

                thread::spawn(move || Self::handle_client(stream, engine.clone()));
            }
        });
        Ok(())
    }

    fn handle_client(stream: TcpStream, engine: Arc<Engine<'static, S, T>>) {
        let reader = BufReader::new(&stream);
        let mut writer = stream.try_clone().unwrap();

        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };

            let response = Self::handle_command(&line, engine.clone());
            let _ = writer.write_all(response.as_bytes());
        }
    }

    fn handle_command(line: &str, engine: Arc<Engine<'static, S, T>>) -> String {
        let tokens: Vec<&str> = line.trim().split_whitespace().collect();
        if tokens.is_empty() {
            return "-ERR Empty command\n".to_string();
        }

        match tokens[0].to_uppercase().as_str() {
            "GET" if tokens.len() == 2 => {
                let mut result = vec![];
                let statements = ast::parse("from users select id, name");

                let mut rx = &engine.begin_read_only().unwrap();

                for statement in statements {
                    let plan = plan_mut(rx.catalog().unwrap(), statement).unwrap();
                    let er = execute_plan(plan, rx).unwrap();
                    result.push(er);
                }

                dbg!(&result);
                
                
                "$-1\n".to_string()
            }
            // db
            //     .get(tokens[1])
            //     .map(|v| format!("+{}\n", v.value()))
            //     .unwrap_or_else(|| ),
            "SET" if tokens.len() == 3 => {
                let key = tokens[1].to_string();
                let value = tokens[2].to_string();
                // rayon::spawn_fifo({
                // 	let db = db.clone();
                // 	move || {
                // db.insert(key, value);
                // }
                // });
                "+OK\n".to_string()
            }

            "BATCHSET" if tokens.len() >= 3 && tokens.len() % 2 == 1 => {
                let kvs: Vec<(String, String)> = tokens[1..]
                    .chunks(2)
                    .map(|pair| (pair[0].to_string(), pair[1].to_string()))
                    .collect();

                // let db = db.clone();
                // rayon::spawn_fifo(move || {
                // kvs.into_par_iter().for_each(|(k, v)| {
                //     db.insert(k, v);
                // });
                // });

                "+OK\n".to_string()
            }

            _ => "-ERR Unknown or invalid command\n".to_string(),
        }
    }
}

impl<'a, S: StorageEngine, T: TransactionEngine<'a, S>> Server<'a, S, T> {}

impl<'a, S: StorageEngine, T: TransactionEngine<'a, S>> DB<'a> for Server<'a, S, T> {
    fn tx_execute_as(&'a self, _principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let mut tx = self.engine.begin().unwrap();

        for statement in statements {
            let plan = plan_mut(tx.catalog().unwrap(), statement).unwrap();
            let er = execute_plan_mut(plan, &mut tx).unwrap();
            result.push(er);
        }

        tx.commit().unwrap();

        result
    }

    fn rx_execute_as(&'a self, principal: &Principal, rql: &str) -> Vec<ExecutionResult> {
        let mut result = vec![];
        let statements = ast::parse(rql);

        let rx = self.engine.begin_read_only().unwrap();
        for statement in statements {
            let plan = plan(statement).unwrap();
            let er = execute_plan(plan, &rx).unwrap();
            result.push(er);
        }

        result
    }

    fn session_read_only(
        &'a self,
        into: impl IntoSessionRx<'a, Self>,
    ) -> base::Result<SessionRx<'a, Self>> {
        into.into_session_rx(&self)
        // todo!()
    }

    fn session(&'a self, into: impl IntoSessionTx<'a, Self>) -> base::Result<SessionTx<'a, Self>> {
        into.into_session_tx(&self)
        // todo!()
    }
}
