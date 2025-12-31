// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//
// use crate::engine::Engine;
// use log::error;
// use reifydb_transaction::TransactionMut;
//
// /// A client session. Executes raw RQL statements against an engine and
// /// handles transaction control.
// pub struct Session<'a, P: Storage::Engine, T: Transaction::Engine< S>, E: Engine< S, T>> {
//     /// The engine.
//     engine: & E,
//     /// The current read transaction, if any.
//     rx: Option<E::Tx>,
//     /// The current read-write transaction, if any.
//     tx: Option<E::Tx>,
// }
//
// impl<'a, E: Engine<'a>> Session<'a, E> {
//     pub fn new(engine: & E) -> Self {
//         Self { engine, rx: None, tx: None }
//     }
// }
//
// /// If the session has an open transaction when dropped, roll it back.
// impl<'a, E: Engine<'a>> Drop for Session<'a, E> {
//     fn drop(&mut self) {
//         let Some(tx) = self.tx.take() else { return };
//         if let Err(error) = tx.rollback() {
//             error!("implicit transaction rollback failed: {error}")
//         }
//     }
// }
