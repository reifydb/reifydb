// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
//
// use crate::engine::Engine;
// use log::error;
// use transaction::TransactionMut;
//
// /// A client session. Executes raw RQL statements against an engine and
// /// handles transaction control.
// pub struct Session<'a, S: storage::Engine, T: transaction::Engine<'a, S>, E: Engine<'a, S, T>> {
//     /// The engine.
//     engine: &'a E,
//     /// The current read transaction, if any.
//     rx: Option<E::Tx>,
//     /// The current read-write transaction, if any.
//     tx: Option<E::Tx>,
// }
//
// impl<'a, E: Engine<'a>> Session<'a, E> {
//     pub fn new(engine: &'a E) -> Self {
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
