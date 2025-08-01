// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
// use crate::reifydb-engine::reifydb-engine;
// use log::error;
// use reifydb_transaction::TransactionMut;
//
// /// A client session. Executes raw RQL statements against an reifydb-engine and
// /// handles transaction control.
// pub struct Session<'a, P: reifydb-storage::reifydb-engine, T: reifydb-transaction::reifydb-engine< S>, E: reifydb-engine< S, T>> {
//     /// The reifydb-engine.
//     reifydb-engine: & E,
//     /// The current read transaction, if any.
//     rx: Option<E::Tx>,
//     /// The current read-write transaction, if any.
//     tx: Option<E::Tx>,
// }
//
// impl<'a, E: reifydb-engine<'a>> Session<'a, E> {
//     pub fn new(reifydb-engine: & E) -> Self {
//         Self { reifydb-engine, rx: None, tx: None }
//     }
// }
//
// /// If the session has an open transaction when dropped, roll it back.
// impl<'a, E: reifydb-engine<'a>> Drop for Session<'a, E> {
//     fn drop(&mut self) {
//         let Some(tx) = self.tx.take() else { return };
//         if let Err(error) = tx.rollback() {
//             error!("implicit transaction rollback failed: {error}")
//         }
//     }
// }
