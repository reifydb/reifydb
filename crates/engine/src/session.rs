// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
//
// use crate::reifydb_engine::reifydb_engine;
// use log::error;
// use transaction::TransactionMut;
//
// /// A client session. Executes raw RQL statements against an reifydb_engine and
// /// handles transaction control.
// pub struct Session<'a, P: Persistence::reifydb_engine, T: transaction::reifydb_engine< S>, E: reifydb_engine< S, T>> {
//     /// The reifydb_engine.
//     reifydb_engine: & E,
//     /// The current read transaction, if any.
//     rx: Option<E::Tx>,
//     /// The current read-write transaction, if any.
//     tx: Option<E::Tx>,
// }
//
// impl<'a, E: reifydb_engine<'a>> Session<'a, E> {
//     pub fn new(reifydb_engine: & E) -> Self {
//         Self { reifydb_engine, rx: None, tx: None }
//     }
// }
//
// /// If the session has an open transaction when dropped, roll it back.
// impl<'a, E: reifydb_engine<'a>> Drop for Session<'a, E> {
//     fn drop(&mut self) {
//         let Some(tx) = self.tx.take() else { return };
//         if let Err(error) = tx.rollback() {
//             error!("implicit transaction rollback failed: {error}")
//         }
//     }
// }
