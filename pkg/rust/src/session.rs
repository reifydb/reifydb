// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// use auth::Principal;
// use engine::execute::ExecutionResult;
//
// #[derive(Debug, Clone)]
// pub struct SessionRx<'a, T: DB<'a>> {
//     principal: Principal,
//     db: &'a T,
// }
//
// impl<'a, T: DB<'a>> SessionRx<'a, T> {
//     /// runs tx
//     pub fn tx_execute(&self, rql: &str) -> Vec<ExecutionResult> {
//         todo!()
//     }
//
//     /// runs rx
//     pub fn execute(&self, rql: &str) -> Vec<ExecutionResult> {
//         // self.db.rx_execute(&self.principal, rql)
//         todo!()
//     }
// }
//
// pub trait IntoSessionRx<'a, T: DB<'a>> {
//     fn into_session_rx(self, db: &'a T) -> base::Result<SessionRx<'a, T>>;
// }
//
// impl<'a, T: DB<'a>> IntoSessionRx<'a, T> for Principal {
//     fn into_session_rx(self, db: &'a T) -> base::Result<SessionRx<'a, T>> {
//         Ok(SessionRx { principal: self, db })
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct SessionTx<'a, T: DB<'a>> {
//     principal: Principal,
//     db: &'a T,
// }
//
// impl<'a, T: DB<'a>> SessionTx<'a, T> {
//     /// runs tx
//     pub fn execute(&self, rql: &str) -> Vec<ExecutionResult> {
//         // self.db.tx_execute(&self.principal, rql)
//         todo!()
//     }
//
//     /// runs rx
//     pub fn rx_execute(&self, rql: &str) -> Vec<ExecutionResult> {
//         // self.db.rx_execute(&self.principal, rql)
//         todo!()
//     }
// }
//
// pub trait IntoSessionTx<'a, T: DB<'a>> {
//     fn into_session_tx(self, db: &'a T) -> base::Result<SessionTx<'a, T>>;
// }
//
// impl<'a, T: DB<'a>> IntoSessionTx<'a, T> for Principal {
//     fn into_session_tx(self, db: &'a T) -> base::Result<SessionTx<'a, T>> {
//         Ok(SessionTx { principal: self, db })
//     }
// }
