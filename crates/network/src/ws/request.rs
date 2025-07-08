// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub payload: RequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
    Auth(AuthRequest),
    Tx(TxRequest),
    Rx(RxRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
    pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxRequest {
    pub statements: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RxRequest {
    pub statements: Vec<String>,
}
