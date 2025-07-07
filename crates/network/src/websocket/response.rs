// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Kind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    #[serde(flatten)]
    pub payload: ResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
    Auth(AuthResponsePayload),
    Execute(ExecuteResponsePayload),
    Query(QueryResponsePayload),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponsePayload {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteResponsePayload {
    pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponsePayload {
    pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebsocketFrame {
    pub name: String,
    pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebsocketColumn {
    pub name: String,
    pub kind: Kind,
    pub data: Vec<String>,
}
