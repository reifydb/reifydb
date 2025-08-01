// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Type;
use reifydb_core::result::error::diagnostic::Diagnostic;
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
    Auth(AuthResponse),
    Err(ErrResponse),
    Write(WriteResponse),
    Read(ReadResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrResponse {
    pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadResponse {
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
    pub ty: Type,
    pub data: Vec<String>,
    pub frame: Option<String>,
}
