// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
    Write(WriteRequest),
    Read(ReadRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
    pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub statements: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRequest {
    pub statements: Vec<String>,
}
