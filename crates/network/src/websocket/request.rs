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
    Auth(AuthRequestPayload),
    Query(QueryRequestPayload),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequestPayload {
    pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequestPayload {
    pub statements: Vec<String>,
}
