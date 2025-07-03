// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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
    Query(QueryResponsePayload),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponsePayload {}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponsePayload {
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column{
    pub name: String,
}