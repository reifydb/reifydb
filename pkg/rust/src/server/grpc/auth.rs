// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::server::grpc::AuthenticatedUser;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct AuthInterceptor;

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let metadata = req.metadata();
        let token = extract_token(metadata)?;

        let principal = decode_token_to_principal(&token)
            .map_err(|_| Status::unauthenticated("Invalid token"))?;

        req.extensions_mut().insert(principal);

        Ok(req)
    }
}

fn extract_token(metadata: &MetadataMap) -> Result<String, Status> {
    metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("Missing auth header"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid header format"))
        .map(|s| s.trim_start_matches("Bearer ").to_string())
}

// Dummy parser — replace with JWT decoding
fn decode_token_to_principal(token: &str) -> Result<AuthenticatedUser, ()> {
    Ok(AuthenticatedUser { user_id: token.to_string(), roles: vec!["user".into()] })
}
