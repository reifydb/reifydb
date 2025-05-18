use reifydb::server::grpc::auth::AuthInterceptor;
use reifydb::server::grpc::query_service;
use tonic::service::interceptor::InterceptorLayer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = "[::1]:4321".parse().unwrap();

    Server::builder()
        .layer(InterceptorLayer::new(AuthInterceptor {}))
        .add_service(query_service())
        .serve(address)
        .await?;
    Ok(())
}
