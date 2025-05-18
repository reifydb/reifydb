use reifydb::server::service::query_service;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = "[::1]:4321".parse().unwrap();

    Server::builder().add_service(query_service()).serve(address).await?;
    Ok(())
}
