use reifydb::ReifyDB;
use reifydb::server::{DatabaseConfig, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ReifyDB::server(ServerConfig {
        database: DatabaseConfig { socket_addr: "[::1]:4321".parse().ok() },
    })
    .before_bootstrap(|ctx| async move {
        ctx.info("test");
    })
    .serve()
    .await
    .unwrap();

    Ok(())
}
