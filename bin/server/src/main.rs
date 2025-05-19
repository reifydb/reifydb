use reifydb::ReifyDB;
use reifydb::server::{DatabaseConfig, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ReifyDB::server()
        .with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: "127.0.0.1:4321".parse().ok() },
        })
        .before_bootstrap(|ctx| async move {
            ctx.info("test");
        })
        .on_create(|ctx| async move {
            ctx.tx(r#"create schema test"#);
            ctx.tx(r#"create table test.arith(id: int2, num: int2)"#);
            ctx.tx(r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);
        })
        .serve()
        .await
        .unwrap();

    Ok(())
}
