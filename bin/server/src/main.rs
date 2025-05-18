use reifydb::server::{DatabaseConfig, ServerConfig};
use reifydb::{ReifyDB, memory, mvcc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ReifyDB::server_with(
        ServerConfig { database: DatabaseConfig { socket_addr: "[::1]:4321".parse().ok() } },
        mvcc(memory()),
    )
    .before_bootstrap(|ctx| async move {
        ctx.info("test");
    })
    .on_create(|ctx| async move{
        ctx.tx(r#"create schema test"#);
        ctx.tx(r#"create table test.arith(id: int2, num: int2)"#);
        ctx.tx(r#"insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)"#);
        
        
    })
    .serve()
    .await
    .unwrap();

    Ok(())
}
