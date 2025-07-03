use reifydb::ReifyDB;
use reifydb::runtime::Runtime;
use reifydb::server::{DatabaseConfig, ServerConfig};

fn main() {
    let rt = Runtime::new().unwrap();

    ReifyDB::server()
        .with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: "127.0.0.1:54321".parse().ok() },
        })
        .on_create(|ctx| async move {
            ctx.tx("create schema test");
            ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
            ctx.tx("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)");
        })
        .serve_blocking(rt);
}
