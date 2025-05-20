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
            ctx.tx("create table test.arith(id: int2, num: int2)");
            ctx.tx("insert (1,6), (2,8), (3,4), (4,2), (5,3) into test.arith(id,num)");
            for l in ctx.tx("from test.arith select avg(id, num)") {
                println!("{}", l)
            }
        })
        .serve_blocking(rt);
}
