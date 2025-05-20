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
            for l in ctx.tx("select abs(+1)") {
                println!("{}", l)
            }
        })
        .serve_blocking(rt);
}
