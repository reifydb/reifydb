use reifydb::{Params, Session, embedded};

fn main() {
    // Create and start an in-memory database
    let mut db = embedded::memory_optimistic().build().unwrap();
    db.start().unwrap();

    // Set up the test scenario exactly as specified
    db.command_as_root("create namespace test", Params::None).unwrap();
    
    db.command_as_root(
        "create table test.orders { id: int4, joined_id: int4, amount: int4 }",
        Params::None,
    ).unwrap();
    
    db.command_as_root(
        "create table test.customers { id: int4, name: utf8 }",
        Params::None,
    ).unwrap();
    
    db.command_as_root(
        "from [{id: 1, joined_id: 101, amount: 100}] insert test.orders",
        Params::None,
    ).unwrap();
    
    db.command_as_root(
        "from [{id: 101, name: \"Alice\"}] insert test.customers",
        Params::None,
    ).unwrap();

    // Execute the problematic query
    println!("Query: from test.orders inner join {{ from test.customers }} on joined_id == id");
    println!("---");
    
    for frame in db.query_as_root(
        "from test.orders inner join { from test.customers } on joined_id == id",
        Params::None,
    ).unwrap() {
        println!("{}", frame);
    }

    // Let's also check the explain output to understand what's happening
    println!("\n\nLogical plan:");
    println!("-------------");
    let logical = db.query_as_root(
        "explain logical 'from test.orders inner join { from test.customers } on joined_id == id'",
        Params::None,
    ).unwrap();
    for frame in logical {
        println!("{}", frame);
    }

    // Check physical plan
    println!("\n\nPhysical plan:");
    println!("--------------");
    let physical = db.query_as_root(
        "explain physical 'from test.orders inner join { from test.customers } on joined_id == id'",
        Params::None,
    ).unwrap();
    for frame in physical {
        println!("{}", frame);
    }

    // Try with an explicit alias to see the difference
    println!("\n\nWith explicit alias 'customers':");
    println!("--------------------------------");
    for frame in db.query_as_root(
        "from test.orders inner join { from test.customers } customers on joined_id == customers.id",
        Params::None,
    ).unwrap() {
        println!("{}", frame);
    }

    // Also try referencing the joined table's id explicitly
    println!("\n\nWith orders.joined_id:");
    println!("----------------------");
    for frame in db.query_as_root(
        "from test.orders inner join { from test.customers } customers on orders.joined_id == customers.id",
        Params::None,
    ).unwrap() {
        println!("{}", frame);
    }
}