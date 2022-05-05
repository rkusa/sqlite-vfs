use std::io;

use durable_object::client::Client;

fn main() -> io::Result<()> {
    pretty_env_logger::init();

    let mut client = Client::connect(
        "127.0.0.1:6000",
        "test.db",
        durable_object::request::OpenAccess::Create,
    )?;
    let data = "Some data ...".as_bytes();
    client.put(0, data.to_vec())?;
    assert_eq!(client.get(0..data.len() as u64)?, data);

    Ok(())
}
