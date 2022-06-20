use std::io;

use durable_object::server::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    pretty_env_logger::init();

    let server = Server::default();
    server.start("127.0.0.1:6000").await
}
