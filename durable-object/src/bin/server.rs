use std::io;

use durable_object::server::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    pretty_env_logger::init();

    let server = Server::default();
    server.start("/tmp/test-vfs-sock").await
}
