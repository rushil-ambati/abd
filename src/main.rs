mod abd;

use abd::{messages::ABDMessage, reader, server::Server, writer::Writer};
use ractor::Actor;

const NUM_REPLICAS: usize = 3;
const NUM_READERS: usize = 2;

#[tokio::main]
async fn main() {
    let mut servers = Vec::new();
    let mut server_handles = Vec::new();

    for _ in 0..NUM_REPLICAS {
        let (server, server_handle) = Actor::spawn(None, Server, ())
            .await
            .expect("Failed to start Server");
        servers.push(server);
        server_handles.push(server_handle);
    }

    // Fail a subset of the replica servers (e.g., fail one server)
    server_handles[0].abort();
    // server_handles[1].abort();

    let mut readers = Vec::new();
    let mut reader_handles = Vec::new();

    for _ in 0..NUM_READERS {
        let (reader, reader_handle) = Actor::spawn(None, reader::Reader, servers.clone())
            .await
            .expect("Failed to start Reader");
        readers.push(reader);
        reader_handles.push(reader_handle);
    }

    let (writer, writer_handle) = Actor::spawn(None, Writer, servers.clone())
        .await
        .expect("Failed to start Writer");

    // Write a value to the shared object.
    writer
        .cast(ABDMessage::Write("First version".to_string()))
        .expect("Failed to Write");

    // Wait briefly to ensure processing.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Read the value from the shared object.
    for reader in readers.iter() {
        // Read the value from the shared object.
        reader.cast(ABDMessage::Read).expect("Failed to Read");
    }

    writer_handle.await.expect("Writer actor failed");
    for handle in reader_handles {
        handle.await.expect("Reader actor failed");
    }
    for handle in server_handles {
        handle.await.expect("Server actor failed");
    }
}
