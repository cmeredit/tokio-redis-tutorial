use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Debug)]
enum Command {
    Get { key: String },
    Set { key: String, val: Bytes },
}

#[tokio::main]
async fn main() {
    // MP/SC channel that buffers up to 32 messages
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    tokio::spawn(async move {
        tx.send("Sending from task 1").await.unwrap();
    });

    tokio::spawn(async move {
        tx2.send("Sending from task 2").await.unwrap();
    });

    while let Some(message) = rx.recv().await {
        println!("Got \"{}\"", message);
    }
}
