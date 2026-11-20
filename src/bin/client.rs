use bytes::Bytes;
use mini_redis::client;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

#[tokio::main]
async fn main() {
    // MP/SC channel that buffers up to 32 messages
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    // Spawn a manager task.
    // This task connects to the server and waits to route messages from other local tasks.
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Attempt to send the result.
                    // Oneshot produces a result immediately - no await.
                    // An Err result indicates that the receiver is closed. In that case, we can just continue.
                    let _ = resp.send(res);
                }
                Command::Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    // Attempt to send the result.
                    // Oneshot produces a result immediately - no await.
                    // An Err result indicates that the receiver is closed. In that case, we can just continue.
                    let _ = resp.send(res);
                }
            }
        }
    });

    let t1 = tokio::spawn(async move {
        // Construct the request (with channel so we can get the result back)
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };

        // Issue the request and print the result
        let res = issue_request(cmd, tx, resp_rx).await;
        println!("Got {:?} for 'get' command", res)
    });
    let t2 = tokio::spawn(async move {
        // Construct the request (with channel so we can get a status back)
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };

        // Issue the request and print the result
        let res = issue_request(cmd, tx2, resp_rx).await;
        println!("Got {:?} for 'set' command", res)
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}

async fn issue_request<T>(
    cmd: Command,
    tx: Sender<Command>,
    resp_rx: oneshot::Receiver<T>,
) -> Result<T, RecvError> {
    // Send the manager the request
    tx.send(cmd).await.unwrap();

    // Await the response
    resp_rx.await
}
