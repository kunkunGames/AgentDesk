use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Shared broadcast sender for pushing events to all connected WS clients.
pub type BroadcastTx = Arc<broadcast::Sender<String>>;

pub fn new_broadcast() -> BroadcastTx {
    let (tx, _) = broadcast::channel::<String>(256);
    Arc::new(tx)
}

pub async fn ws_handler(ws: WebSocketUpgrade, State(tx): State<BroadcastTx>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, tx))
}

async fn handle_socket(socket: WebSocket, tx: BroadcastTx) {
    let (mut sender, mut receiver) = socket.split();

    // Send connected event
    let connected = json!({"type": "connected"}).to_string();
    if sender.send(Message::Text(connected.into())).await.is_err() {
        return;
    }

    let mut rx = tx.subscribe();

    // Forward broadcast messages to this client
    let mut send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => {
                            if sender.send(Message::Text(msg.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::debug!("[ws] Client lagged, skipped {n} messages");
                        }
                        Err(_) => break,
                    }
                }
                // Send ping every 30s to keep alive
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    if sender.send(Message::Ping(vec![].into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Consume incoming messages (ignore them, just detect disconnect)
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    });

    // Wait for either task to finish
    tokio::select! {
        _ = &mut send_task => { recv_task.abort(); }
        _ = &mut recv_task => { send_task.abort(); }
    }
}
