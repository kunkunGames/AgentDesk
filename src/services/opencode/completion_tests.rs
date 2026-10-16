//! Transport loss must not authorize successful completion or discard output.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::*;

pub(super) fn fixture(
    responses: Vec<(&'static str, u16, String)>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for (expected_request, status, body) in responses {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing {expected_request}");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fixture accept: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert!(line.starts_with(expected_request), "{line}");
            let mut content_length = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut request_body = vec![0; content_length];
            reader.read_exact(&mut request_body).unwrap();
            write!(
                socket,
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    (origin, server)
}

fn consume(
    input: String,
    origin: &str,
    cancel: Option<&CancelToken>,
) -> (Result<(), String>, Vec<StreamMessage>) {
    let (tx, rx) = mpsc::channel();
    let reader =
        BufReader::new(Box::new(std::io::Cursor::new(input.into_bytes())) as Box<dyn Read + Send>);
    let result = consume_sse_inner(reader, "active", &tx, cancel, origin, "");
    (result, rx.try_iter().collect())
}

fn output_event(text: &str) -> String {
    format!(
        "data: {}\n\n",
        serde_json::json!({
            "type": "message.completed",
            "properties": {
                "sessionID": "active",
                "message": {"role": "assistant", "parts": [{"type": "text", "text": text}]}
            }
        })
    )
}

fn assert_no_done(events: &[StreamMessage]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StreamMessage::Done { .. })),
        "{events:?}"
    );
}

#[test]
fn nonterminal_eof_with_valid_json_is_not_success_even_when_abort_acknowledged() {
    for status in [200, 500] {
        let (origin, server) =
            fixture(vec![("POST /session/active/abort ", status, "true".into())]);
        let text = r#"{"success":true,"checks_passed":true}"#;
        let (result, events) = consume(output_event(text), &origin, None);
        assert!(result.unwrap_err().contains("execution state is unknown"));
        assert_no_done(&events);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamMessage::Text { content } if content == text))
        );
        server.join().unwrap();
    }
}

#[test]
fn message_recovery_preserves_text_without_promoting_it_to_terminal() {
    let payload = serde_json::json!([{
        "info": {"id": "message-1", "role": "assistant"},
        "parts": [{"id": "part-1", "type": "text", "text": "recovered answer"}]
    }]);
    let (origin, server) = fixture(vec![
        ("GET /session/active/message ", 200, payload.to_string()),
        ("POST /session/active/abort ", 200, "true".into()),
    ]);
    let (result, events) = consume(String::new(), &origin, None);
    assert!(result.unwrap_err().contains("without a terminal event"));
    assert_no_done(&events);
    assert!(events.iter().any(
        |event| matches!(event, StreamMessage::Text { content } if content == "recovered answer")
    ));
    server.join().unwrap();
}

#[test]
fn manual_cancel_cannot_be_overridden_by_buffered_success_json() {
    let (origin, server) = fixture(vec![("POST /session/active/abort ", 200, "true".into())]);
    let cancel = CancelToken::new();
    cancel.publish_cancel("manual_cancel");
    let (result, events) = consume(output_event(r#"{"success":true}"#), &origin, Some(&cancel));
    assert!(result.unwrap_err().contains("cancelled"));
    assert_no_done(&events);
    server.join().unwrap();
}

#[test]
fn only_matching_session_terminal_event_authorizes_done() {
    for terminal in [
        "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"other\"}}\n\n",
        "data: {\"type\":\"session.idle\",\"properties\":{}}\n\n",
    ] {
        let (origin, server) = fixture(vec![("POST /session/active/abort ", 200, "true".into())]);
        let (result, events) = consume(output_event("answer") + terminal, &origin, None);
        assert!(result.is_err());
        assert_no_done(&events);
        server.join().unwrap();
    }

    let input = output_event("answer")
        + "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"active\"}}\n\n";
    let (result, events) = consume(input, "http://127.0.0.1:9", None);
    assert!(result.is_ok());
    assert_eq!(
        events
            .iter()
            .filter(
                |event| matches!(event, StreamMessage::Done { result, .. } if result == "answer")
            )
            .count(),
        1
    );
}
