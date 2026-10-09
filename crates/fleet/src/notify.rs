//! `notify` — the fleet's event-driven wake injector (the P2 notification-wake, co-designed with
//! v-task-board; see ../../DESIGN.md).
//!
//! The board push-fires a best-effort HTTP POST to each agent's registered `webhook_url` for every inbox
//! event, carrying `recipient`, `type`, `task_id`, `event_seq`, … (never polling). Division of labor: the
//! board emits events; FLEET owns the managed session wake over a private Unix socket.
//! This remains one endpoint registered as each board-backed agent's `webhook_url`.
//! Each POST maps the event to a prompt accepted by the local managed session host — `[notification] task #<task_id>` for a task assignment, `[notification] message
//! #<event_seq>` for a direct message — so the agent reacts to the event instead of polling.

use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Map a board webhook event to the wake prompt to inject, or `None` to ignore the event. The wake model is
/// SUBSCRIPTION = NOTIFICATION (operator directive, #386): if an agent is subscribed to a target it is woken
/// on that target's activity — comments included — and unsubscribe is the opt-out. So an event wakes the
/// recipient's live session when either (a) it is a direct-delivery type they cannot be a passive bystander
/// of — a `task.assigned` (new work), a `message.direct` (addressed to them), a `channel.post` (a channel
/// they are a member of) — or (b) it is a `task.commented` on a target the recipient has a DIRECT
/// subscription to (`subscribed == true`). A firehose-only recipient (present via a whole-board subscription,
/// not a direct one) is NOT woken on a comment — it accrues in the durable inbox for the next poll, so a
/// board-wide coordinator isn't woken on every ticket. Presence churn and the agent's own actions never wake.
/// Pure — unit-tested.
///
/// `subscribed` is the board's per-recipient hint (#384, superseding the earlier `actionable` field): `true`
/// when the recipient has a DIRECT subscription to this event's target, `false` when they are present only
/// via the whole-board firehose. It is what lets `task.commented` wake a collaborator — two agents conversing
/// on a task must not wait out each other's poll interval (the operator's zero-polling mandate) — while a
/// firehose bystander still drops to poll. This deliberately retires the #215 anti-FYI-drain gate (the
/// operator accepts the noise trade, with unsubscribe + auto-subscribe as the noise control). `task.assigned`
/// / `message.direct` / `channel.post` stay wake-on-type: their delivery already IS the subscription (a DM in
/// particular has no subscribable target), and keeping them type-gated also preserves their wake for a
/// pre-#384 payload that carries no `subscribed` field yet.
///
/// `reactive_unaddressed` narrows the wake for a REACTIVE relay/responder agent (task_580). A reactive agent
/// is a member/subscriber of the channels it bridges and the tasks it watches, so under subscription =
/// notification (#384) it is woken on EVERY ambient post/comment there — a full harness tick it correctly
/// concludes "no action" on, during live chatter it is not addressed in (the frank + v-slack-bridge evidence).
/// This flag suppresses the two AMBIENT-CAPABLE subscription wakes (`channel.post` and `task.commented`) for a
/// recipient that is reactive AND that the board says this event does NOT address (not an @mention of it, not
/// a reply in a thread it is engaged in). It never gates `task.assigned` / `message.direct` — those inherently
/// address the recipient. It defaults false (a non-reactive agent, or a board not yet sending the
/// reactive/addressed hints), so this is additive and dormant — identical to the pre-task_580 wake set until
/// the board opts a reactive agent in. The board owns the complementary half (do not auto-subscribe a
/// bridge-owner to the channels it bridges); the two together cut the waste at both the wake layer and the
/// subscription source.
pub fn notification_prompt(
    event_type: &str,
    task_id: Option<i64>,
    event_seq: Option<i64>,
    channel_id: Option<i64>,
    subscribed: bool,
    reactive_unaddressed: bool,
    doc_ref: Option<&str>,
) -> Option<String> {
    match event_type {
        "task.assigned" => task_id.map(|id| format!("[notification] task #{id}")),
        // A document approval wakes its subscribers: the board delivers `document.approved` only to the doc's
        // subscribers, so delivery IS the subscription — wake-on-type, like a DM (task_1147). Render the
        // board's canonical typed ref verbatim as `[approval] doc_123`. The ref the board sends is ALREADY
        // `doc_<id>`, so NEVER prepend `doc ` — that doubles to `doc doc_123`.
        "document.approved" => doc_ref.map(|r| format!("[notification] [approval] {r}")),
        // A comment on a document the recipient DIRECTLY subscribes to (task_1269): the human-review case —
        // a reviewer's comment on a doc under review must wake its owner/assignee, not sit for the next poll.
        // The board delivers `document.comment` to the doc's subscribers (owner/assignee land in the
        // `subscribed=true` set), so it is subscription=notification exactly like `task.commented`, only keyed
        // on the doc `ref` (document events carry `ref`/`doc_<id>`, not a task_id). A firehose-only recipient
        // stays `subscribed=false` and accrues for poll; a reactive-unaddressed recipient is suppressed as
        // elsewhere. Without this arm a `document.comment` frame — which the board DOES push — fell through to
        // `_ => None` and dropped until the next poll (the ~14-min lag cameron reported).
        "document.comment" if subscribed && !reactive_unaddressed => {
            doc_ref.map(|r| format!("[notification] comment on {r}"))
        }
        "message.direct" => event_seq.map(|seq| format!("[notification] message #{seq}")),
        // A reactive relay/responder member of this channel is not addressed by this post (task_580): suppress
        // the wake so ambient channel chatter it would conclude "no action" on does not cost a harness tick. It
        // still accrues in the durable inbox for the next poll, so nothing is lost — only the wasted wake is cut.
        "channel.post" if reactive_unaddressed => None,
        // A post to a channel the agent is a member of: the board only delivers `channel.post` to a channel's
        // subscribers/members (minus the actor), so delivery IS the subscription filter — the agent joined
        // because it cares (e.g. a `deploys`-channel waiter, #171). Wake-on-type for the same reason as above.
        "channel.post" => channel_id.map(|id| format!("[notification] channel #{id}")),
        // A comment wakes when the recipient has a DIRECT subscription to the target (#384, subscription =
        // notification): the collaboration case the zero-polling mandate targets. A firehose-only recipient
        // stays `subscribed=false` and accrues for poll (so a board-wide coordinator isn't woken per ticket).
        // A reactive recipient this comment does not address is suppressed for the same reason as channel.post.
        "task.commented" if subscribed && !reactive_unaddressed => {
            task_id.map(|id| format!("[notification] comment on task #{id}"))
        }
        // A fresh task CREATED in a target the recipient DIRECTLY subscribes to wakes it (task_1104): the
        // intake-triage case. board-triage subscribes to the intake projects (e.g. the uncategorized front
        // door), so a fresh operator-filed create wakes it at once instead of waiting out its ~30-min idle poll
        // — the "why did this take so long to get triaged" gap. Gated on `subscribed` for the same reason as a
        // comment: a direct project subscriber (the triager) wakes, a firehose-only board-wide coordinator does
        // not (it accrues for poll, so a create never loop-wakes a whole-board watcher). A reactive recipient
        // the create does not address is suppressed identically. An assigned-at-create task still wakes its
        // direct subscriber here (harmless: the triager sees it is already owned and moves on in one tick) — not
        // gating on assignment keeps this free of any new payload field and can never MISS a triage wake.
        "task.created" if subscribed && !reactive_unaddressed => {
            task_id.map(|id| format!("[notification] new task #{id}"))
        }
        // A firehose-only task.commented / task.created, plus task.status_changed / task.updated /
        // presence.updated and every other type, are INFORMATIONAL here — they accrue for the next poll and
        // never inject a wake.
        _ => None,
    }
}

/// Extract `(recipient, wake-prompt)` from a webhook payload, or `None` if the event is not actionable or
/// is missing the recipient/type. Pure — unit-tested.
pub fn payload_to_wake(v: &Value) -> Option<(String, String)> {
    let recipient = v.get("recipient").and_then(Value::as_str)?;
    let event_type = v.get("type").and_then(Value::as_str)?;
    let task_id = v.get("task_id").and_then(Value::as_i64);
    let event_seq = v.get("event_seq").and_then(Value::as_i64);
    let channel_id = v.get("channel_id").and_then(Value::as_i64);
    // #384: the board's per-recipient subscription hint (true = direct subscription to the target). Absent on
    // a pre-#384 payload -> `false`, which leaves every type-gated wake (assign/dm/channel) intact and simply
    // keeps a comment dropping-to-poll.
    let subscribed = v
        .get("subscribed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // task_580: the two per-recipient hints that gate a REACTIVE agent's ambient wakes. `reactive` (board
    // metadata) marks a relay/responder; `addressed` is whether THIS event addresses it (an @mention, a reply
    // in a thread it is engaged in). We suppress only when the recipient is reactive AND the board EXPLICITLY
    // says the event does not address it (`addressed == Some(false)`). An absent `addressed` (a board that does
    // not yet send the signal) stays `None` -> NOT suppressed, so enabling `reactive` without the addressing
    // signal can never silently drop a legitimate addressed wake — it only narrows once the board sends both.
    let reactive = v.get("reactive").and_then(Value::as_bool).unwrap_or(false);
    let addressed = v.get("addressed").and_then(Value::as_bool);
    let reactive_unaddressed = reactive && addressed == Some(false);
    // The board's canonical typed id for a document event (task_1147): top-level `ref`, falling back to
    // `data.ref` depending on how the event body is shaped. Rendered verbatim (already `doc_<id>`).
    let doc_ref = v.get("ref").and_then(Value::as_str).or_else(|| {
        v.get("data")
            .and_then(|d| d.get("ref"))
            .and_then(Value::as_str)
    });
    let prompt = notification_prompt(
        event_type,
        task_id,
        event_seq,
        channel_id,
        subscribed,
        reactive_unaddressed,
        doc_ref,
    )?;
    Some((recipient.to_string(), prompt))
}

/// Send a bounded managed-session control request; success means host acceptance.
pub fn session_control(
    state_root: &std::path::Path,
    agent: &str,
    request: Value,
) -> Result<(), String> {
    use std::io::{BufRead, Read, Write};
    use std::os::unix::net::UnixStream;
    if agent.is_empty()
        || agent.len() > 64
        || !agent
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err("invalid agent identifier".into());
    }
    let mut encoded = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
    if encoded.len() > 64 * 1024 {
        return Err("session control request exceeds 64KiB".into());
    }
    encoded.push(b'\n');
    let socket = state_root.join(agent).join("control.sock");
    let mut stream =
        UnixStream::connect(socket).map_err(|e| format!("connect managed session: {e}"))?;
    let timeout = Some(std::time::Duration::from_secs(2));
    stream
        .set_read_timeout(timeout)
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(timeout)
        .map_err(|e| e.to_string())?;
    stream
        .write_all(&encoded)
        .map_err(|e| format!("write managed session: {e}"))?;
    let mut response = String::new();
    std::io::BufReader::new(stream)
        .take(4096)
        .read_line(&mut response)
        .map_err(|e| format!("read managed session: {e}"))?;
    if !response.ends_with('\n') {
        return Err("incomplete managed session acknowledgement".into());
    }
    let response: Value = serde_json::from_str(&response)
        .map_err(|_| "invalid managed session acknowledgement".to_string())?;
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err("managed session rejected control request".into());
    }
    Ok(())
}

pub fn session_inject(state_root: &std::path::Path, agent: &str, text: &str) -> Result<(), String> {
    session_control(state_root, agent, serde_json::json!({"prompt":text}))
}

pub fn session_interrupt(state_root: &std::path::Path, agent: &str) -> Result<(), String> {
    session_control(state_root, agent, serde_json::json!({"interrupt":true}))
}

/// Existing callers retain their host/session routing signature; delivery uses the managed local socket.
pub fn tmux_inject(_session: &str, agent: &str, text: &str) -> Result<(), String> {
    session_inject(&crate::codex_state_root(), agent, text)
}

/// How the notifier handles one incoming request. The board POSTs webhook events; a supervisor (a systemd
/// service health check, `fleet board-health`, a monitor) probes liveness with a plain `GET`. Classifying up
/// front lets a liveness probe get a clean `200` without being read as a webhook — which would log a spurious
/// "unparseable body" line and is indistinguishable from a real event.
#[derive(Debug, PartialEq, Eq)]
enum Incoming {
    /// A liveness probe (`GET /health`, `/healthz`, or `/`) — answer `200` and read nothing.
    HealthProbe,
    /// A board webhook event — read the body and map it to a wake (the default for any other request).
    Webhook,
}

/// Classify an incoming request by method + path (query string ignored): a `GET` to `/health`, `/healthz`, or
/// `/` is a liveness probe; every other request is a webhook. Pure — unit-tested.
fn classify_request(method: &str, url: &str) -> Incoming {
    let path = url.split('?').next().unwrap_or(url);
    if method.eq_ignore_ascii_case("GET") && matches!(path, "/health" | "/healthz" | "/") {
        Incoming::HealthProbe
    } else {
        Incoming::Webhook
    }
}

/// Parse an HTTP/1.x request line (`"METHOD SP REQUEST-URI SP HTTP-VERSION"`) into `(method, path)`. `None`
/// if the line doesn't have at least two whitespace-separated tokens (the HTTP-version token, if present, is
/// ignored — nothing here branches on HTTP/1.0 vs 1.1). Pure — unit-tested.
fn parse_request_line(line: &str) -> Option<(String, String)> {
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    Some((method.to_string(), path.to_string()))
}

/// Case-insensitive `Content-Length` lookup from a raw header block (one `Name: value` per line, request
/// line already stripped). Defaults to 0 when absent or unparseable — the body is empty or the sender is
/// malformed either way, and 0 just means "read no body" rather than hang waiting for one. Pure — unit-tested.
fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0)
}

/// Find the `\r\n\r\n` header/body boundary in a raw byte buffer — the index of its first byte, or `None` if
/// the buffer doesn't contain one yet (more reads are needed). Pure — unit-tested.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// A header block this large (no `\r\n\r\n` found yet) is not a real HTTP client — stop reading rather than
/// grow the buffer without bound.
const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Cap a claimed `Content-Length` so a malformed/hostile value can't force an unbounded body read — every
/// real board webhook payload (a JSON event envelope) is tiny next to this.
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Read one HTTP/1.x request off `stream`: the request line, headers (kept only for `Content-Length`), and
/// exactly that many body bytes. `Content-Length` only — every sender here (the board's webhook POST, a
/// liveness `GET`) either sets it or has no body; chunked transfer-encoding is unsupported (the board never
/// sends it, so supporting it would be untested dead code). `Err` on a malformed request line or a read that
/// ends before the headers/body complete. Generic over `AsyncRead` so it is unit-testable against an in-memory
/// `tokio::io::duplex` pair, with no real socket needed.
async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<(String, String, String)> {
    use std::io::{Error, ErrorKind};
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "fleet notify: request headers exceeded 64KiB",
            ));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "fleet notify: connection closed before headers completed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.lines();
    let (method, path) = lines.next().and_then(parse_request_line).ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "fleet notify: malformed request line",
        )
    })?;
    let headers = lines.collect::<Vec<_>>().join("\n");
    let content_length = parse_content_length(&headers).min(MAX_BODY_BYTES);
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "fleet notify: connection closed before body completed",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok((method, path, String::from_utf8_lossy(&body).into_owned()))
}

/// Handle one accepted connection end to end: read the request, ack it `200` immediately (best-effort — a
/// wake is never worth wedging the endpoint the board POSTs to), then — for a webhook, not a liveness probe
/// — parse the body and inject the wake. `tmux_inject` shells out and sleeps (~600ms per event); running it
/// via `spawn_blocking` keeps that off this connection's async task so it never delays accepting the board's
/// next webhook POST or a concurrent liveness probe (the single blocking-accept-loop this replaced could not
/// overlap those at all).
/// Default escalation threshold N (doc_3410): the drift directive is a graceful nudge for the first N
/// consecutive flagged ticks, then escalates to a hard stop-and-return.
const DRIFT_ESCALATE_N: u32 = 2;

/// Best-effort fleet-detected drift directive to prepend to a wake prompt (task_1325 Stage-1 slice 2b, the
/// enforced tier of doc_3410). On each wake it reads the recipient's board presence and actionable open-task
/// count (sync board.rs, called from inside `spawn_blocking` so it never touches the async executor),
/// advances the persisted per-agent `DriftState`, and renders the correction directive when the
/// switch-do-not-idle condition (task_736) flags: a graceful `Directive` for the first N ticks, then a hard
/// stop-and-return `Escalate`. Any board or store error yields `None` — a drift check must never block or
/// corrupt the wake itself.
fn drift_wake_prefix(hub_root: &std::path::Path, agent: &str) -> Option<String> {
    let board = crate::board::Board::connect_timeout(std::time::Duration::from_millis(100)).ok()?;
    let presence = board
        .get_agent(agent)
        .ok()?
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let open = board.open_task_count(agent).ok()?;
    let signal = crate::drift::switch_do_not_idle_signal(&presence, open);
    let store = crate::drift::DriftStore::new(hub_root);
    let mut state = store.load(agent);
    let action = state.observe(signal, DRIFT_ESCALATE_N);
    if let Err(e) = store.save(agent, &state) {
        eprintln!("drift: persist {agent} failed: {e}");
    }
    crate::drift::directive_text(action)
}

/// A board event identity is supplied by the board, never derived from prompt text.
fn payload_to_control(event: &Value) -> Option<(String, Value)> {
    let (recipient, mut request) =
        if event.get("type").and_then(Value::as_str) == Some("session.abort") {
            (
                event.get("recipient")?.as_str()?.to_string(),
                serde_json::json!({"interrupt":true}),
            )
        } else {
            let (recipient, prompt) = payload_to_wake(event)?;
            (recipient, serde_json::json!({"prompt":prompt}))
        };
    if let Some(sequence) = event.get("event_seq").and_then(Value::as_i64) {
        request["event_id"] = serde_json::json!(format!("{recipient}:{sequence}"));
    }
    Some((recipient, request))
}

async fn respond(stream: &mut TcpStream, status: &str) {
    let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.write_all(response.as_bytes()),
    )
    .await;
}

/// Report the serving binary's identity without probing or waking a session.
async fn respond_health(stream: &mut TcpStream) {
    let body = serde_json::json!({
        "service": "fleet-notify",
        "status": "ok",
        "check": "liveness",
        "version": env!("CARGO_PKG_VERSION"),
        "build_revision": env!("FLEET_BUILD_REV"),
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.write_all(response.as_bytes()),
    )
    .await;
}

async fn handle_connection(stream: TcpStream, hub_root: Arc<std::path::Path>) {
    handle_connection_at(
        stream,
        hub_root,
        crate::codex_state_root(),
        true,
        std::time::Duration::from_secs(2),
    )
    .await;
}

async fn handle_connection_at(
    mut stream: TcpStream,
    hub_root: Arc<std::path::Path>,
    state_root: std::path::PathBuf,
    with_drift: bool,
    delivery_timeout: std::time::Duration,
) {
    let (method, path, body) =
        match tokio::time::timeout(std::time::Duration::from_secs(2), read_request(&mut stream))
            .await
        {
            Ok(Ok(parts)) => parts,
            _ => {
                respond(&mut stream, "400 Bad Request").await;
                return;
            }
        };
    if classify_request(&method, &path) == Incoming::HealthProbe {
        respond_health(&mut stream).await;
        return;
    }
    let event: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(_) => {
            respond(&mut stream, "400 Bad Request").await;
            return;
        }
    };
    let Some((recipient, mut request)) = payload_to_control(&event) else {
        // Intentional policy suppression is successful handling, not failed delivery.
        respond(&mut stream, "204 No Content").await;
        return;
    };
    if with_drift && request.get("prompt").is_some() {
        let agent = recipient.clone();
        let prefix = tokio::task::spawn_blocking(move || drift_wake_prefix(&hub_root, &agent));
        // Drift is advisory and must not hold delivery behind an unavailable board.
        if let Ok(Ok(Some(prefix))) =
            tokio::time::timeout(std::time::Duration::from_millis(250), prefix).await
        {
            request["prompt"] = serde_json::json!(format!(
                "{prefix}\n\n{}",
                request["prompt"].as_str().unwrap_or_default()
            ));
        }
    }
    // Only board-issued identities permit retry after an ambiguous acknowledgement.
    // The host durably deduplicates these IDs before acknowledging acceptance.
    let attempts = if request.get("event_id").is_some() {
        2
    } else {
        1
    };
    for attempt in 0..attempts {
        let root = state_root.clone();
        let agent = recipient.clone();
        let payload = request.clone();
        let delivery = tokio::task::spawn_blocking(move || session_control(&root, &agent, payload));
        if matches!(
            tokio::time::timeout(delivery_timeout, delivery).await,
            Ok(Ok(Ok(())))
        ) {
            respond(&mut stream, "200 OK").await;
            return;
        }
        if attempt + 1 < attempts {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    respond(&mut stream, "503 Service Unavailable").await;
}

/// Run the notifier: bind a local HTTP endpoint and, for each board webhook POST, inject the wake prompt
/// into the recipient agent's managed Codex session. Blocks (a long-running daemon) — but each connection
/// runs on its own tokio task ([`handle_connection`]), so a slow wake injection on one event never stalls the
/// board's next webhook POST or a liveness probe arriving concurrently. Actionable events receive `200` only after host acknowledgement; failures return `503`.
/// Malformed input returns `400`; intentionally ignored events return `204`. The board currently does not
/// retry webhook failures, so its durable inbox remains the fallback. A
/// supervisor liveness-probes the daemon with a `GET` to `/health` (see [`classify_request`]).
pub fn serve(port: u16, session: &str, hub_root: &std::path::Path) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("fleet notify: build tokio runtime: {e}"))?;
    rt.block_on(serve_async(port, session, hub_root))
}

async fn serve_async(port: u16, session: &str, hub_root: &std::path::Path) -> Result<(), String> {
    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|e| format!("fleet notify: bind 127.0.0.1:{port}: {e}"))?;
    eprintln!(
        "fleet notify: listening on http://127.0.0.1:{port} — waking session '{session}' on board webhooks (GET /health for liveness)"
    );
    let hub_root: Arc<std::path::Path> = Arc::from(hub_root);
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("fleet notify: accept: {e}"))?;
        tokio::spawn(handle_connection(stream, hub_root.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn request_without_host(request: &str) -> String {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let path = root.path().to_path_buf();
        let task = tokio::spawn(async move {
            handle_connection_at(
                server,
                Arc::from(path.as_path()),
                path.clone(),
                false,
                std::time::Duration::from_secs(1),
            )
            .await;
        });
        client.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            client.read_to_string(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        task.await.unwrap();
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        response
    }

    #[tokio::test]
    async fn health_reports_build_and_liveness_scope_without_a_session() {
        for path in ["/health", "/healthz", "/", "/health?probe=version"] {
            let response = request_without_host(&format!("GET {path} HTTP/1.1\r\n\r\n")).await;
            let (headers, body) = response.split_once("\r\n\r\n").unwrap();
            assert!(headers.starts_with("HTTP/1.1 200 OK"));
            assert!(headers.contains("Content-Type: application/json\r\n"));
            assert!(headers.contains("Cache-Control: no-store\r\n"));
            assert_eq!(parse_content_length(headers), body.len());
            assert_eq!(
                serde_json::from_str::<Value>(body).unwrap(),
                serde_json::json!({
                    "service": "fleet-notify",
                    "status": "ok",
                    "check": "liveness",
                    "version": env!("CARGO_PKG_VERSION"),
                    "build_revision": env!("FLEET_BUILD_REV"),
                })
            );
        }
    }

    #[tokio::test]
    async fn healthy_notifier_still_rejects_undeliverable_or_malformed_wakes() {
        let event = r#"{"recipient":"missing","type":"task.assigned","task_id":5}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n{event}",
            event.len()
        );
        let response = request_without_host(&request).await;
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert_eq!(response.split_once("\r\n\r\n").unwrap().1, "");
        let response =
            request_without_host("POST /health HTTP/1.1\r\nContent-Length: 1\r\n\r\n{").await;
        assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
    }

    #[test]
    fn board_event_identity_survives_duplicate_delivery_and_interrupt() {
        let event = serde_json::json!({"recipient":"worker","type":"task.assigned","task_id":5,"event_seq":91});
        let first = payload_to_control(&event).unwrap();
        assert_eq!(first, payload_to_control(&event).unwrap());
        assert_eq!(first.1["event_id"], "worker:91");
        let interrupt = payload_to_control(
            &serde_json::json!({"recipient":"worker","type":"session.abort","event_seq":92}),
        )
        .unwrap();
        assert_eq!(
            interrupt.1,
            serde_json::json!({"interrupt":true,"event_id":"worker:92"})
        );
        let unsequenced = payload_to_control(
            &serde_json::json!({"recipient":"worker","type":"task.assigned","task_id":5}),
        )
        .unwrap();
        assert!(unsequenced.1.get("event_id").is_none());
    }

    async fn http_delivery_case(
        accepted: bool,
        retry_succeeds: bool,
        host_delay: std::time::Duration,
        deadline: std::time::Duration,
    ) -> String {
        use std::io::{BufRead, Write};
        let root = std::path::PathBuf::from("/tmp").join(format!(
            "notify-http-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("worker")).unwrap();
        let host =
            std::os::unix::net::UnixListener::bind(root.join("worker/control.sock")).unwrap();
        let host = std::thread::spawn(move || {
            for attempt in 0..if retry_succeeds { 2 } else { 1 } {
                let (mut socket, _) = host.accept().unwrap();
                let mut line = String::new();
                std::io::BufReader::new(socket.try_clone().unwrap())
                    .read_line(&mut line)
                    .unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&line).unwrap()["event_id"],
                    "worker:7"
                );
                std::thread::sleep(host_delay);
                let accepted = accepted || (retry_succeeds && attempt == 1);
                let response = format!("{{\"ok\":{accepted}}}\n");
                let _ = socket.write_all(response.as_bytes());
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let root_for_task = root.clone();
        let task = tokio::spawn(async move {
            handle_connection_at(
                server,
                Arc::from(root_for_task.as_path()),
                root_for_task.clone(),
                false,
                deadline,
            )
            .await;
        });
        let body = r#"{"recipient":"worker","type":"task.assigned","task_id":5,"event_seq":7}"#;
        client
            .write_all(
                format!(
                    "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        task.await.unwrap();
        tokio::task::spawn_blocking(move || host.join().unwrap())
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
        response
    }

    #[tokio::test]
    async fn http_acknowledges_only_host_acceptance() {
        let response = http_delivery_case(
            true,
            false,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(1),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let response = http_delivery_case(
            false,
            false,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(1),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 503"));
    }

    #[tokio::test]
    async fn retries_once_with_original_identity() {
        let response = http_delivery_case(
            false,
            true,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(1),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"));
    }

    #[tokio::test]
    async fn slow_host_is_reported_as_retriable_failure() {
        let response = http_delivery_case(
            true,
            false,
            std::time::Duration::from_millis(150),
            std::time::Duration::from_millis(25),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 503"));
    }

    #[test]
    fn managed_wake_sends_one_json_frame_and_requires_acceptance() {
        use std::io::{BufRead, Write};
        let root = std::path::PathBuf::from("/tmp").join(format!(
            "fleet-notify-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("worker")).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(root.join("worker/control.sock")).unwrap();
        let host = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            std::io::BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap(),
                serde_json::json!({"prompt":"line one\nline two"})
            );
            stream.write_all(b"{\"ok\":true}\n").unwrap();
        });
        session_inject(&root, "worker", "line one\nline two").unwrap();
        host.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn managed_control_rejects_path_escape_and_absent_host() {
        assert!(session_inject(std::path::Path::new("/tmp"), "../escape", "wake").is_err());
        assert!(session_interrupt(std::path::Path::new("/no-such-fleet-root"), "worker").is_err());
    }

    #[test]
    fn managed_interrupt_propagates_host_rejection() {
        use std::io::{BufRead, Write};
        let root = std::path::PathBuf::from("/tmp").join(format!(
            "fleet-interrupt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("worker")).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(root.join("worker/control.sock")).unwrap();
        let host = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            std::io::BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap(),
                serde_json::json!({"interrupt":true})
            );
            stream
                .write_all(b"{\"ok\":false,\"error\":\"busy\"}\n")
                .unwrap();
        });
        assert!(
            session_interrupt(&root, "worker")
                .unwrap_err()
                .contains("rejected")
        );
        host.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn classify_request_routes_get_health_paths_to_a_probe_else_webhook() {
        // A GET to a health path (query string ignored) is a liveness probe.
        assert_eq!(classify_request("GET", "/health"), Incoming::HealthProbe);
        assert_eq!(classify_request("GET", "/healthz"), Incoming::HealthProbe);
        assert_eq!(classify_request("GET", "/"), Incoming::HealthProbe);
        assert_eq!(
            classify_request("GET", "/health?probe=1"),
            Incoming::HealthProbe
        );
        assert_eq!(
            classify_request("get", "/health"),
            Incoming::HealthProbe,
            "method match is case-insensitive"
        );
        // The board POSTs webhooks — never a probe, even to a health path.
        assert_eq!(classify_request("POST", "/"), Incoming::Webhook);
        assert_eq!(classify_request("POST", "/health"), Incoming::Webhook);
        // A non-health GET is treated as a webhook (the default), not a probe.
        assert_eq!(classify_request("GET", "/webhook"), Incoming::Webhook);
        assert_eq!(classify_request("GET", "/events"), Incoming::Webhook);
    }

    #[test]
    fn parse_request_line_splits_method_and_path_and_ignores_the_version_token() {
        assert_eq!(
            parse_request_line("POST /webhook HTTP/1.1"),
            Some(("POST".into(), "/webhook".into()))
        );
        assert_eq!(
            parse_request_line("GET / HTTP/1.0"),
            Some(("GET".into(), "/".into()))
        );
        // The HTTP-version token is optional for parsing purposes — two tokens are enough.
        assert_eq!(
            parse_request_line("GET /health"),
            Some(("GET".into(), "/health".into()))
        );
        assert_eq!(
            parse_request_line(""),
            None,
            "an empty line has no method token"
        );
        assert_eq!(
            parse_request_line("GET"),
            None,
            "a lone method has no path token"
        );
    }

    #[test]
    fn parse_content_length_is_case_insensitive_and_defaults_to_zero() {
        assert_eq!(parse_content_length("Content-Length: 42\r\nOther: x"), 42);
        assert_eq!(
            parse_content_length("content-length: 7"),
            7,
            "header name match is case-insensitive"
        );
        assert_eq!(parse_content_length("CONTENT-LENGTH: 3"), 3);
        assert_eq!(
            parse_content_length("Other: x"),
            0,
            "absent header defaults to 0"
        );
        assert_eq!(
            parse_content_length("Content-Length: not-a-number"),
            0,
            "unparseable value defaults to 0"
        );
        assert_eq!(parse_content_length(""), 0);
    }

    #[test]
    fn find_header_end_locates_the_crlf_crlf_boundary() {
        assert_eq!(find_header_end(b"GET / HTTP/1.1\r\n\r\nbody"), Some(14));
        assert_eq!(
            find_header_end(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"),
            Some(23)
        );
        assert_eq!(
            find_header_end(b"GET / HTTP/1.1\r\nHost: x"),
            None,
            "no body separator read yet"
        );
    }

    #[tokio::test]
    async fn read_request_parses_a_post_with_a_body_by_content_length() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client
            .write_all(b"POST /webhook HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\n\r\n{\"a\":true}EXTRA")
            .await
            .unwrap();
        let (method, path, body) = read_request(&mut server).await.unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/webhook");
        // Exactly Content-Length bytes — "EXTRA" (the next pipelined request, if any) is left unread.
        assert_eq!(body, "{\"a\":true}");
        drop(client);
    }

    #[tokio::test]
    async fn read_request_parses_a_get_with_no_body() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        drop(client); // close the write end so a short/absent body never hangs the read
        let (method, path, body) = read_request(&mut server).await.unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/health");
        assert_eq!(body, "");
    }

    #[tokio::test]
    async fn read_request_errors_when_the_connection_closes_before_headers_complete() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client
            .write_all(b"GET /health HTTP/1.1\r\nHost: x")
            .await
            .unwrap();
        drop(client);
        assert!(read_request(&mut server).await.is_err());
    }

    #[test]
    fn prompt_wakes_direct_delivery_types_and_subscribed_comments_not_firehose() {
        // Direct-delivery types wake on TYPE — the recipient can't be a passive bystander of them — so they
        // wake regardless of the `subscribed` hint (here `false`). A DM in particular has no subscribable
        // target, so it MUST stay type-gated.
        assert_eq!(
            notification_prompt("task.assigned", Some(42), None, None, false, false, None)
                .as_deref(),
            Some("[notification] task #42")
        );
        assert_eq!(
            notification_prompt("message.direct", None, Some(438), None, false, false, None)
                .as_deref(),
            Some("[notification] message #438")
        );
        // A post to a channel the agent is a member of (delivery = membership; #171) wakes on type.
        assert_eq!(
            notification_prompt("channel.post", None, Some(9), Some(7), false, false, None)
                .as_deref(),
            Some("[notification] channel #7")
        );
        assert_eq!(
            notification_prompt("channel.post", None, Some(9), None, false, false, None),
            None,
            "no channel_id → can't form a prompt"
        );
        // A comment on a target the recipient DIRECTLY subscribes to wakes (#384, subscription = notification)
        // — the collaboration case the zero-polling mandate targets.
        assert_eq!(
            notification_prompt("task.commented", Some(42), Some(9), None, true, false, None)
                .as_deref(),
            Some("[notification] comment on task #42")
        );
        assert_eq!(
            notification_prompt("task.commented", None, Some(9), None, true, false, None),
            None,
            "no task_id → can't form a prompt even when subscribed"
        );
        // NO wake (accrues for the next poll): a comment from a FIREHOSE-only recipient (no direct
        // subscription) must not loop-wake a board-wide coordinator on every ticket; a status change never
        // wakes at all.
        assert_eq!(
            notification_prompt(
                "task.commented",
                Some(42),
                Some(9),
                None,
                false,
                false,
                None
            ),
            None,
            "a firehose-only comment accrues for poll, never wakes"
        );
        assert_eq!(
            notification_prompt(
                "task.status_changed",
                Some(42),
                Some(9),
                None,
                true,
                false,
                None
            ),
            None,
            "status change never wakes, even when subscribed"
        );
        // an assignment without a task_id, or a DM without a seq, can't form a prompt
        assert_eq!(
            notification_prompt("task.assigned", None, Some(1), None, false, false, None),
            None
        );
        assert_eq!(
            notification_prompt("message.direct", Some(1), None, None, false, false, None),
            None
        );
        // presence churn and other event types are ignored
        assert_eq!(
            notification_prompt("presence.updated", None, Some(3), None, true, false, None),
            None
        );
        assert_eq!(
            notification_prompt("task.updated", Some(5), None, None, true, false, None),
            None
        );
    }

    #[test]
    fn task_created_wakes_a_direct_subscriber_not_a_firehose_recipient() {
        // task_1104: a fresh create in a target the recipient DIRECTLY subscribes to wakes it (the intake-triage
        // case — board-triage subscribes to the intake projects and must triage a fresh create at once, not wait
        // out its idle poll). Gated on `subscribed` exactly like task.commented.
        assert_eq!(
            notification_prompt("task.created", Some(1182), Some(9), None, true, false, None)
                .as_deref(),
            Some("[notification] new task #1182"),
            "a create on a directly-subscribed target wakes the triager"
        );
        // A firehose-only recipient (subscribed=false) does NOT wake on a create — it accrues for poll, so a
        // create never loop-wakes a board-wide coordinator present via the whole-board firehose.
        assert_eq!(
            notification_prompt(
                "task.created",
                Some(1182),
                Some(9),
                None,
                false,
                false,
                None
            ),
            None,
            "a firehose-only create accrues for poll, never wakes"
        );
        // A reactive recipient the create does not address is suppressed, same as an ambient channel.post/comment.
        assert_eq!(
            notification_prompt("task.created", Some(1182), Some(9), None, true, true, None),
            None,
            "a reactive subscriber a create does not address is not woken (task_580 gate applies)"
        );
        // No task_id → can't form a prompt even when subscribed.
        assert_eq!(
            notification_prompt("task.created", None, Some(9), None, true, false, None),
            None
        );
        // End-to-end through payload_to_wake: a subscribed create wakes the intake triager; a firehose one does not.
        let subbed = serde_json::json!({"recipient":"board-triage","type":"task.created","task_id":1182,"event_seq":9,"subscribed":true});
        assert_eq!(
            payload_to_wake(&subbed),
            Some((
                "board-triage".into(),
                "[notification] new task #1182".into()
            ))
        );
        let firehose = serde_json::json!({"recipient":"board-pm","type":"task.created","task_id":1182,"event_seq":9,"subscribed":false});
        assert_eq!(
            payload_to_wake(&firehose),
            None,
            "a firehose create does not wake"
        );
        // A pre-#384 payload with no `subscribed` field defaults false → drops to poll (never a spurious wake).
        let no_hint =
            serde_json::json!({"recipient":"x","type":"task.created","task_id":1182,"event_seq":9});
        assert_eq!(payload_to_wake(&no_hint), None);
    }

    #[test]
    fn document_comment_wakes_a_subscribed_doc_owner_keyed_on_the_doc_ref() {
        // task_1269: a reviewer's comment on a doc under review wakes its owner/assignee (the subscribed=true
        // set), keyed on the doc `ref` since document events carry no task_id. Mirrors the task.commented gate.
        assert_eq!(
            notification_prompt(
                "document.comment",
                None,
                Some(9),
                None,
                true,
                false,
                Some("doc_3394")
            )
            .as_deref(),
            Some("[notification] comment on doc_3394"),
            "a comment on a directly-subscribed doc wakes its owner, with the verbatim doc ref"
        );
        // A firehose-only recipient (subscribed=false) accrues for poll — never a per-comment firehose wake.
        assert_eq!(
            notification_prompt(
                "document.comment",
                None,
                Some(9),
                None,
                false,
                false,
                Some("doc_3394")
            ),
            None
        );
        // A reactive recipient the comment does not address is suppressed (task_580 gate).
        assert_eq!(
            notification_prompt(
                "document.comment",
                None,
                Some(9),
                None,
                true,
                true,
                Some("doc_3394")
            ),
            None
        );
        // No doc ref → no prompt even when subscribed.
        assert_eq!(
            notification_prompt("document.comment", None, Some(9), None, true, false, None),
            None
        );
        // End-to-end through payload_to_wake with the ref under `data.ref` (the board's shape).
        let subbed = serde_json::json!({"recipient":"v-fleet-tooling","type":"document.comment","event_seq":9,"subscribed":true,"data":{"ref":"doc_3394"}});
        assert_eq!(
            payload_to_wake(&subbed),
            Some((
                "v-fleet-tooling".into(),
                "[notification] comment on doc_3394".into()
            ))
        );
    }

    #[test]
    fn reactive_unaddressed_suppresses_only_ambient_subscription_wakes() {
        // task_580: a reactive relay/responder is woken on every ambient channel.post / task.commented it is a
        // member/subscriber of. The reactive_unaddressed gate suppresses exactly those two ambient-capable
        // subscription wakes — and ONLY those — when the recipient is reactive and the event does not address it.
        // channel.post: woken when not gated, SUPPRESSED when reactive_unaddressed.
        assert_eq!(
            notification_prompt("channel.post", None, Some(9), Some(7), false, false, None)
                .as_deref(),
            Some("[notification] channel #7"),
            "ungated channel.post still wakes a member"
        );
        assert_eq!(
            notification_prompt("channel.post", None, Some(9), Some(7), false, true, None),
            None,
            "a reactive member not addressed by the post is NOT woken (task_580)"
        );
        // task.commented: a direct-subscribed comment wakes, but is SUPPRESSED when reactive_unaddressed.
        assert_eq!(
            notification_prompt("task.commented", Some(42), Some(9), None, true, false, None)
                .as_deref(),
            Some("[notification] comment on task #42"),
            "ungated subscribed comment still wakes"
        );
        assert_eq!(
            notification_prompt("task.commented", Some(42), Some(9), None, true, true, None),
            None,
            "a reactive subscriber not addressed by the comment is NOT woken (task_580)"
        );
        // The gate NEVER touches the inherently-addressed direct-delivery types: an assignment or a DM to a
        // reactive agent still wakes even when reactive_unaddressed is set (those address it by definition).
        assert_eq!(
            notification_prompt("task.assigned", Some(42), None, None, false, true, None)
                .as_deref(),
            Some("[notification] task #42"),
            "an assignment addresses the recipient — reactive gate must not suppress it"
        );
        assert_eq!(
            notification_prompt("message.direct", None, Some(438), None, false, true, None)
                .as_deref(),
            Some("[notification] message #438"),
            "a DM addresses the recipient — reactive gate must not suppress it"
        );
    }

    #[test]
    fn payload_reactive_gate_requires_reactive_and_explicit_not_addressed() {
        // The suppression engages only when the payload says reactive=true AND addressed=false. A reactive post
        // the board does NOT mark addressed-either-way (addressed absent) must still wake — so enabling reactive
        // without the addressing signal never silently drops a legitimate wake; it only narrows once both land.
        let base = |extra: serde_json::Value| {
            let mut v = serde_json::json!({"recipient":"frank","type":"channel.post","channel_id":7,"event_seq":9});
            v.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            v
        };
        // reactive + explicitly not addressed -> suppressed.
        assert_eq!(
            payload_to_wake(&base(
                serde_json::json!({"reactive":true,"addressed":false})
            )),
            None,
            "reactive + addressed=false suppresses the ambient wake"
        );
        // reactive but addressed -> wakes (an @mention of the reactive agent IS actionable).
        assert_eq!(
            payload_to_wake(&base(serde_json::json!({"reactive":true,"addressed":true}))),
            Some(("frank".into(), "[notification] channel #7".into())),
            "a reactive agent the post addresses still wakes"
        );
        // reactive but no addressing signal at all -> wakes (fail-safe: never drop a wake on a half-rolled-out board).
        assert_eq!(
            payload_to_wake(&base(serde_json::json!({"reactive":true}))),
            Some(("frank".into(), "[notification] channel #7".into())),
            "reactive without an addressed hint falls back to waking"
        );
        // not reactive, addressed=false -> wakes (a normal member is unaffected by the reactive gate).
        assert_eq!(
            payload_to_wake(&base(serde_json::json!({"addressed":false}))),
            Some(("frank".into(), "[notification] channel #7".into())),
            "a non-reactive member is never gated by addressing"
        );
    }

    #[test]
    fn payload_to_wake_pulls_recipient_and_prompt_or_none() {
        let assign = serde_json::json!({"recipient":"v-bolero","type":"task.assigned","task_id":7,"event_seq":100});
        assert_eq!(
            payload_to_wake(&assign),
            Some(("v-bolero".into(), "[notification] task #7".into()))
        );
        let dm = serde_json::json!({"recipient":"v-capmeshd","type":"message.direct","channel_id":1,"event_seq":438});
        assert_eq!(
            payload_to_wake(&dm),
            Some(("v-capmeshd".into(), "[notification] message #438".into()))
        );
        // missing recipient / informational type / missing ids -> None (no wake)
        assert_eq!(
            payload_to_wake(&serde_json::json!({"type":"task.assigned","task_id":7})),
            None
        );
        assert_eq!(
            payload_to_wake(&serde_json::json!({"recipient":"x","type":"presence.updated"})),
            None
        );
        // a comment to a FIREHOSE-only recipient does NOT wake (it accrues for poll). A pre-#384 payload has
        // no `subscribed` field → defaults false → drops-to-poll.
        assert_eq!(
            payload_to_wake(
                &serde_json::json!({"recipient":"x","type":"task.commented","task_id":7,"event_seq":9})
            ),
            None
        );
        assert_eq!(
            payload_to_wake(
                &serde_json::json!({"recipient":"x","type":"task.commented","task_id":7,"event_seq":9,"subscribed":false})
            ),
            None
        );
        // a comment to a recipient with a DIRECT subscription to the task (#384) DOES wake.
        let subbed = serde_json::json!({"recipient":"v-effects","type":"task.commented","task_id":7,"event_seq":9,"subscribed":true});
        assert_eq!(
            payload_to_wake(&subbed),
            Some((
                "v-effects".into(),
                "[notification] comment on task #7".into()
            ))
        );
        // a channel.post (tunnel payload carries recipient + channel_id) wakes the subscriber — #171
        let post = serde_json::json!({"recipient":"waiter","type":"channel.post","channel_id":7,"event_seq":51,"data":{"body":"deploy…","from":"deployer"}});
        assert_eq!(
            payload_to_wake(&post),
            Some(("waiter".into(), "[notification] channel #7".into()))
        );
    }

    #[test]
    fn document_approved_wakes_with_the_canonical_ref_verbatim_not_doubled() {
        // task_1147: a document.approved wakes its subscribers with "[approval] <ref>", rendering the board's
        // canonical typed ref verbatim. The board already sends ref="doc_<id>", so the prompt must NOT prepend
        // "doc " — the regression this guards is the doubled "doc doc_123".
        assert_eq!(
            notification_prompt(
                "document.approved",
                None,
                None,
                None,
                false,
                false,
                Some("doc_123")
            )
            .as_deref(),
            Some("[notification] [approval] doc_123")
        );
        // end-to-end: top-level `ref`.
        let top = serde_json::json!({"recipient":"charter-steward","type":"document.approved","ref":"doc_104"});
        assert_eq!(
            payload_to_wake(&top),
            Some((
                "charter-steward".into(),
                "[notification] [approval] doc_104".into()
            ))
        );
        // end-to-end: nested `data.ref` (the alternate body shape) resolves the same.
        let nested = serde_json::json!({"recipient":"x","type":"document.approved","data":{"ref":"doc_55","title":"t"}});
        assert_eq!(
            payload_to_wake(&nested),
            Some(("x".into(), "[notification] [approval] doc_55".into()))
        );
        // never doubled: the rendered prompt carries exactly one "doc_" and no "doc doc".
        let (_, prompt) = payload_to_wake(&top).unwrap();
        assert!(
            !prompt.contains("doc doc"),
            "ref is rendered verbatim, not re-prefixed"
        );
        // a document.approved with no ref at all cannot be rendered -> no wake (defensive, not a doubled id).
        assert_eq!(
            payload_to_wake(&serde_json::json!({"recipient":"x","type":"document.approved"})),
            None
        );
    }
}
