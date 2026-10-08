//! Managed Codex app-server transport. No terminal scraping or configuration writes.
//! Protocol reference: https://learn.chatgpt.com/docs/app-server
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::watch,
    time::Instant,
};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_EVENTS: usize = 32 * 1024 * 1024;
const MAX_STDERR: usize = 1024 * 1024;

struct StderrDrain(tokio::task::JoinHandle<std::io::Result<()>>);
impl Drop for StderrDrain {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn drain_stderr(
    mut stream: tokio::process::ChildStderr,
    mut log: tokio::fs::File,
) -> std::io::Result<()> {
    let mut buffer = [0u8; 8192];
    let mut retained = 0usize;
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let keep = count.min(MAX_STDERR.saturating_sub(retained));
        if keep > 0 {
            log.write_all(&buffer[..keep]).await?;
            retained += keep;
        }
        // Continue draining once capped so diagnostics cannot stall the subprocess.
    }
    log.sync_all().await
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub executable: PathBuf,
    pub cwd: PathBuf,
    pub prompt: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    /// Explicit continuation only; a crashed attempt must not silently replay its prompt.
    pub resume_thread: Option<String>,
    pub timeout: Duration,
    pub event_log: PathBuf,
    /// Bounded telemetry; never forwards prompts, command text, model output, or auth data.
    pub event_tx: Option<tokio::sync::mpsc::Sender<Value>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Completed,
    Failed,
    Interrupted,
    TimedOut,
    AuthRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub status: RunStatus,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub error: Option<String>,
}

impl RunResult {
    fn finish(&mut self, status: RunStatus, error: impl Into<Option<String>>) {
        self.status = status;
        self.error = error.into();
    }
}

/// Own the process group even when a caller drops the future. Explicit cleanup also reaps it.
struct Process {
    child: Child,
    group: Option<u32>,
}
impl Process {
    fn kill_group(&self) {
        #[cfg(unix)]
        if let Some(id) = self.group {
            // The child is a new process-group leader; never signal the worker's own group.
            unsafe {
                libc::kill(-(id as i32), libc::SIGKILL);
            }
        }
    }
    async fn cleanup(&mut self) {
        self.kill_group();
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        self.group = None;
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.kill_group();
    }
}

async fn send(input: &mut ChildStdin, message: Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&message)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(1), async {
        input.write_all(&bytes).await?;
        input.flush().await
    })
    .await
    .context("app-server stopped reading requests")??;
    Ok(())
}

/// A bounded frame reader. The accumulated bytes survive cancellation of this future.
async fn frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    pending: &mut Vec<u8>,
) -> Result<Option<Value>> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if pending.is_empty() {
                return Ok(None);
            }
            bail!("app-server closed stdout with an incomplete protocol frame");
        }
        let end = available.iter().position(|b| *b == b'\n');
        let len = end.map_or(available.len(), |n| n + 1);
        if pending.len() + len > MAX_FRAME {
            bail!("app-server protocol frame exceeds limit");
        }
        pending.extend_from_slice(&available[..len]);
        reader.consume(len);
        if end.is_some() {
            let parsed = serde_json::from_slice(pending).context("invalid app-server JSON frame");
            pending.clear();
            return parsed.map(Some);
        }
    }
}

fn auth_error(error: &Value) -> bool {
    // codexErrorInfo may be a string enum or an externally tagged object.
    let text = error.to_string().to_ascii_lowercase();
    text.contains("unauthorized")
        || text.contains("authentication")
        || text.contains("not logged in")
        || text.contains("login required")
        || text.contains("httpstatuscode\":401")
}

fn nonempty_id(value: &Value, pointer: &str) -> Result<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("app-server response missing {pointer}"))
}

fn emit(options: &RunOptions, method: &str, result: &RunResult, params: Option<&Value>) {
    if let Some(sender) = &options.event_tx {
        let mut event =
            json!({"method":method,"thread_id":result.thread_id,"turn_id":result.turn_id});
        if let Some(params) = params {
            for key in ["type", "status"] {
                if let Some(value) = params
                    .get("item")
                    .and_then(|item| item.get(key))
                    .and_then(Value::as_str)
                {
                    event[format!("item_{key}")] = json!(value);
                }
            }
        }
        // Consumer slowness must never block the managed subprocess or lease cancellation.
        let _ = sender.try_send(event);
    }
}

/// Runs one attempt. Completion is a Codex turn outcome, never task acceptance.
pub async fn run(options: RunOptions, mut cancel: watch::Receiver<bool>) -> Result<RunResult> {
    if options.timeout.is_zero() {
        bail!("Codex timeout must be positive");
    }
    if options.prompt.trim().is_empty() {
        bail!("Codex prompt must not be empty");
    }
    let mut result = RunResult {
        status: RunStatus::Failed,
        thread_id: None,
        turn_id: None,
        error: None,
    };
    if *cancel.borrow() {
        result.finish(RunStatus::Interrupted, None);
        return Ok(result);
    }
    let mut log_options = tokio::fs::OpenOptions::new();
    log_options.write(true).create_new(true);
    #[cfg(unix)]
    log_options.mode(0o600);
    let mut log = log_options
        .open(&options.event_log)
        .await
        .context("create private Codex event log")?;
    let stderr_log = log_options
        .open(options.event_log.with_file_name("stderr.log"))
        .await
        .context("create private Codex stderr log")?;
    let mut command = Command::new(&options.executable);
    command
        .arg("app-server")
        .current_dir(&options.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn().context("launch Codex app-server")?;
    let mut process = Process {
        group: child.id(),
        child,
    };
    let mut input = process
        .child
        .stdin
        .take()
        .context("Codex stdin unavailable")?;
    let mut output = BufReader::new(
        process
            .child
            .stdout
            .take()
            .context("Codex stdout unavailable")?,
    );
    let stderr = process
        .child
        .stderr
        .take()
        .context("Codex stderr unavailable")?;
    let mut stderr_task = StderrDrain(tokio::spawn(drain_stderr(stderr, stderr_log)));
    let deadline = Instant::now() + options.timeout;
    let execution = async {
        send(&mut input, json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"fleet","title":"Fleet","version":env!("CARGO_PKG_VERSION")}}})).await?;
        let mut pending = Vec::new();
        let mut bytes = 0usize;
        let mut stage = 1u64;
        let mut cancel_open = true;
        loop {
            let message = tokio::select! {
                biased;
                changed = cancel.changed(), if cancel_open => {
                    if changed.is_err() { cancel_open = false; continue; }
                    if !*cancel.borrow() { continue; }
                    result.finish(RunStatus::Interrupted, Some("attempt cancelled".into()));
                    break;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    result.finish(RunStatus::TimedOut, Some("Codex attempt deadline exceeded".into()));
                    break;
                }
                message = frame(&mut output, &mut pending) => match message? {
                    Some(message) => message,
                    None => bail!("app-server exited before a terminal turn event"),
                }
            };
            let mut encoded = serde_json::to_vec(&message)?;
            encoded.push(b'\n');
            bytes += encoded.len();
            if bytes > MAX_EVENTS { bail!("Codex event log exceeds limit"); }
            log.write_all(&encoded).await?;
            // Flush IDs/events promptly so interrupted attempts can be inspected after a crash.
            log.flush().await?;
            if message.get("method").is_some() && message.get("id").is_some() {
                let method = message["method"].as_str().unwrap_or_default();
                // Never approve a request, supply credentials, or alter policy on behalf of the model.
                send(&mut input, json!({"id":message["id"],"error":{"code":-32601,"message":"Fleet cannot fulfill interactive server requests"}})).await?;
                result.finish(if method.starts_with("account/") { RunStatus::AuthRequired } else { RunStatus::Failed }, Some(format!("Codex requires operator handling: {method}")));
                break;
            }
            if let Some(id) = message.get("id").and_then(Value::as_u64) {
                if id != stage { bail!("unexpected app-server response id {id}"); }
                if let Some(error) = message.get("error") {
                    result.finish(if auth_error(error) { RunStatus::AuthRequired } else { RunStatus::Failed }, Some(format!("Codex request failed: {error}")));
                    break;
                }
                if !message.get("result").is_some_and(Value::is_object) {
                    bail!("app-server response has no result object");
                }
                match stage {
                    1 => {
                        send(&mut input, json!({"method":"initialized","params":{}})).await?;
                        let mut params = json!({"cwd": options.cwd});
                        if let Some(model) = &options.model { params["model"] = json!(model); }
                        let method = if let Some(thread) = &options.resume_thread {
                            params["threadId"] = json!(thread); "thread/resume"
                        } else { "thread/start" };
                        send(&mut input, json!({"id":2,"method":method,"params":params})).await?;
                        stage = 2;
                    }
                    2 => {
                        let thread = nonempty_id(&message, "/result/thread/id")?;
                        if options.resume_thread.as_ref().is_some_and(|expected| expected != &thread) { bail!("resumed thread identity mismatch"); }
                        result.thread_id = Some(thread.clone());
                        emit(&options, "thread_started", &result, None);
                        let mut params = json!({"threadId":thread,"input":[{"type":"text","text":options.prompt}]});
                        if let Some(effort) = &options.effort { params["effort"] = json!(effort); }
                        send(&mut input, json!({"id":3,"method":"turn/start","params":params})).await?;
                        stage = 3;
                    }
                    3 => {
                        let turn = nonempty_id(&message, "/result/turn/id")?;
                        if result.turn_id.as_ref().is_some_and(|known| known != &turn) { bail!("turn identity mismatch"); }
                        result.turn_id = Some(turn);
                        emit(&options, "turn_started", &result, None);
                        stage = 4;
                    }
                    _ => bail!("unexpected app-server response"),
                }
                continue;
            }
            let method = message["method"].as_str().unwrap_or_default();
            let params = &message["params"];
            if matches!(method, "turn/started" | "turn/completed") {
                // Resumed threads may emit history before turn/start answers. Only its
                // authoritative response establishes this attempt's turn identity.
                if stage != 4 { continue; }
                if params["threadId"].as_str() != result.thread_id.as_deref() { continue; }
                let turn = nonempty_id(params, "/turn/id")?;
                if result.turn_id.as_ref().is_some_and(|known| known != &turn) { continue; }
                result.turn_id = Some(turn);
                emit(&options, method, &result, Some(params));
                if method == "turn/completed" {
                    let error = &params["turn"]["error"];
                    let status = match params["turn"]["status"].as_str() {
                        Some("completed") if error.is_null() => RunStatus::Completed,
                        Some("interrupted") => RunStatus::Interrupted,
                        Some("failed") if auth_error(error) => RunStatus::AuthRequired,
                        Some("failed") => RunStatus::Failed,
                        _ => bail!("unknown terminal Codex turn status"),
                    };
                    result.finish(status, (!error.is_null()).then(|| error.to_string()));
                    break;
                }
            }
            if matches!(method, "item/started" | "item/completed")
                && params["threadId"].as_str() == result.thread_id.as_deref()
                && params["turnId"].as_str() == result.turn_id.as_deref()
            {
                emit(&options, method, &result, Some(params));
            }
            if method == "error" && auth_error(params) {
                result.finish(RunStatus::AuthRequired, Some("Codex authentication is required".into()));
                break;
            }
        }
        Ok::<(), anyhow::Error>(())
    }.await;
    if let Err(error) = execution {
        result.finish(RunStatus::Failed, Some(error.to_string()));
    }
    if result.status != RunStatus::Completed
        && let (Some(thread), Some(turn)) = (&result.thread_id, &result.turn_id)
    {
        // Best effort protocol cancellation, bounded even if the subprocess stopped reading.
        let _ = tokio::time::timeout(Duration::from_secs(1), send(&mut input, json!({"id":99,"method":"turn/interrupt","params":{"threadId":thread,"turnId":turn}}))).await;
        // Give the managed server a short chance to cancel its own tool sessions before killing.
        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            let mut pending = Vec::new();
            while let Some(value) = frame(&mut output, &mut pending).await? {
                if value["method"] == "turn/completed" {
                    break;
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await;
    }
    process.cleanup().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), &mut stderr_task.0).await;
    log.sync_all().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mock(body: &str) -> (tempfile::TempDir, RunOptions) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("codex");
        let script = format!(
            "#!/usr/bin/env python3\nimport json, sys, time, os\ndef send(x):\n print(json.dumps(x), flush=True)\nfor line in sys.stdin:\n m=json.loads(line)\n method=m.get('method')\n if method=='initialize':\n  assert m['params']['clientInfo']['name']=='fleet'\n  send({{'id':m['id'],'result':{{}}}})\n elif method in ('thread/start','thread/resume'):\n  assert 'sandbox' not in m['params'] and 'approvalPolicy' not in m['params']\n  send({{'id':m['id'],'result':{{'thread':{{'id':'thread-test'}}}}}})\n elif method=='turn/start':\n  send({{'id':m['id'],'result':{{'turn':{{'id':'turn-test','status':'inProgress'}}}}}})\n{body}\n elif method=='turn/interrupt':\n  open(os.path.join(os.getcwd(),'interrupted'),'w').write('yes')\n  send({{'method':'turn/completed','params':{{'threadId':'thread-test','turn':{{'id':'turn-test','status':'interrupted'}}}}}})\n"
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let options = RunOptions {
            executable,
            cwd: directory.path().to_owned(),
            prompt: "test prompt".into(),
            model: None,
            effort: None,
            resume_thread: None,
            timeout: Duration::from_secs(3),
            event_log: directory.path().join("events.jsonl"),
            event_tx: None,
        };
        (directory, options)
    }

    #[tokio::test]
    async fn completes_only_matching_terminal_event() {
        let (_dir, options) = mock(
            "  send({'method':'turn/completed','params':{'threadId':'other','turn':{'id':'x','status':'failed'}}})\n  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'turn-test','status':'completed'}}})",
        );
        let (_tx, rx) = watch::channel(false);
        let result = run(options, rx).await.unwrap();
        assert_eq!(result.status, RunStatus::Completed);
        assert_eq!(result.thread_id.as_deref(), Some("thread-test"));
        assert_eq!(result.turn_id.as_deref(), Some("turn-test"));
    }

    #[tokio::test]
    async fn missing_terminal_is_failure_even_on_zero_exit() {
        let (_dir, options) = mock("  sys.exit(0)");
        let (_tx, rx) = watch::channel(false);
        assert_eq!(run(options, rx).await.unwrap().status, RunStatus::Failed);
    }

    #[tokio::test]
    async fn auth_failure_is_distinct() {
        let (_dir, options) = mock(
            "  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'turn-test','status':'failed','error':{'message':'account expired','codexErrorInfo':'Unauthorized'}}}})",
        );
        let (_tx, rx) = watch::channel(false);
        assert_eq!(
            run(options, rx).await.unwrap().status,
            RunStatus::AuthRequired
        );
    }

    #[tokio::test]
    async fn cancels_with_interrupt() {
        let (dir, mut options) = mock("  pass");
        let (event_tx, mut events) = tokio::sync::mpsc::channel(8);
        options.event_tx = Some(event_tx);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run(options, rx));
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = events.recv().await {
                if event["method"] == "turn_started" {
                    return;
                }
            }
            panic!("mock exited before turn start");
        })
        .await
        .unwrap();
        tx.send(true).unwrap();
        assert_eq!(
            handle.await.unwrap().unwrap().status,
            RunStatus::Interrupted
        );
        assert!(dir.path().join("interrupted").exists());
    }

    #[tokio::test]
    async fn timeout_is_distinct_and_interrupts() {
        let (dir, mut options) = mock("  pass");
        // Allow concurrent Python mock startup on a busy Mac before exercising
        // the post-turn-start deadline and its protocol interrupt.
        options.timeout = Duration::from_secs(3);
        let (_tx, rx) = watch::channel(false);
        assert_eq!(run(options, rx).await.unwrap().status, RunStatus::TimedOut);
        assert!(dir.path().join("interrupted").exists());
    }

    #[tokio::test]
    async fn refuses_interactive_requests_without_approval() {
        let (_dir, options) =
            mock("  send({'id':15,'method':'item/commandExecution/requestApproval','params':{}})");
        let (_tx, rx) = watch::channel(false);
        let result = run(options, rx).await.unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.error.unwrap().contains("operator handling"));
    }

    #[tokio::test]
    async fn malformed_json_is_failure() {
        let (_dir, options) = mock("  print('not JSON', flush=True)");
        let (_tx, rx) = watch::channel(false);
        assert_eq!(run(options, rx).await.unwrap().status, RunStatus::Failed);
    }

    #[tokio::test]
    async fn telemetry_contains_ids_and_no_content() {
        let (_dir, mut options) = mock(
            "  send({'method':'item/completed','params':{'threadId':'thread-test','turnId':'turn-test','item':{'type':'agentMessage','text':'private content'}}})\n  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'turn-test','status':'completed'}}})",
        );
        let (event_tx, mut events) = tokio::sync::mpsc::channel(16);
        options.event_tx = Some(event_tx);
        let (_tx, rx) = watch::channel(false);
        assert_eq!(run(options, rx).await.unwrap().status, RunStatus::Completed);
        let first = events.recv().await.unwrap();
        assert_eq!(first["method"], "thread_started");
        assert_eq!(first["thread_id"], "thread-test");
        let second = events.recv().await.unwrap();
        assert_eq!(second["method"], "turn_started");
        assert_eq!(second["turn_id"], "turn-test");
        while let Some(event) = events.recv().await {
            assert!(!event.to_string().contains("private content"));
        }
    }

    #[tokio::test]
    async fn ignores_old_terminal_until_start_response() {
        let (_dir, mut options) = mock(
            "  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'turn-test','status':'failed','error':{'message':'actual attempt failed'}}}})",
        );
        options.resume_thread = Some("thread-test".into());
        let text = std::fs::read_to_string(&options.executable).unwrap();
        let before = " elif method=='turn/start':\n";
        let after = " elif method=='turn/start':\n  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'old-turn','status':'completed'}}})\n";
        std::fs::write(&options.executable, text.replace(before, after)).unwrap();
        let (_tx, rx) = watch::channel(false);
        let result = run(options, rx).await.unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.turn_id.as_deref(), Some("turn-test"));
    }

    #[tokio::test]
    async fn stderr_is_private_bounded_and_fully_drained() {
        let (dir, options) = mock(
            "  sys.stderr.write('x' * (2 * 1024 * 1024))\n  sys.stderr.flush()\n  send({'method':'turn/completed','params':{'threadId':'thread-test','turn':{'id':'turn-test','status':'completed'}}})",
        );
        let (_tx, rx) = watch::channel(false);
        assert_eq!(run(options, rx).await.unwrap().status, RunStatus::Completed);
        let metadata = std::fs::metadata(dir.path().join("stderr.log")).unwrap();
        assert_eq!(metadata.len(), MAX_STDERR as u64);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
}
