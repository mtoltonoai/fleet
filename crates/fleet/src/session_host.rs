//! One local persistent Codex conversation per existing Fleet roster agent.
//! The board remains the coordination authority; this is not a task scheduler.
use crate::codex::{self, RunOptions, RunResult, RunStatus};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

const MAX_PROMPT: usize = 64 * 1024;
const MAX_PENDING: usize = 32;
/// Persist the latest 4096 accepted delivery IDs, with no time expiry. Older IDs may replay.
const MAX_ACCEPTED_IDS: usize = 4096;
const TICK_PROMPT: &str = "Fleet scheduled check: first use the board check_stop tool and stop work if requested. Read your board messages and assigned work, then perform one bounded unit of actionable work under your existing charter. Preserve task versus turn distinction: a completed Codex turn alone is not proof that a task is accepted. If there is no actionable work, report idle and return; Fleet schedules the next check. Do not start an in-session sleep or recurrence loop.";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub agent: String,
    pub pid: u32,
    pub status: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub last_outcome: Option<RunStatus>,
    pub updated_at: u64,
    /// Private durable wake mailbox; never published to the board or status output.
    #[serde(default)]
    pending: VecDeque<String>,
    #[serde(default)]
    accepted_event_ids: VecDeque<String>,
}

fn safe_agent(agent: &str) -> bool {
    !agent.is_empty()
        && agent.len() <= 64
        && agent
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}

fn cadence(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    let (digits, factor) = match input.as_bytes().last() {
        Some(b's') => (&input[..input.len() - 1], 1u64),
        Some(b'm') => (&input[..input.len() - 1], 60),
        Some(b'h') => (&input[..input.len() - 1], 3600),
        Some(b'd') => (&input[..input.len() - 1], 86400),
        _ => (input, 1),
    };
    let seconds = digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(factor))
        .filter(|n| *n > 0 && *n <= 86400)
        .ok_or_else(|| {
            "interval must be a positive duration up to 24h (Ns, Nm, Nh, Nd)".to_string()
        })?;
    Ok(Duration::from_secs(seconds))
}

fn cadence_or_default(input: &str) -> Duration {
    cadence(input).unwrap_or_else(|_| {
        eprintln!("Fleet: unsupported interval {input:?}; using 30m");
        Duration::from_secs(1800)
    })
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn private_dir(path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("session path must be a real directory".into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())
}

fn save(directory: &Path, state: &mut SessionState) -> Result<(), String> {
    state.updated_at = now();
    let temporary = directory.join("state.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)
        .map_err(|e| e.to_string())?;
    let bytes = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::rename(temporary, directory.join("state.json")).map_err(|e| e.to_string())?;
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

/// Watchdogs consume protocol state, never terminal text. Stale/dead hosts are reported offline.
pub fn read_status(root: &Path, agent: &str) -> Option<String> {
    if !safe_agent(agent) {
        return None;
    }
    let state: SessionState =
        serde_json::from_slice(&std::fs::read(root.join(agent).join("state.json")).ok()?).ok()?;
    if state.agent != agent {
        return None;
    }
    let alive = state.pid > 0 && unsafe { libc::kill(state.pid as i32, 0) } == 0;
    Some(if !alive || now().saturating_sub(state.updated_at) > 45 {
        "offline".into()
    } else {
        state.status
    })
}

struct RuntimeFiles {
    _lock: File,
    socket: PathBuf,
}
impl Drop for RuntimeFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
    }
}

fn acquire(directory: &Path) -> Result<RuntimeFiles, String> {
    private_dir(directory)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join("host.lock"))
        .map_err(|e| e.to_string())?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("a session host already owns this agent".into());
    }
    let socket = directory.join("control.sock");
    if let Ok(metadata) = std::fs::symlink_metadata(&socket) {
        if !metadata.file_type().is_socket() {
            return Err("control path exists and is not a socket".into());
        }
        std::fs::remove_file(&socket).map_err(|e| e.to_string())?;
    }
    Ok(RuntimeFiles {
        _lock: lock,
        socket,
    })
}

#[derive(Debug)]
enum Control {
    Prompt(String),
    Interrupt,
    Resume,
}

struct Request {
    control: Control,
    event_id: Option<String>,
    reply: oneshot::Sender<Result<(), String>>,
}

fn parse_control(line: &[u8]) -> Result<Control, String> {
    let value: Value = serde_json::from_slice(line).map_err(|_| "invalid JSON".to_string())?;
    let object = value.as_object().ok_or("expected JSON object")?;
    if object.len() != 1 + usize::from(object.contains_key("event_id")) {
        return Err("send exactly one prompt or interrupt".into());
    }
    if let Some(id) = value.get("event_id")
        && !id
            .as_str()
            .is_some_and(|id| !id.is_empty() && id.len() <= 256)
    {
        return Err("event_id must contain 1..256 bytes".into());
    }
    if value.get("interrupt") == Some(&Value::Bool(true)) {
        return Ok(Control::Interrupt);
    }
    if value.get("resume") == Some(&Value::Bool(true)) {
        return Ok(Control::Resume);
    }
    let prompt = value
        .get("prompt")
        .and_then(Value::as_str)
        .filter(|p| !p.trim().is_empty() && p.len() <= MAX_PROMPT)
        .ok_or("prompt must contain 1..65536 bytes")?;
    Ok(Control::Prompt(prompt.into()))
}

async fn connection(mut stream: UnixStream, sender: mpsc::Sender<Request>) {
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        let mut reader = BufReader::new(&mut stream);
        let mut bytes = Vec::new();
        loop {
            let part = reader.fill_buf().await.map_err(|e| e.to_string())?;
            if part.is_empty() {
                return Err("request must end with newline".to_string());
            }
            let end = part.iter().position(|b| *b == b'\n');
            let len = end.map_or(part.len(), |n| n + 1);
            if bytes.len() + len > MAX_PROMPT {
                return Err("request exceeds limit".into());
            }
            bytes.extend_from_slice(&part[..len]);
            reader.consume(len);
            if end.is_some() {
                break;
            }
        }
        let (reply, response) = oneshot::channel();
        let control = parse_control(&bytes)?;
        let event_id = serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| "invalid JSON".to_string())?["event_id"]
            .as_str()
            .map(str::to_owned);
        sender
            .try_send(Request {
                control,
                event_id,
                reply,
            })
            .map_err(|_| "session control mailbox is full".to_string())?;
        response
            .await
            .map_err(|_| "session host stopped".to_string())?
    })
    .await
    .unwrap_or_else(|_| Err("request timed out".into()));
    let response = match outcome {
        Ok(()) => json!({"ok":true}),
        Err(error) => json!({"ok":false,"error":error}),
    };
    let mut encoded = serde_json::to_vec(&response).unwrap_or_default();
    encoded.push(b'\n');
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.write_all(&encoded)).await;
}

struct Active {
    task: JoinHandle<anyhow::Result<RunResult>>,
    cancel: watch::Sender<bool>,
    directory: PathBuf,
}

type AttemptOutcome = Result<anyhow::Result<RunResult>, tokio::task::JoinError>;

fn outcome_json(outcome: &AttemptOutcome) -> Value {
    match outcome {
        Ok(Ok(result)) => json!(result),
        Ok(Err(error)) => json!({"status":"failed","error":format!("{error:#}")}),
        Err(error) => json!({"status":"failed","error":error.to_string()}),
    }
}

fn save_result(directory: &Path, value: &Value) -> Result<(), String> {
    let temporary = directory.join("result.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)
        .map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?)
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::rename(temporary, directory.join("result.json")).map_err(|e| e.to_string())?;
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}
impl Drop for Active {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        self.task.abort();
    }
}

#[derive(Clone)]
struct HostOptions {
    agent: String,
    root: PathBuf,
    cwd: PathBuf,
    executable: PathBuf,
    model: Option<String>,
    effort: Option<String>,
    cadence: Duration,
    kickoff: String,
    board_native: bool,
    board_base: String,
    once: bool,
    file_hub: Option<(PathBuf, PathBuf)>,
}

fn tick_prompt(options: &HostOptions) -> String {
    if options.board_native {
        TICK_PROMPT.into()
    } else {
        format!(
            "Run one bounded tick of your existing role: fleet heartbeat {} (stop if STOPPED), drain fleet inbox {}, and follow your previously loaded role contract. A completed Codex turn alone does not mean a task is accepted. Return when the tick is finished; do not start an in-session sleep or recurrence loop.",
            options.agent, options.agent
        )
    }
}

fn board_policy(record: &Value) -> Result<(bool, Option<Duration>), String> {
    let held = record["retired"].as_bool().unwrap_or(false)
        || record.get("retired_at").is_some_and(|v| !v.is_null())
        || !matches!(record["lifecycle_intent"].as_str(), None | Some("run"));
    let interval = record
        .pointer("/metadata/interval")
        .and_then(Value::as_str)
        .map(cadence_or_default);
    Ok((held, interval))
}

struct BoardUpdate {
    held: bool,
    interval: Option<Duration>,
    effort: Option<String>,
}
fn board_effort(record: &Value) -> Result<String, String> {
    let effort = match record.pointer("/metadata/effort") {
        None | Some(Value::Null) => "max",
        Some(Value::String(value)) => value,
        _ => return Err("invalid Board effort type".into()),
    };
    if effort.is_empty() || effort.len() > 32 || !effort.bytes().all(|b| b.is_ascii_lowercase()) {
        return Err("invalid Board effort value".into());
    }
    Ok(effort.into())
}

async fn board_sync(
    base: &str,
    agent: &str,
    status: &str,
    gate: bool,
) -> Result<BoardUpdate, String> {
    let base = base.to_owned();
    let agent = agent.to_owned();
    let status = status.to_owned();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            let board = crate::board::Board::with_base_timeout(&base, Duration::from_secs(3));
            let record = board.get_agent(&agent)?;
            let policy = board_policy(&record)?;
            // A changed/invalid effort affects only next-turn admission, never
            // cancellation of an already running turn during heartbeat refresh.
            let effort = if gate {
                Some(board_effort(&record)?)
            } else {
                None
            };
            if !policy.0 {
                board.post_json(
                    &format!("/agents/{agent}/status"),
                    &json!({"status":status,"status_message":"Fleet Codex session host heartbeat"}),
                )?;
            }
            Ok::<_, String>(BoardUpdate {
                held: policy.0,
                interval: policy.1,
                effort,
            })
        }),
    )
    .await
    .map_err(|_| "board heartbeat timed out".to_string())?
    .map_err(|e| e.to_string())?
}

struct BoardJob {
    task: JoinHandle<Result<BoardUpdate, String>>,
    gate: bool,
}
impl Drop for BoardJob {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn start_board_job(options: &HostOptions, status: &str, gate: bool) -> BoardJob {
    let base = options.board_base.clone();
    let agent = options.agent.clone();
    let status = status.to_owned();
    BoardJob {
        gate,
        task: tokio::spawn(async move { board_sync(&base, &agent, &status, gate).await }),
    }
}

async fn host(options: HostOptions, mut shutdown: watch::Receiver<bool>) -> Result<(), String> {
    if !safe_agent(&options.agent) {
        return Err("invalid Fleet agent identifier".into());
    }
    private_dir(&options.root)?;
    let directory = options.root.join(&options.agent);
    let files = acquire(&directory)?;
    let listener =
        UnixListener::bind(&files.socket).map_err(|e| format!("bind control socket: {e}"))?;
    std::fs::set_permissions(&files.socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| e.to_string())?;
    let previous = match std::fs::read(directory.join("state.json")) {
        Ok(bytes) => Some(
            serde_json::from_slice::<SessionState>(&bytes)
                .map_err(|e| format!("invalid session state: {e}"))?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.to_string()),
    };
    if previous.as_ref().is_some_and(|s| s.agent != options.agent) {
        return Err("session state agent mismatch".into());
    }
    let mut paused = previous
        .as_ref()
        .is_some_and(|s| s.status != "idle" && s.status != "stopped");
    let mut state = previous.unwrap_or(SessionState {
        agent: options.agent.clone(),
        pid: std::process::id(),
        status: "idle".into(),
        thread_id: None,
        turn_id: None,
        last_outcome: None,
        updated_at: now(),
        pending: VecDeque::new(),
        accepted_event_ids: VecDeque::new(),
    });
    if state.status == "busy" {
        state.status = "recovery_required".into();
    }
    state.pid = std::process::id();
    if !paused {
        state.status = "idle".into();
    }
    if !paused && state.pending.is_empty() {
        state
            .pending
            .push_back(if state.thread_id.is_some() && !options.once {
                tick_prompt(&options)
            } else {
                options.kickoff.clone()
            });
    }
    save(&directory, &mut state)?;
    let (commands, mut controls) = mpsc::channel::<Request>(MAX_PENDING);
    let connections = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
    let (events, mut telemetry) = mpsc::channel::<Value>(256);
    let mut active: Option<Active> = None;
    let mut board_job: Option<BoardJob> = None;
    let mut board_ready = false;
    let mut current_effort = options.effort.clone();
    let mut next_tick = Instant::now() + options.cadence;
    let mut current_cadence = options.cadence;
    let mut next_board = Instant::now() + Duration::from_secs(60);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *shutdown.borrow() {
            break;
        }
        if active.is_none() && !paused && !state.pending.is_empty() {
            if options.board_native && !board_ready {
                if board_job.is_none() {
                    board_job = Some(start_board_job(&options, "busy", true));
                }
            } else if options
                .file_hub
                .as_ref()
                .is_some_and(|(stop, _)| stop.exists())
            {
                paused = true;
                state.status = "paused".into();
                save(&directory, &mut state)?;
                continue;
            }
            if (!options.board_native || board_ready)
                && let Some(prompt) = state.pending.pop_front()
            {
                board_ready = false;
                while telemetry.try_recv().is_ok() {}
                let stamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let attempt = directory.join(format!("turn-{stamp}"));
                private_dir(&attempt)?;
                state.status = "busy".into();
                state.turn_id = None;
                save(&directory, &mut state)?;
                let (cancel, receiver) = watch::channel(false);
                active = Some(Active {
                    cancel,
                    directory: attempt.clone(),
                    task: tokio::spawn(codex::run(
                        RunOptions {
                            executable: options.executable.clone(),
                            cwd: options.cwd.clone(),
                            prompt,
                            model: options.model.clone(),
                            effort: current_effort.clone(),
                            resume_thread: state.thread_id.clone(),
                            timeout: Duration::from_secs(1800),
                            event_log: attempt.join("events.jsonl"),
                            event_tx: Some(events.clone()),
                        },
                        receiver,
                    )),
                });
            }
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            Some(request) = controls.recv() => {
                if request.reply.is_closed() { continue; }
                if request.event_id.as_ref().is_some_and(|id|state.accepted_event_ids.contains(id)) {
                    let _=request.reply.send(Ok(())); continue;
                }
                let mut updated=state.clone();
                let mut new_paused=paused;
                let mut interrupt=false;
                let accepted=match request.control {
                Control::Prompt(prompt) => {
                    if paused { Err("session paused; operator resume is required".into()) }
                    else if state.pending.len() >= MAX_PENDING { Err("session pending mailbox is full".into()) }
                    else { updated.pending.push_back(prompt); Ok(()) }
                }
                Control::Interrupt => {
                    new_paused=true; interrupt=true;
                    updated.status="paused".into(); Ok(())
                }
                Control::Resume => {
                    if active.is_some() { Err("wait for the active turn to stop before resuming".into()) }
                    else {
                        new_paused=false; updated.status="idle".into();
                        if updated.pending.is_empty() { updated.pending.push_back(if updated.thread_id.is_some() && !options.once { tick_prompt(&options) } else { options.kickoff.clone() }); }
                        Ok(())
                    }
                }
                };
                let accepted=accepted.and_then(|()| {
                    if let Some(id)=request.event_id { updated.accepted_event_ids.push_back(id); while updated.accepted_event_ids.len()>MAX_ACCEPTED_IDS {updated.accepted_event_ids.pop_front();} }
                    save(&directory,&mut updated)?;
                    state=updated; paused=new_paused;
                    if interrupt && let Some(running)=&active {let _=running.cancel.send(true);}
                    if paused || active.is_none() {board_ready=false;}
                    Ok(())
                });
                let _=request.reply.send(accepted);
            },
            Some(event) = telemetry.recv(), if active.is_some() => {
                if let Some(id) = event["thread_id"].as_str() { state.thread_id = Some(id.into()); }
                if let Some(id) = event["turn_id"].as_str() { state.turn_id = Some(id.into()); }
                save(&directory, &mut state)?;
            }
            outcome = async { (&mut active.as_mut().unwrap().task).await }, if active.is_some() => {
                let finished=active.take().unwrap();
                let private_result=outcome_json(&outcome);
                save_result(&finished.directory,&private_result)?;
                if private_result["status"]!="completed" { eprintln!("Fleet Codex attempt {}: details in {}",private_result["status"],finished.directory.join("result.json").display()); }
                drop(finished);
                let held_by_board=matches!(state.status.as_str(),"board_paused"|"board_unavailable");
                let hold_status=state.status.clone();
                match outcome {
                    Ok(Ok(result)) => {
                        state.thread_id = result.thread_id.or(state.thread_id);
                        state.turn_id = result.turn_id.or(state.turn_id);
                        state.last_outcome = Some(result.status);
                        state.status = match result.status {
                            RunStatus::Completed if !paused => "idle",
                            RunStatus::Completed | RunStatus::Interrupted => { paused=true; "paused" },
                            RunStatus::AuthRequired => { paused=true; "auth_required" },
                            RunStatus::Failed | RunStatus::TimedOut => { paused=true; "failed" },
                        }.into();
                        if held_by_board && matches!(result.status,RunStatus::Interrupted|RunStatus::Completed) { state.status=hold_status; }
                    }
                    _ => { paused=true; state.status="failed".into(); state.last_outcome=Some(RunStatus::Failed); }
                }
                save(&directory, &mut state)?;
                next_tick = Instant::now()+current_cadence;
                if options.once { break; }
                board_ready=false;
                next_board=Instant::now();
            }
            incoming = listener.accept() => {
                let (stream, _) = incoming.map_err(|e| e.to_string())?;
                if let Ok(permit) = connections.clone().try_acquire_owned() {
                    let sender = commands.clone();
                    tokio::spawn(async move { let _permit=permit; connection(stream, sender).await; });
                }
            }
            _ = heartbeat.tick() => {
                if let Some((stop, stamp))=&options.file_hub {
                    if stop.exists() {
                        paused=true; state.status="paused".into();
                        if let Some(running)=&active { let _=running.cancel.send(true); }
                    } else {
                        if let Some(parent)=stamp.parent() { std::fs::create_dir_all(parent).map_err(|e|e.to_string())?; }
                        std::fs::write(stamp,"tick\n").map_err(|e|e.to_string())?;
                    }
                }
                save(&directory, &mut state)?;
            }
            result=async {(&mut board_job.as_mut().unwrap().task).await}, if board_job.is_some()=> {
                let job=board_job.take().unwrap();
                let result=result.map_err(|e|e.to_string()).and_then(|result|result);
                match result {
                    Ok(BoardUpdate {held:false,interval,effort}) => {
                        // Existing work already owns its RunOptions. This updates
                        // only future admission, including the next gate-bound turn.
                        if let Some(effort)=effort {current_effort=Some(effort);}
                        if let Some(interval)=interval {
                            if current_cadence!=interval && active.is_none() {next_tick=Instant::now()+interval;}
                            current_cadence=interval;
                        }
                        if active.is_none() && matches!(state.status.as_str(),"board_paused"|"board_unavailable") {paused=false;state.status="idle".into();next_tick=Instant::now();save(&directory,&mut state)?;}
                        board_ready=job.gate && !paused;
                    }
                    other => {
                        board_ready=false;
                        if !paused || matches!(state.status.as_str(),"board_paused"|"board_unavailable") {state.status=if other.is_ok() {"board_paused"} else {"board_unavailable"}.into();}
                        paused=true;
                        if let Some(running)=&active { let _=running.cancel.send(true); }
                        save(&directory,&mut state)?;
                    }
                }
                next_board=Instant::now()+Duration::from_secs(60);
            }
            _ = tokio::time::sleep_until(next_board), if options.board_native && board_job.is_none() => {
                let status=if active.is_some() {"busy"} else if paused {"blocked"} else {"idle"};
                board_job=Some(start_board_job(&options,status,false));
                next_board=Instant::now()+Duration::from_secs(60);
            }
            _ = tokio::time::sleep_until(next_tick), if active.is_none() && !paused => {
                state.pending.push_back(tick_prompt(&options)); next_tick=Instant::now()+current_cadence;
            }
        }
    }
    if let Some(mut running) = active.take() {
        let _ = running.cancel.send(true);
        match tokio::time::timeout(Duration::from_secs(3), &mut running.task).await {
            Ok(outcome) => {
                save_result(&running.directory, &outcome_json(&outcome))?;
                if let Ok(Ok(result)) = outcome {
                    state.thread_id = result.thread_id.or(state.thread_id);
                    state.turn_id = result.turn_id.or(state.turn_id);
                    state.last_outcome = Some(result.status);
                    state.status = "paused".into();
                }
            }
            Err(_) => {
                save_result(
                    &running.directory,
                    &json!({"status":"interrupted","error":"host shutdown exceeded cancellation grace"}),
                )?;
                state.status = "recovery_required".into();
            }
        }
        paused = true;
    }
    if !paused {
        state.status = "stopped".into();
    } else if state.status == "busy" {
        state.status = "recovery_required".into();
    }
    save(&directory, &mut state)?;
    if options.once && state.last_outcome != Some(RunStatus::Completed) {
        return Err(format!(
            "Codex one-shot ended as {}; inspect private attempt result.json",
            state.status
        ));
    }
    Ok(())
}

/// Entry point called by the existing Fleet launch path inside its tmux window.
#[allow(clippy::too_many_arguments)] // Matches the existing Fleet launch contract at the CLI boundary.
pub fn serve(
    agent: &str,
    state_root: PathBuf,
    model: Option<String>,
    effort: Option<String>,
    interval: &str,
    kickoff: String,
    board_native: bool,
    once: bool,
) -> Result<(), String> {
    let options = HostOptions {
        agent: agent.into(),
        root: state_root,
        cwd: std::env::current_dir().map_err(|e| e.to_string())?,
        executable: "codex".into(),
        model,
        effort,
        cadence: cadence_or_default(interval),
        kickoff,
        board_native,
        board_base: crate::board::Board::base_url(),
        once,
        file_hub: if !board_native && !once {
            let fleet = crate::Fleet::resolve();
            Some((
                fleet.stopfile(agent),
                fleet.root.join("heartbeat").join(agent),
            ))
        } else {
            None
        },
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let result = runtime.block_on(async {
        let (stop, receiver) = watch::channel(false);
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| e.to_string())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| e.to_string())?;
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .map_err(|e| e.to_string())?;
        let signals = tokio::spawn(async move {
            tokio::select! { _=terminate.recv()=>{}, _=interrupt.recv()=>{}, _=hangup.recv()=>{} }
            let _ = stop.send(true);
        });
        let result = host(options, receiver).await;
        signals.abort();
        result
    });
    // HTTP blocking workers have their own deadlines; do not wait for them on signal shutdown.
    // The active Codex process is cancelled and reaped inside host() before reaching this point.
    runtime.shutdown_timeout(Duration::from_millis(100));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_interval_and_control() {
        assert_eq!(cadence("2m").unwrap(), Duration::from_secs(120));
        for invalid in ["0", "0s", "-1m", "99999999999999999h", "2d", ""] {
            assert!(cadence(invalid).is_err());
        }
        assert!(matches!(
            parse_control(br#"{"prompt":"hello"}"#).unwrap(),
            Control::Prompt(_)
        ));
        assert!(matches!(
            parse_control(br#"{"interrupt":true}"#).unwrap(),
            Control::Interrupt
        ));
        for invalid in [
            r#"{"interrupt":false}"#,
            r#"{"prompt":""}"#,
            r#"{"prompt":"x","interrupt":true}"#,
        ] {
            assert!(parse_control(invalid.as_bytes()).is_err());
        }
        assert!(!safe_agent("../escape"));
    }

    #[test]
    fn daily_cadence_matches_watchdog_and_board_policy() {
        let daily = Duration::from_secs(86400);
        for input in ["1d", " 1d ", "24h", "1440m", "86400s", "86400"] {
            assert_eq!(cadence(input).unwrap(), daily, "{input}");
            assert_eq!(crate::parse_interval_secs(input), Some(daily.as_secs()));
            assert_eq!(
                cadence_or_default(input),
                daily,
                "startup interval: {input}"
            );
            let record = json!({"lifecycle_intent":"run","metadata":{"interval":input}});
            assert_eq!(board_policy(&record).unwrap(), (false, Some(daily)));
        }
    }

    #[test]
    fn cadence_preserves_duration_bounds() {
        for input in ["1", "1s", "1m", "1h", "1d"] {
            assert!(cadence(input).is_ok(), "{input}");
        }
        for input in [
            "0d",
            "2d",
            "25h",
            "1441m",
            "86401s",
            "86401",
            "18446744073709551615d",
        ] {
            assert!(cadence(input).is_err(), "{input}");
            assert_eq!(cadence_or_default(input), Duration::from_secs(1800));
        }
    }

    #[test]
    fn lock_is_exclusive_and_state_private() {
        let directory = tempfile::tempdir().unwrap();
        let _owner = acquire(directory.path()).unwrap();
        assert!(acquire(directory.path()).is_err());
        let mut state = SessionState {
            agent: "test".into(),
            pid: std::process::id(),
            status: "idle".into(),
            thread_id: None,
            turn_id: None,
            last_outcome: None,
            updated_at: 0,
            pending: VecDeque::new(),
            accepted_event_ids: VecDeque::new(),
        };
        save(directory.path(), &mut state).unwrap();
        assert_eq!(
            std::fs::metadata(directory.path().join("state.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(state.updated_at > 0);
    }

    #[tokio::test]
    async fn local_control_ack_and_interrupt() {
        let (client, server) = UnixStream::pair().unwrap();
        let (sender, mut receiver) = mpsc::channel(2);
        let task = tokio::spawn(connection(server, sender));
        let (read, mut write) = client.into_split();
        write.write_all(b"{\"interrupt\":true}\n").await.unwrap();
        let request = receiver.recv().await.unwrap();
        assert!(matches!(request.control, Control::Interrupt));
        request.reply.send(Ok(())).unwrap();
        let mut line = String::new();
        BufReader::new(read).read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["ok"], true);
        task.await.unwrap();
    }

    fn mock_host(status: &str) -> (tempfile::TempDir, HostOptions) {
        // Short path keeps control.sock under Darwin's Unix socket path limit.
        let temp = tempfile::Builder::new()
            .prefix("fleet-host-")
            .tempdir_in("/tmp")
            .unwrap();
        let executable = temp.path().join("codex");
        let script = format!(
            r#"#!/usr/bin/env python3
import json,sys,os
def send(v): print(json.dumps(v),flush=True)
for line in sys.stdin:
 m=json.loads(line)
 method=m.get('method')
 with open('requests.jsonl','a') as f: f.write(json.dumps(m)+'\n')
 if method=='initialize': send({{'id':m['id'],'result':{{}}}})
 elif method in ('thread/start','thread/resume'): send({{'id':m['id'],'result':{{'thread':{{'id':'thread-host'}}}}}})
 elif method=='turn/start':
  send({{'id':m['id'],'result':{{'turn':{{'id':'turn-host','status':'inProgress'}}}}}})
  send({{'method':'turn/completed','params':{{'threadId':'thread-host','turn':{{'id':'turn-host','status':'{status}','error':{error}}}}}}})
 elif method=='turn/interrupt': send({{'id':m['id'],'result':{{}}}})
"#,
            error = if status == "failed" {
                "{'codexErrorInfo':'Unauthorized','message':'auth required'}"
            } else {
                "None"
            }
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let options = HostOptions {
            agent: "tester".into(),
            root: temp.path().join("runtime"),
            cwd: temp.path().into(),
            executable,
            model: None,
            effort: None,
            cadence: Duration::from_secs(3600),
            kickoff: "mock kickoff".into(),
            board_native: false,
            board_base: String::new(),
            once: false,
            file_hub: None,
        };
        (temp, options)
    }

    async fn wait_state(root: &Path, status: &str) -> SessionState {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(bytes) = std::fs::read(root.join("tester/state.json"))
                    && let Ok(state) = serde_json::from_slice::<SessionState>(&bytes)
                    && state.status == status
                    && state.last_outcome.is_some()
                {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mock host never reached expected state")
    }

    async fn control(root: &Path, value: Value) -> Value {
        let mut stream = UnixStream::connect(root.join("tester/control.sock"))
            .await
            .unwrap();
        let mut line = serde_json::to_vec(&value).unwrap();
        line.push(b'\n');
        stream.write_all(&line).await.unwrap();
        let mut reply = String::new();
        BufReader::new(stream).read_line(&mut reply).await.unwrap();
        serde_json::from_str(&reply).unwrap()
    }

    #[tokio::test]
    async fn host_reuses_thread_and_does_not_accept_tasks() {
        let (temp, options) = mock_host("completed");
        let root = options.root.clone();
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(host(options, receiver));
        let state = wait_state(&root, "idle").await;
        assert_eq!(state.thread_id.as_deref(), Some("thread-host"));
        let result_paths: Vec<_> = std::fs::read_dir(root.join("tester"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("result.json"))
            .filter(|path| path.exists())
            .collect();
        assert_eq!(result_paths.len(), 1);
        assert_eq!(
            std::fs::metadata(&result_paths[0])
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let result: Value =
            serde_json::from_slice(&std::fs::read(&result_paths[0]).unwrap()).unwrap();
        assert_eq!(result["status"], "completed");
        assert_eq!(
            control(&root, json!({"prompt":"second wake"})).await["ok"],
            true
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let requests = std::fs::read_to_string(temp.path().join("requests.jsonl")).unwrap();
                if requests.contains("thread/resume") && requests.contains("second wake") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        wait_state(&root, "idle").await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert!(!root.join("tester/control.sock").exists());
        assert_eq!(read_status(&root, "tester").as_deref(), Some("stopped"));
    }

    #[tokio::test]
    async fn auth_pause_rejects_ordinary_wakes_until_explicit_resume() {
        let (_temp, mut options) = mock_host("failed");
        options.cadence = Duration::from_secs(1);
        let root = options.root.clone();
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(host(options, receiver));
        wait_state(&root, "auth_required").await;
        assert_eq!(
            control(&root, json!({"prompt":"webhook"})).await["ok"],
            false
        );
        assert_eq!(control(&root, json!({"resume":true})).await["ok"], true);
        wait_state(&root, "auth_required").await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn board_lifecycle_is_authoritative() {
        for record in [
            json!({"retired":true}),
            json!({"retired_at":"2026-01-01"}),
            json!({"lifecycle_intent":"paused"}),
        ] {
            assert!(board_policy(&record).unwrap().0);
        }
        let (held, interval) =
            board_policy(&json!({"lifecycle_intent":"run","metadata":{"interval":"3h"}})).unwrap();
        assert!(!held);
        assert_eq!(interval, Some(Duration::from_secs(10800)));
    }

    #[tokio::test]
    async fn repeated_once_uses_new_kickoff_without_recurring() {
        let (temp, mut options) = mock_host("completed");
        options.once = true;
        let (_stop, receiver) = watch::channel(false);
        host(options.clone(), receiver.clone()).await.unwrap();
        options.kickoff = "second one-shot kickoff".into();
        host(options, receiver).await.unwrap();
        let requests = std::fs::read_to_string(temp.path().join("requests.jsonl")).unwrap();
        assert!(requests.contains("second one-shot kickoff"));
        let turns = requests
            .lines()
            .filter(|line| serde_json::from_str::<Value>(line).unwrap()["method"] == "turn/start")
            .count();
        assert_eq!(turns, 2);
    }

    #[tokio::test]
    async fn delayed_board_gate_keeps_control_and_shutdown_responsive() {
        let (temp, mut options) = mock_host("completed");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        options.board_native = true;
        options.board_base = format!("http://{}", listener.local_addr().unwrap());
        let root = options.root.clone();
        let (seen, mut requests) = mpsc::channel(2);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut first = String::new();
            reader.read_line(&mut first).await.unwrap();
            assert!(first.starts_with("GET /agents/tester"));
            seen.send(()).await.unwrap();
            // Hold the real HTTP request open longer than the control-socket deadline.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(host(options, receiver));
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let reply = tokio::time::timeout(
            Duration::from_millis(750),
            control(&root, json!({"interrupt":true,"event_id":"interrupt-1"})),
        )
        .await
        .unwrap();
        assert_eq!(reply["ok"], true);
        assert!(
            !temp.path().join("requests.jsonl").exists(),
            "no Codex inference before board policy confirmation"
        );
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(750), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn delivery_ids_survive_restart_and_do_not_duplicate_turns() {
        let (temp, options) = mock_host("completed");
        let root = options.root.clone();
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(host(options.clone(), receiver));
        wait_state(&root, "idle").await;
        let delivery = json!({"prompt":"unique-delivery-prompt","event_id":"board:42:tester"});
        assert_eq!(control(&root, delivery.clone()).await["ok"], true);
        assert_eq!(control(&root, delivery.clone()).await["ok"], true);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if std::fs::read_to_string(temp.path().join("requests.jsonl"))
                    .unwrap()
                    .contains("unique-delivery-prompt")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        wait_state(&root, "idle").await;
        assert_eq!(
            control(&root, json!({"interrupt":true,"event_id":"pause:tester"})).await["ok"],
            true
        );
        // Duplicate ack succeeds while paused without resuming work.
        assert_eq!(control(&root, delivery.clone()).await["ok"], true);
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        let before = std::fs::read_to_string(temp.path().join("requests.jsonl")).unwrap();
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(host(options, receiver));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !root.join("tester/control.sock").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(control(&root, delivery).await["ok"], true);
        assert_eq!(read_status(&root, "tester").as_deref(), Some("paused"));
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        let after = std::fs::read_to_string(temp.path().join("requests.jsonl")).unwrap();
        assert_eq!(
            before, after,
            "duplicate persisted delivery must not launch a paid turn"
        );
        assert_eq!(after.matches("unique-delivery-prompt").count(), 1);
        let state: SessionState =
            serde_json::from_slice(&std::fs::read(root.join("tester/state.json")).unwrap())
                .unwrap();
        assert!(state.accepted_event_ids.contains(&"board:42:tester".into()));
    }
    #[tokio::test]
    async fn board_effort_refresh_applies_next_turn_without_interrupt_or_thread_change() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use tokio::io::AsyncReadExt;
        let (temp, mut options) = mock_host("completed");
        options.board_native = true;
        options.effort = Some("low".into());
        options.model = Some("gpt-6-astra".into());
        let script=std::fs::read_to_string(&options.executable).unwrap().replace("elif method=='turn/start':", "elif method=='turn/start':\n  if m['params'].get('effort')=='medium':\n   import time\n   open('first-turn-started','w').close()\n   while not os.path.exists('release-first'): time.sleep(0.01)");
        std::fs::write(&options.executable, script).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        options.board_base = format!("http://{}", listener.local_addr().unwrap());
        let desired_max = Arc::new(AtomicBool::new(false));
        let flag = desired_max.clone();
        let (server_stop, mut server_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            loop {
                let incoming = tokio::select! {v=listener.accept()=>v, _=&mut server_rx=>break};
                let (stream, _) = incoming.unwrap();
                let mut reader = BufReader::new(stream);
                let mut first = String::new();
                reader.read_line(&mut first).await.unwrap();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                        length = v.trim().parse::<usize>().unwrap();
                    }
                }
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).await.unwrap();
                let response=if first.starts_with("GET "){json!({"id":"tester","lifecycle_intent":"run","metadata":{"effort":if flag.load(Ordering::SeqCst){"max"}else{"medium"}}})}else{json!({})}.to_string();
                reader
                    .get_mut()
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            response.len(),
                            response
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let root = options.root.clone();
        let (stop, rx) = watch::channel(false);
        let host_task = tokio::spawn(host(options, rx));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !temp.path().join("first-turn-started").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        desired_max.store(true, Ordering::SeqCst);
        assert_eq!(
            control(
                &root,
                json!({"prompt":"next ordinary turn","event_id":"effort-max"})
            )
            .await["ok"],
            true
        );
        std::fs::write(temp.path().join("release-first"), b"continue").unwrap();
        let requests = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let text = std::fs::read_to_string(temp.path().join("requests.jsonl")).unwrap();
                let values: Vec<Value> = text
                    .lines()
                    .filter_map(|l| serde_json::from_str(l).ok())
                    .collect();
                if values
                    .iter()
                    .filter(|v| v["method"] == "turn/start")
                    .count()
                    == 2
                {
                    break values;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let done = wait_state(&root, "idle").await;
        assert_eq!(done.thread_id.as_deref(), Some("thread-host"));
        let efforts: Vec<_> = requests
            .iter()
            .filter(|v| v["method"] == "turn/start")
            .map(|v| v["params"]["effort"].as_str().unwrap())
            .collect();
        assert_eq!(efforts, vec!["medium", "max"]);
        assert!(!requests.iter().any(|v| v["method"] == "turn/interrupt"));
        assert!(requests.iter().any(|v|v["method"]=="thread/resume" && v["params"]["threadId"]=="thread-host"));
        stop.send(true).unwrap();
        host_task.await.unwrap().unwrap();
        server_stop.send(()).unwrap();
        server.await.unwrap();
        assert_eq!(board_effort(&json!({})).unwrap(), "max");
        assert!(board_effort(&json!({"metadata":{"effort":42}})).is_err());
    }
}
