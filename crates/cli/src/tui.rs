use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command as StdCommand, Output, Stdio};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use serde_json::Value;

use crate::{Cli, CliOutput, SecretStoreKind};

type TuiResult<T> = Result<T, TuiError>;

const UI_EVENT_WAIT: Duration = Duration::from_millis(50);
const STREAM_APPEND_FLUSH_INTERVAL: Duration = Duration::from_millis(125);
const FOCUS_ACCENT: Color = Color::Green;
const ACCOUNT_ACCENT: Color = Color::White;
const DEFAULT_STREAM_CANDIDATE: &str = crate::DEFAULT_PRODUCTION_QUIC_BROKER_CANDIDATE;
const SLASH_SUGGESTION_LIMIT: usize = 8;
const TUI_MESSAGE_SCROLLBACK_LIMIT: usize = 1_000;
const TUI_LIVE_STREAM_PREVIEW_LIMIT: usize = 128;
const TUI_LIVE_STREAM_TEXT_LIMIT: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
enum TuiError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Cli(String),
}

pub(crate) async fn run_tui(cli: Cli) -> CliOutput {
    match TuiApp::new(cli).and_then(|mut app| app.run()) {
        Ok(()) => CliOutput {
            code: 0,
            stdout: String::new(),
            stderr: String::new(),
        },
        Err(err) => CliOutput {
            code: 1,
            stdout: String::new(),
            stderr: format!("error: {err}\n"),
        },
    }
}

#[derive(Clone, Debug)]
struct DmClient {
    exe: PathBuf,
    home: Option<PathBuf>,
    socket: Option<PathBuf>,
    relay: Option<String>,
    secret_store: Option<SecretStoreKind>,
    keychain_service: Option<String>,
}

impl DmClient {
    fn from_cli(cli: &Cli) -> TuiResult<Self> {
        Ok(Self {
            exe: std::env::current_exe()?,
            home: cli.home.clone(),
            socket: cli.socket.clone(),
            relay: cli.relay.clone(),
            secret_store: cli.secret_store,
            keychain_service: cli.keychain_service.clone(),
        })
    }

    fn run_json<S>(&self, account: Option<&str>, args: &[S]) -> TuiResult<Value>
    where
        S: AsRef<str>,
    {
        let mut command = self.command(account, args);
        let output = command.output()?;
        parse_json_output(output)
    }

    fn run_json_with_stdin<S>(
        &self,
        account: Option<&str>,
        args: &[S],
        stdin: &str,
    ) -> TuiResult<Value>
    where
        S: AsRef<str>,
    {
        let mut child = self
            .command(account, args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or_else(|| TuiError::Cli("dm stdin pipe was not available".to_owned()))?
            .write_all(stdin.as_bytes())?;
        parse_json_output(child.wait_with_output()?)
    }

    fn spawn_json_lines<S>(&self, account: Option<&str>, args: &[S]) -> TuiResult<Child>
    where
        S: AsRef<str>,
    {
        let mut command = self.command(account, args);
        command.stdout(Stdio::piped()).stderr(Stdio::null());
        Ok(command.spawn()?)
    }

    fn command<S>(&self, account: Option<&str>, args: &[S]) -> StdCommand
    where
        S: AsRef<str>,
    {
        let mut command = StdCommand::new(&self.exe);
        command.arg("--json");
        if let Some(home) = &self.home {
            command.arg("--home").arg(home);
        }
        if let Some(socket) = &self.socket {
            command.arg("--socket").arg(socket);
        }
        if let Some(relay) = &self.relay {
            command.arg("--relay").arg(relay);
        }
        if let Some(secret_store) = self.secret_store {
            command.arg("--secret-store").arg(secret_store.as_str());
        }
        if let Some(service) = &self.keychain_service {
            command.arg("--keychain-service").arg(service);
        }
        if let Some(account) = account {
            command.arg("--account").arg(account);
        }
        for arg in args {
            command.arg(arg.as_ref());
        }
        command
    }
}

fn parse_json_output(output: Output) -> TuiResult<Value> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let envelope: Value = serde_json::from_str(stdout.trim()).map_err(|err| {
        let mut message = format!("dm returned invalid JSON: {err}");
        if !stderr.trim().is_empty() {
            message.push_str(&format!("; stderr: {}", stderr.trim()));
        }
        TuiError::Cli(message)
    })?;
    if envelope.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(envelope.get("result").cloned().unwrap_or(Value::Null));
    }
    let message = envelope
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .or_else(|| {
            envelope
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str)
        })
        .unwrap_or_else(|| stderr.trim());
    Err(TuiError::Cli(message.to_owned()))
}

#[derive(Debug, Eq, PartialEq)]
struct DmInvocation {
    args: Vec<String>,
    stdin: Option<String>,
}

fn account_setup_invocation(identity: Option<String>) -> DmInvocation {
    match identity {
        Some(identity) if crate::is_nostr_secret(&identity) => DmInvocation {
            args: vec!["login".to_owned(), "--nsec-stdin".to_owned()],
            stdin: Some(format!("{identity}\n")),
        },
        Some(identity) => DmInvocation {
            args: vec!["login".to_owned(), identity],
            stdin: None,
        },
        None => DmInvocation {
            args: vec!["create-identity".to_owned()],
            stdin: None,
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AccountRow {
    account_id: String,
    npub: String,
    display_name: Option<String>,
    local_signing: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChatRow {
    group_id: String,
    name: String,
    archived: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MessageRow {
    message_id: String,
    direction: String,
    from: String,
    from_display_name: Option<String>,
    plaintext: String,
    display_text: String,
    recorded_at: u64,
    received_at: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DaemonView {
    running: bool,
    pid: Option<u64>,
    last_runtime_activity: Option<DaemonRuntimeActivityView>,
    stream_watches: Vec<DaemonStreamWatchView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonRuntimeActivityView {
    accounts: u64,
    events: u64,
    joined_groups: u64,
    messages: u64,
    errors: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonStreamWatchView {
    watch_id: String,
    group_id: String,
    stream_id: Option<String>,
    status: String,
    text: Option<String>,
    transcript_hash: Option<String>,
    chunk_count: Option<u64>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LiveStreamPreview {
    group_id: String,
    stream_id: String,
    author: String,
    status: String,
    text: String,
    error: Option<String>,
    optimistic: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupDiagnostics {
    group_id: String,
    epoch: Option<u64>,
    member_count: Option<u64>,
    components: Vec<GroupComponentDiagnostics>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupComponentDiagnostics {
    component: String,
    component_id: Option<u64>,
    data_hex: String,
}

#[derive(Debug)]
enum SubscriptionEvent {
    Result(Value),
    Error(String),
    Ended,
}

struct MessageSubscription {
    account_id: String,
    child: Child,
    rx: Receiver<SubscriptionEvent>,
}

impl Drop for MessageSubscription {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct ChatSubscription {
    account_id: String,
    include_archived: bool,
    child: Child,
    rx: Receiver<SubscriptionEvent>,
}

impl Drop for ChatSubscription {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct GroupStateSubscription {
    account_id: String,
    group_id: String,
    child: Child,
    rx: Receiver<SubscriptionEvent>,
}

impl Drop for GroupStateSubscription {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Focus {
    Accounts,
    Chats,
    Messages,
    Composer,
}

impl Focus {
    fn next(self) -> Self {
        match self {
            Self::Accounts => Self::Chats,
            Self::Chats => Self::Messages,
            Self::Messages => Self::Composer,
            Self::Composer => Self::Accounts,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Accounts => Self::Composer,
            Self::Chats => Self::Accounts,
            Self::Messages => Self::Chats,
            Self::Composer => Self::Messages,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Accounts => "accounts",
            Self::Chats => "chats",
            Self::Messages => "messages",
            Self::Composer => "composer",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SlashCommand {
    Help,
    Refresh,
    Account(String),
    AccountCreate,
    AccountAddPublic(String),
    AccountImportSecret(String),
    DaemonStatus,
    DaemonStart,
    DaemonStop,
    ChatNew {
        name: String,
        members: Vec<String>,
    },
    ChatRename(String),
    ChatDescribe(String),
    ChatArchive,
    ChatUnarchive,
    ChatArchived(bool),
    MembersAdd(Vec<String>),
    MembersRemove(Vec<String>),
    MembersList,
    KeysFetch(String),
    KeysRotate,
    ProfileName(String),
    StreamCompose {
        stream_id: Option<String>,
        quic_candidates: Vec<String>,
    },
    StreamStart {
        stream_id: Option<String>,
        quic_candidates: Vec<String>,
    },
    StreamWatch {
        stream_id: Option<String>,
        insecure_local: bool,
    },
    StreamStatus,
    StreamFinish {
        stream_id: String,
        transcript_hash: String,
        chunk_count: u64,
        text: String,
    },
    StreamVerify {
        stream_id: String,
        transcript_hash: String,
        chunk_count: Option<u64>,
    },
    Quit,
}

#[derive(Debug, Eq, PartialEq)]
struct SlashCommandSuggestion {
    usage: &'static str,
    description: &'static str,
}

const SLASH_COMMAND_SUGGESTIONS: &[SlashCommandSuggestion] = &[
    SlashCommandSuggestion {
        usage: "/help",
        description: "show full TUI help",
    },
    SlashCommandSuggestion {
        usage: "/refresh",
        description: "reload accounts and chats",
    },
    SlashCommandSuggestion {
        usage: "/account <npub-or-hex>",
        description: "select an account",
    },
    SlashCommandSuggestion {
        usage: "/create-identity",
        description: "create a local signing identity",
    },
    SlashCommandSuggestion {
        usage: "/login <nsec-or-npub>",
        description: "import or add an identity",
    },
    SlashCommandSuggestion {
        usage: "/daemon status",
        description: "show daemon state",
    },
    SlashCommandSuggestion {
        usage: "/daemon start",
        description: "start the daemon",
    },
    SlashCommandSuggestion {
        usage: "/daemon stop",
        description: "stop the daemon",
    },
    SlashCommandSuggestion {
        usage: "/chat new <name> [member-npub-or-hex ...]",
        description: "create a chat",
    },
    SlashCommandSuggestion {
        usage: "/chat rename <name>",
        description: "rename the selected chat",
    },
    SlashCommandSuggestion {
        usage: "/chat describe <description>",
        description: "update the selected chat description",
    },
    SlashCommandSuggestion {
        usage: "/chat archive",
        description: "archive the selected chat",
    },
    SlashCommandSuggestion {
        usage: "/chat unarchive",
        description: "unarchive the selected chat",
    },
    SlashCommandSuggestion {
        usage: "/chat archived [on|off]",
        description: "toggle archived chat visibility",
    },
    SlashCommandSuggestion {
        usage: "/members add <npub-or-hex> [...]",
        description: "add members to the selected chat",
    },
    SlashCommandSuggestion {
        usage: "/members remove <npub-or-hex> [...]",
        description: "remove members from the selected chat",
    },
    SlashCommandSuggestion {
        usage: "/members list",
        description: "show selected chat members",
    },
    SlashCommandSuggestion {
        usage: "/keys fetch <npub-or-hex>",
        description: "fetch another account's KeyPackage",
    },
    SlashCommandSuggestion {
        usage: "/keys rotate",
        description: "mint and publish a replacement KeyPackage",
    },
    SlashCommandSuggestion {
        usage: "/name <display-name>",
        description: "publish a profile display name",
    },
    SlashCommandSuggestion {
        usage: "/profile name <display-name>",
        description: "publish a profile display name",
    },
    SlashCommandSuggestion {
        usage: "/stream [--stream-id <id>] [--quic-candidate <url>]",
        description: "open the streaming composer",
    },
    SlashCommandSuggestion {
        usage: "/stream start <quic-candidate> [...]",
        description: "anchor an agent stream start",
    },
    SlashCommandSuggestion {
        usage: "/stream watch [stream-id] [--insecure-local]",
        description: "watch brokered stream previews",
    },
    SlashCommandSuggestion {
        usage: "/stream status",
        description: "show daemon stream-watch state",
    },
    SlashCommandSuggestion {
        usage: "/stream finish <stream-id> <transcript-hash> <chunk-count> <text>",
        description: "anchor an agent stream final",
    },
    SlashCommandSuggestion {
        usage: "/stream verify <stream-id> <transcript-hash> [chunk-count]",
        description: "verify a stream transcript",
    },
    SlashCommandSuggestion {
        usage: "/quit",
        description: "exit the TUI",
    },
];

struct TuiApp {
    client: DmClient,
    initial_account: Option<String>,
    running: bool,
    focus: Focus,
    accounts: Vec<AccountRow>,
    selected_account: usize,
    chats: Vec<ChatRow>,
    selected_chat: usize,
    messages_account_id: Option<String>,
    messages_group_id: Option<String>,
    unread_counts: HashMap<String, usize>,
    show_archived_chats: bool,
    messages: Vec<MessageRow>,
    messages_scroll: u16,
    messages_viewport: u16,
    live_stream_previews: Vec<LiveStreamPreview>,
    chat_subscription: Option<ChatSubscription>,
    message_subscription: Option<MessageSubscription>,
    group_state_subscription: Option<GroupStateSubscription>,
    daemon: DaemonView,
    group_diagnostics: Option<GroupDiagnostics>,
    input: String,
    streaming: Option<StreamComposer>,
    status: String,
    show_help: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamComposer {
    stream_id: String,
    group_id: String,
    pending_text: String,
    last_flush: Instant,
}

fn subscription_event_from_json(envelope: Value) -> SubscriptionEvent {
    if envelope.get("stream_end").and_then(Value::as_bool) == Some(true) {
        return SubscriptionEvent::Ended;
    }
    if envelope.get("ok").and_then(Value::as_bool) == Some(true) {
        return SubscriptionEvent::Result(envelope.get("result").cloned().unwrap_or(Value::Null));
    }
    if envelope.get("ok").and_then(Value::as_bool) == Some(false) {
        return SubscriptionEvent::Error(subscription_error_message(&envelope));
    }
    if let Some(result) = envelope.get("result") {
        return SubscriptionEvent::Result(result.clone());
    }
    if envelope.get("error").is_some() {
        return SubscriptionEvent::Error(subscription_error_message(&envelope));
    }
    SubscriptionEvent::Error("message subscription returned an unrecognized event".to_owned())
}

fn subscription_error_message(envelope: &Value) -> String {
    envelope
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .or_else(|| {
            envelope
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str)
        })
        .unwrap_or("message subscription failed")
        .to_owned()
}

fn spawn_subscription_reader(
    child: &mut Child,
    label: &'static str,
) -> TuiResult<Receiver<SubscriptionEvent>> {
    let Some(stdout) = child.stdout.take() else {
        return Err(TuiError::Cli(format!(
            "{label} subscription did not expose stdout"
        )));
    };
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.trim().is_empty() => {}
                Ok(line) => match serde_json::from_str::<Value>(&line) {
                    Ok(envelope) => {
                        let event = subscription_event_from_json(envelope);
                        let ended = matches!(event, SubscriptionEvent::Ended);
                        if tx.send(event).is_err() || ended {
                            return;
                        }
                    }
                    Err(err) => {
                        if tx
                            .send(SubscriptionEvent::Error(format!(
                                "invalid {label} subscription JSON: {err}"
                            )))
                            .is_err()
                        {
                            return;
                        }
                    }
                },
                Err(err) => {
                    let _ = tx.send(SubscriptionEvent::Error(err.to_string()));
                    return;
                }
            }
        }
        let _ = tx.send(SubscriptionEvent::Ended);
    });
    Ok(rx)
}

impl TuiApp {
    fn new(cli: Cli) -> TuiResult<Self> {
        let client = DmClient::from_cli(&cli)?;
        Ok(Self {
            client,
            initial_account: cli.account.clone(),
            running: true,
            focus: Focus::Composer,
            accounts: Vec::new(),
            selected_account: 0,
            chats: Vec::new(),
            selected_chat: 0,
            messages_account_id: None,
            messages_group_id: None,
            unread_counts: HashMap::new(),
            show_archived_chats: false,
            messages: Vec::new(),
            messages_scroll: 0,
            messages_viewport: 0,
            live_stream_previews: Vec::new(),
            chat_subscription: None,
            message_subscription: None,
            group_state_subscription: None,
            daemon: DaemonView::default(),
            group_diagnostics: None,
            input: String::new(),
            streaming: None,
            status: "loading accounts".to_owned(),
            show_help: false,
        })
    }

    fn run(&mut self) -> TuiResult<()> {
        let mut terminal = ratatui::init();
        let result = (|| -> TuiResult<()> {
            let _ = self.refresh_daemon_status();
            self.refresh_accounts()?;
            let mut dirty = true;
            while self.running {
                dirty |= self.tick();
                if dirty {
                    terminal.draw(|frame| self.render(frame))?;
                    dirty = false;
                }
                if event::poll(UI_EVENT_WAIT)? {
                    match event::read()? {
                        Event::Key(key) if key.kind == KeyEventKind::Press => {
                            self.handle_key(key)?;
                            dirty = true;
                        }
                        _ => {
                            dirty = true;
                        }
                    }
                }
            }
            Ok(())
        })();
        ratatui::restore();
        result
    }

    fn tick(&mut self) -> bool {
        let now = Instant::now();
        let mut changed = false;
        changed |= self.drain_chat_subscription();
        changed |= self.drain_group_state_subscription();
        changed |= self.drain_message_subscription();
        match self.flush_stream_append_if_due(now) {
            Ok(flushed) => changed |= flushed,
            Err(err) => {
                self.status = format!("stream append failed: {err}");
                changed = true;
            }
        }
        changed
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(3),
                Constraint::Length(12),
            ])
            .split(area);

        self.render_header(frame, root[0]);

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(34),
                Constraint::Length(36),
                Constraint::Min(24),
            ])
            .split(root[1]);

        self.render_accounts(frame, body[0]);
        self.render_chats(frame, body[1]);
        self.render_messages(frame, body[2]);
        self.render_composer(frame, root[2]);
        self.render_status_panel(frame, root[3]);
        self.render_slash_suggestions(frame, root[2]);

        if self.show_help {
            self.render_help(frame, centered_rect(70, 70, area));
        }
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let account = self
            .selected_account_row()
            .map(|account| shorten(&terminal_safe_text(&account_display_label(account)), 18))
            .unwrap_or_else(|| "no account".to_owned());
        let chat = self
            .selected_chat_row()
            .map(|chat| shorten(&terminal_safe_text(&chat.name), 24))
            .unwrap_or_else(|| "no chat".to_owned());
        let daemon = terminal_safe_text(&daemon_header_label(&self.daemon));
        let line = Line::from(vec![
            Span::styled(
                "dm",
                Style::default()
                    .fg(FOCUS_ACCENT)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::raw(format!("focus={}  ", self.focus.title())),
            Span::raw(format!("daemon={daemon}  ")),
            Span::raw(format!("account={account}  chat={chat}")),
        ]);
        frame.render_widget(
            Paragraph::new(line)
                .block(Block::default().borders(Borders::ALL).title("Darkmatter"))
                .alignment(Alignment::Left),
            area,
        );
    }

    fn render_accounts(&self, frame: &mut Frame, area: Rect) {
        let items = if self.accounts.is_empty() {
            vec![ListItem::new("no accounts")]
        } else {
            self.accounts
                .iter()
                .enumerate()
                .map(|(index, account)| {
                    let marker = if index == self.selected_account {
                        ">"
                    } else {
                        " "
                    };
                    let signing = if account.local_signing {
                        "local"
                    } else {
                        "public"
                    };
                    let style = selected_style(index == self.selected_account);
                    ListItem::new(Line::from(vec![
                        Span::raw(format!("{marker} ")),
                        Span::styled(
                            shorten(&terminal_safe_text(&account_display_label(account)), 22),
                            row_label_style(index == self.selected_account, ACCOUNT_ACCENT),
                        ),
                        Span::raw(format!(" {signing}")),
                    ]))
                    .style(style)
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(panel_block("Accounts", self.focus == Focus::Accounts)),
            area,
        );
    }

    fn render_chats(&self, frame: &mut Frame, area: Rect) {
        let items = if self.chats.is_empty() {
            vec![ListItem::new("no chats")]
        } else {
            self.chats
                .iter()
                .enumerate()
                .map(|(index, chat)| {
                    let selected = index == self.selected_chat;
                    let unread_count = self
                        .unread_counts
                        .get(&chat.group_id)
                        .copied()
                        .unwrap_or_default();
                    ListItem::new(chat_row_line(chat, selected, unread_count))
                        .style(selected_style(selected))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(panel_block("Chats", self.focus == Focus::Chats)),
            area,
        );
    }

    fn render_messages(&mut self, frame: &mut Frame, area: Rect) {
        let mut lines = if self.messages.is_empty() {
            vec![Line::from("no messages")]
        } else {
            message_lines(&self.messages, self.message_account_row())
        };
        let group_id = self
            .messages_group_id
            .as_deref()
            .or_else(|| self.selected_chat_row().map(|chat| chat.group_id.as_str()));
        for preview in stream_preview_lines(&self.daemon, &self.live_stream_previews, group_id) {
            lines.push(preview);
        }

        let inner_width = area.width.saturating_sub(2);
        let inner_height = area.height.saturating_sub(2);
        self.messages_viewport = inner_height;

        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
        let total = u16::try_from(paragraph.line_count(inner_width)).unwrap_or(u16::MAX);
        let (clamped_scroll, scroll_top) =
            messages_scroll_offsets(total, inner_height, self.messages_scroll);
        self.messages_scroll = clamped_scroll;

        let title = if scroll_top == 0 && self.messages_scroll == 0 {
            "Messages".to_owned()
        } else if self.messages_scroll == 0 {
            format!("Messages [{scroll_top} above]")
        } else {
            format!(
                "Messages [{scroll_top} above | {} below]",
                self.messages_scroll
            )
        };

        frame.render_widget(
            paragraph
                .block(panel_block(&title, self.focus == Focus::Messages))
                .scroll((scroll_top, 0)),
            area,
        );
    }

    fn render_composer(&self, frame: &mut Frame, area: Rect) {
        let prompt = if self.streaming.is_some() && self.input.is_empty() {
            "streaming... type text, Enter finishes, Esc cancels".to_owned()
        } else if self.input.is_empty() {
            "type a message or / for commands".to_owned()
        } else {
            composer_display_text(&self.input)
        };
        let lines = vec![Line::from(vec![
            Span::styled("> ", Style::default().fg(FOCUS_ACCENT)),
            Span::raw(terminal_safe_text(&prompt)),
        ])];
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel_block("Composer", self.focus == Focus::Composer))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_slash_suggestions(&self, frame: &mut Frame, composer_area: Rect) {
        if self.focus != Focus::Composer || self.streaming.is_some() || self.show_help {
            return;
        }
        let lines = slash_suggestion_lines(&self.input, SLASH_SUGGESTION_LIMIT);
        if lines.is_empty() || composer_area.width < 12 || composer_area.y == 0 {
            return;
        }

        let height = (lines.len() as u16 + 2).min(composer_area.y);
        let width = composer_area.width.saturating_sub(4).clamp(12, 84);
        let area = Rect {
            x: composer_area.x + (composer_area.width.saturating_sub(width) / 2),
            y: composer_area.y - height,
            width,
            height,
        };
        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::default().borders(Borders::ALL).title("Commands"))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_status_panel(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new(status_panel_lines(
                &self.status,
                self.group_diagnostics.as_ref(),
            ))
            .block(panel_block("Status", false))
            .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_help(&self, frame: &mut Frame, area: Rect) {
        let lines = vec![
            Line::from(Span::styled(
                "Darkmatter TUI",
                Style::default()
                    .fg(FOCUS_ACCENT)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("Tab cycles panels. Arrows move. Enter selects or submits. Ctrl-C quits."),
            Line::from(
                "Messages panel: Up/Down or j/k scroll; PageUp/PageDown, Home/End jump. New messages stick to the bottom.",
            ),
            Line::from(""),
            Line::from("/refresh"),
            Line::from("/account <npub-or-hex>"),
            Line::from("/create-identity"),
            Line::from("/login <nsec-or-npub>"),
            Line::from("/daemon status"),
            Line::from("/daemon start"),
            Line::from("/daemon stop"),
            Line::from("/chat new <name> [member-npub-or-hex ...]"),
            Line::from("/chat rename <name>"),
            Line::from("/chat describe <description>"),
            Line::from("/chat archive"),
            Line::from("/chat unarchive"),
            Line::from("/chat archived [on|off]"),
            Line::from("/members add <npub-or-hex> [...]"),
            Line::from("/members remove <npub-or-hex> [...]"),
            Line::from("/members list"),
            Line::from("/keys fetch <npub-or-hex>"),
            Line::from("/keys rotate"),
            Line::from("/name <display-name>"),
            Line::from("/profile name <display-name>"),
            Line::from("/quit"),
        ];
        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::default().borders(Borders::ALL).title("Help"))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_key(&mut self, key: KeyEvent) -> TuiResult<()> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.running = false;
            return Ok(());
        }
        if self.streaming.is_some() {
            // Streaming key handling (finish/cancel/append) performs fallible
            // daemon/relay operations. Mirror the non-streaming Enter path and
            // tick(): catch errors into the status line instead of propagating
            // them out of run() and tearing down the whole TUI session. The
            // composer state is preserved on failures that keep `self.streaming`
            // set, so the user can retry Enter/Esc.
            if let Err(err) = self.handle_streaming_key(key) {
                self.status = format!("error: {err}");
            }
            return Ok(());
        }

        match key.code {
            KeyCode::Char('?') if self.focus != Focus::Composer => {
                self.show_help = !self.show_help;
            }
            KeyCode::Char('q') if self.focus != Focus::Composer && self.input.is_empty() => {
                self.running = false;
            }
            KeyCode::Tab => self.focus = self.focus.next(),
            KeyCode::BackTab => self.focus = self.focus.previous(),
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Down => self.move_selection(1),
            KeyCode::PageUp => {
                let by = self.messages_page();
                self.scroll_messages_up(by);
            }
            KeyCode::PageDown => {
                let by = self.messages_page();
                self.scroll_messages_down(by);
            }
            KeyCode::Home => {
                self.messages_scroll = u16::MAX;
            }
            KeyCode::End => {
                self.messages_scroll = 0;
            }
            KeyCode::Enter => {
                if let Err(err) = self.activate_focus() {
                    self.status = format!("error: {err}");
                }
            }
            KeyCode::Esc => {
                self.show_help = false;
                self.input.clear();
            }
            KeyCode::Backspace if self.focus == Focus::Composer => {
                self.input.pop();
            }
            KeyCode::Char('/') if self.focus != Focus::Composer => {
                self.show_help = false;
                self.focus = Focus::Composer;
                self.input.push('/');
            }
            KeyCode::Char('j') if self.focus != Focus::Composer => self.move_selection(1),
            KeyCode::Char('k') if self.focus != Focus::Composer => self.move_selection(-1),
            KeyCode::Char(character) if self.focus == Focus::Composer => {
                self.show_help = false;
                self.input.push(character);
            }
            _ => {}
        }
        Ok(())
    }

    fn move_selection(&mut self, delta: isize) {
        match self.focus {
            Focus::Accounts => {
                self.selected_account =
                    move_index(self.selected_account, self.accounts.len(), delta);
            }
            Focus::Chats => {
                self.selected_chat = move_index(self.selected_chat, self.chats.len(), delta);
            }
            Focus::Messages => {
                if delta < 0 {
                    self.scroll_messages_up(1);
                } else if delta > 0 {
                    self.scroll_messages_down(1);
                }
            }
            Focus::Composer => {}
        }
    }

    fn messages_page(&self) -> u16 {
        self.messages_viewport.saturating_sub(1).max(1)
    }

    fn scroll_messages_up(&mut self, by: u16) {
        self.messages_scroll = self.messages_scroll.saturating_add(by);
    }

    fn scroll_messages_down(&mut self, by: u16) {
        self.messages_scroll = self.messages_scroll.saturating_sub(by);
    }

    fn activate_focus(&mut self) -> TuiResult<()> {
        match self.focus {
            Focus::Accounts => self.select_current_account(),
            Focus::Chats => self.refresh_messages(),
            Focus::Messages => Ok(()),
            Focus::Composer => self.submit_input(),
        }
    }

    fn submit_input(&mut self) -> TuiResult<()> {
        let input = self.input.trim().to_owned();
        self.input.clear();
        if input.is_empty() {
            return Ok(());
        }
        if input.starts_with('/') {
            let command = parse_slash_command(&input).map_err(TuiError::Cli)?;
            return self.run_slash_command(command);
        }
        self.send_message(input)
    }

    fn handle_streaming_key(&mut self, key: KeyEvent) -> TuiResult<()> {
        match key.code {
            KeyCode::Enter => self.finish_stream_composer(),
            KeyCode::Esc => self.cancel_stream_composer(),
            KeyCode::Backspace => {
                self.status =
                    "stream editing is append-only in this preview; Esc cancels".to_owned();
                Ok(())
            }
            KeyCode::Char(character) => {
                self.input.push(character);
                let mut stream_id = None;
                if let Some(streaming) = self.streaming.as_mut() {
                    streaming.pending_text.push(character);
                    stream_id = Some(streaming.stream_id.clone());
                    self.status = format!(
                        "queued {} byte(s) on {}",
                        streaming.pending_text.len(),
                        shorten(&streaming.stream_id, 18)
                    );
                }
                if let Some(stream_id) = stream_id {
                    self.upsert_active_stream_preview(&stream_id);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn run_slash_command(&mut self, command: SlashCommand) -> TuiResult<()> {
        match command {
            SlashCommand::Help => {
                self.show_help = true;
                Ok(())
            }
            SlashCommand::Refresh => self.refresh_accounts(),
            SlashCommand::Account(selector) => self.select_account_by_selector(&selector),
            SlashCommand::AccountCreate => self.create_or_import_account(None, "created identity"),
            SlashCommand::AccountAddPublic(account) => {
                self.create_or_import_account(Some(account), "logged in public identity")
            }
            SlashCommand::AccountImportSecret(secret) => {
                self.create_or_import_account(Some(secret), "logged in identity")
            }
            SlashCommand::DaemonStatus => {
                self.refresh_daemon_status()?;
                self.status = daemon_status_sentence(&self.daemon);
                Ok(())
            }
            SlashCommand::DaemonStart => self.start_daemon(),
            SlashCommand::DaemonStop => self.stop_daemon(),
            SlashCommand::ChatNew { name, members } => self.create_chat(name, members),
            SlashCommand::ChatRename(name) => self.update_selected_chat(Some(name), None),
            SlashCommand::ChatDescribe(description) => {
                self.update_selected_chat(None, Some(description))
            }
            SlashCommand::ChatArchive => self.set_selected_chat_archived(true),
            SlashCommand::ChatUnarchive => self.set_selected_chat_archived(false),
            SlashCommand::ChatArchived(include) => self.set_archived_chat_visibility(include),
            SlashCommand::MembersAdd(members) => self.add_selected_chat_members(members),
            SlashCommand::MembersRemove(members) => self.remove_selected_chat_members(members),
            SlashCommand::MembersList => self.show_selected_chat_members(),
            SlashCommand::KeysFetch(account) => {
                let result = self.client.run_json(None, &["keys", "fetch", &account])?;
                let bytes = result
                    .get("key_package_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                self.status = format!("fetched key package bytes={bytes}");
                Ok(())
            }
            SlashCommand::KeysRotate => {
                let account_id = self.require_selected_local_account()?;
                let result = self
                    .client
                    .run_json(Some(&account_id), &["keys", "rotate"])?;
                let bytes = result
                    .get("key_package_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                self.status = format!("rotated key package bytes={bytes}");
                Ok(())
            }
            SlashCommand::ProfileName(name) => self.update_profile_name(name),
            SlashCommand::StreamCompose {
                stream_id,
                quic_candidates,
            } => self.start_stream_composer(stream_id, quic_candidates),
            SlashCommand::StreamStart {
                stream_id,
                quic_candidates,
            } => self.start_stream(stream_id, quic_candidates),
            SlashCommand::StreamWatch {
                stream_id,
                insecure_local,
            } => self.watch_stream(stream_id, insecure_local),
            SlashCommand::StreamStatus => {
                self.refresh_daemon_status()?;
                self.status = stream_watch_status(&self.daemon);
                Ok(())
            }
            SlashCommand::StreamFinish {
                stream_id,
                transcript_hash,
                chunk_count,
                text,
            } => self.finish_stream(stream_id, transcript_hash, chunk_count, text),
            SlashCommand::StreamVerify {
                stream_id,
                transcript_hash,
                chunk_count,
            } => self.verify_stream(stream_id, transcript_hash, chunk_count),
            SlashCommand::Quit => {
                self.running = false;
                Ok(())
            }
        }
    }

    fn send_message(&mut self, text: String) -> TuiResult<()> {
        let account_id = self.message_account_id()?;
        let group_id = self.message_group_id()?;
        let args = vec!["message", "send", &group_id, &text];
        let result = self.client.run_json(Some(&account_id), &args)?;
        let status = publish_status("sent message", &result);
        if let Some(message_id) = result
            .get("message_ids")
            .and_then(Value::as_array)
            .and_then(|ids| ids.first())
            .and_then(Value::as_str)
        {
            let now = unix_now_seconds();
            upsert_message(
                &mut self.messages,
                MessageRow {
                    message_id: message_id.to_owned(),
                    direction: "sent".to_owned(),
                    from: account_id,
                    from_display_name: None,
                    plaintext: text.clone(),
                    display_text: text,
                    recorded_at: now,
                    received_at: now,
                },
            );
            sort_and_cap_messages(&mut self.messages);
        } else {
            self.refresh_messages()?;
        }
        self.status = status;
        Ok(())
    }

    fn start_stream_composer(
        &mut self,
        stream_id: Option<String>,
        quic_candidates: Vec<String>,
    ) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let preview_group_id = group_id.clone();
        let insecure_local =
            crate::commands::stream::first_quic_candidate_is_loopback(&quic_candidates);
        let mut args = vec!["stream".to_owned(), "compose-open".to_owned(), group_id];
        if insecure_local {
            args.push("--insecure-local".to_owned());
        }
        if let Some(stream_id) = stream_id {
            args.push("--stream-id".to_owned());
            args.push(stream_id);
        }
        for candidate in quic_candidates {
            args.push("--quic-candidate".to_owned());
            args.push(candidate);
        }
        let result = self.client.run_json(Some(&account_id), &args)?;
        let stream_id = value_string(&result, "stream_id").unwrap_or_else(|| "unknown".to_owned());
        self.streaming = Some(StreamComposer {
            stream_id: stream_id.clone(),
            group_id: preview_group_id.clone(),
            pending_text: String::new(),
            last_flush: Instant::now(),
        });
        self.input.clear();
        self.refresh_messages()?;
        upsert_live_stream_preview(
            &mut self.live_stream_previews,
            LiveStreamPreview {
                group_id: preview_group_id,
                stream_id: stream_id.clone(),
                author: "me".to_owned(),
                status: "streaming".to_owned(),
                text: String::new(),
                error: None,
                optimistic: true,
            },
            false,
        );
        self.status = format!(
            "now streaming {}; type text and press Enter to finish",
            shorten(&stream_id, 18)
        );
        Ok(())
    }

    fn upsert_active_stream_preview(&mut self, stream_id: &str) {
        let Some(group_id) = self
            .streaming
            .as_ref()
            .map(|streaming| streaming.group_id.clone())
        else {
            return;
        };
        upsert_live_stream_preview(
            &mut self.live_stream_previews,
            LiveStreamPreview {
                group_id,
                stream_id: stream_id.to_owned(),
                author: "me".to_owned(),
                status: "streaming".to_owned(),
                text: self.input.clone(),
                error: None,
                optimistic: true,
            },
            true,
        );
    }

    fn flush_stream_append_if_due(&mut self, now: Instant) -> TuiResult<bool> {
        let Some(streaming) = self.streaming.as_ref() else {
            return Ok(false);
        };
        if streaming.pending_text.is_empty()
            || now.duration_since(streaming.last_flush) < STREAM_APPEND_FLUSH_INTERVAL
        {
            return Ok(false);
        }
        self.flush_stream_append()?;
        Ok(true)
    }

    fn flush_stream_append(&mut self) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let Some((stream_id, text)) = self.streaming.as_mut().and_then(|streaming| {
            if streaming.pending_text.is_empty() {
                None
            } else {
                let text = std::mem::take(&mut streaming.pending_text);
                Some((streaming.stream_id.clone(), text))
            }
        }) else {
            return Ok(());
        };
        let args = vec![
            "stream".to_owned(),
            "compose-append".to_owned(),
            "--stream-id".to_owned(),
            stream_id.clone(),
            text.clone(),
        ];
        let result = match self.client.run_json(Some(&account_id), &args) {
            Ok(result) => result,
            Err(err) => {
                if let Some(streaming) = self.streaming.as_mut()
                    && streaming.stream_id == stream_id
                {
                    streaming.pending_text.insert_str(0, &text);
                }
                return Err(err);
            }
        };
        if let Some(streaming) = self.streaming.as_mut()
            && streaming.stream_id == stream_id
        {
            streaming.last_flush = Instant::now();
        }
        let bytes = result
            .get("text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or_default();
        self.status = format!("streaming {} bytes on {}", bytes, shorten(&stream_id, 18));
        Ok(())
    }

    fn finish_stream_composer(&mut self) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let Some(streaming) = self.streaming.take() else {
            return Ok(());
        };
        if self.input.is_empty() {
            self.streaming = Some(streaming);
            self.status = "stream text is empty; type text or Esc cancels".to_owned();
            return Ok(());
        }
        self.streaming = Some(streaming);
        self.flush_stream_append()?;
        let Some(streaming) = self.streaming.take() else {
            return Ok(());
        };
        let args = vec![
            "stream".to_owned(),
            "compose-finish".to_owned(),
            "--stream-id".to_owned(),
            streaming.stream_id.clone(),
        ];
        // Restore the composer if compose-finish fails (daemon gone, broker/QUIC
        // error, relay publish rejection — the failure class from #194). Without
        // this, `self.streaming` stays `None` while `self.input` still holds the
        // draft, so the caught error keeps the TUI alive but the next Enter sends
        // the stream draft through the normal composer path as a regular message.
        let result = match self.client.run_json(Some(&account_id), &args) {
            Ok(result) => result,
            Err(err) => {
                self.streaming = Some(streaming);
                return Err(err);
            }
        };
        self.input.clear();
        remove_live_stream_preview(
            &mut self.live_stream_previews,
            Some(streaming.group_id.as_str()),
            &streaming.stream_id,
        );
        self.refresh_messages()?;
        self.refresh_daemon_status()?;
        let chunk_count = result
            .get("chunk_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        self.status = format!(
            "finished stream {} chunks={chunk_count}",
            shorten(&streaming.stream_id, 18)
        );
        Ok(())
    }

    fn cancel_stream_composer(&mut self) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let Some(streaming) = self.streaming.take() else {
            return Ok(());
        };
        let args = vec![
            "stream".to_owned(),
            "compose-cancel".to_owned(),
            "--stream-id".to_owned(),
            streaming.stream_id.clone(),
        ];
        let _ = self.client.run_json(Some(&account_id), &args);
        self.input.clear();
        remove_live_stream_preview(
            &mut self.live_stream_previews,
            Some(streaming.group_id.as_str()),
            &streaming.stream_id,
        );
        self.status = format!("cancelled stream {}", shorten(&streaming.stream_id, 18));
        Ok(())
    }

    fn update_profile_name(&mut self, name: String) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let result = self.client.run_json(
            Some(&account_id),
            &[
                "profile",
                "update",
                "--name",
                &name,
                "--display-name",
                &name,
            ],
        )?;
        self.refresh_accounts()?;
        let label = result
            .get("profile")
            .and_then(profile_display_name_from_value)
            .unwrap_or(name);
        self.status = format!("published profile name {label}");
        Ok(())
    }

    fn create_chat(&mut self, name: String, members: Vec<String>) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let all_members = unique_member_refs(members);
        let mut args = vec!["group".to_owned(), "create".to_owned(), name];
        args.extend(all_members.iter().cloned());
        let result = self.client.run_json(Some(&account_id), &args)?;
        let group_id = value_string(&result, "group_id");
        let member_count = all_members.len();
        self.refresh_chats()?;
        if let Some(group_id) = group_id.as_deref() {
            self.select_chat_by_group_id(group_id)?;
        }
        self.status = group_id
            .as_deref()
            .map(|group_id| {
                format!(
                    "created chat {} with {} member(s)",
                    shorten(group_id, 18),
                    member_count
                )
            })
            .unwrap_or_else(|| format!("created chat with {member_count} member(s)"));
        Ok(())
    }

    fn add_selected_chat_members(&mut self, members: Vec<String>) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let members = unique_member_refs(members);
        let mut args = vec!["group".to_owned(), "invite".to_owned(), group_id];
        args.extend(members);
        let result = self.client.run_json(Some(&account_id), &args)?;
        let status = publish_status("added member(s)", &result);
        self.refresh_messages()?;
        self.status = status;
        Ok(())
    }

    fn remove_selected_chat_members(&mut self, members: Vec<String>) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let members = unique_member_refs(members);
        let mut args = vec!["group".to_owned(), "remove".to_owned(), group_id];
        args.extend(members);
        let result = self.client.run_json(Some(&account_id), &args)?;
        let status = publish_status("removed member(s)", &result);
        self.refresh_messages()?;
        self.status = status;
        Ok(())
    }

    fn update_selected_chat(
        &mut self,
        name: Option<String>,
        description: Option<String>,
    ) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let mut args = vec!["group".to_owned(), "update".to_owned(), group_id.clone()];
        if let Some(name) = name {
            args.push("--name".to_owned());
            args.push(name);
        }
        if let Some(description) = description {
            args.push("--description".to_owned());
            args.push(description);
        }
        let result = self.client.run_json(Some(&account_id), &args)?;
        let status = publish_status("updated chat", &result);
        self.refresh_chats()?;
        self.select_chat_by_group_id(&group_id)?;
        self.status = status;
        Ok(())
    }

    fn set_selected_chat_archived(&mut self, archived: bool) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let verb = if archived { "archive" } else { "unarchive" };
        self.client
            .run_json(Some(&account_id), &["chats", verb, &group_id])?;
        self.refresh_chats()?;
        self.status = if archived {
            format!("archived chat {}", shorten(&group_id, 18))
        } else {
            format!("unarchived chat {}", shorten(&group_id, 18))
        };
        Ok(())
    }

    fn show_selected_chat_members(&mut self) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let result = self
            .client
            .run_json(Some(&account_id), &["group", "members", &group_id])?;
        self.status = group_members_status(&result);
        Ok(())
    }

    fn set_archived_chat_visibility(&mut self, include: bool) -> TuiResult<()> {
        self.show_archived_chats = include;
        self.refresh_chats()?;
        self.status = if include {
            "showing archived chats".to_owned()
        } else {
            "hiding archived chats".to_owned()
        };
        Ok(())
    }

    fn create_or_import_account(
        &mut self,
        identity: Option<String>,
        action: &'static str,
    ) -> TuiResult<()> {
        let invocation = account_setup_invocation(identity);
        let result = match invocation.stdin {
            Some(stdin) => self
                .client
                .run_json_with_stdin(None, &invocation.args, &stdin)?,
            None => self.client.run_json(None, &invocation.args)?,
        };
        let selector =
            value_string(&result, "account_id").or_else(|| value_string(&result, "npub"));
        let npub = value_string(&result, "npub").unwrap_or_else(|| "unknown".to_owned());
        let result_display_name = result
            .get("profile")
            .and_then(profile_display_name_from_value)
            .or_else(|| non_empty_value_string(&result, "display_name"));
        let local_signing = result
            .get("local_signing")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        self.refresh_accounts()?;
        if let Some(selector) = selector.as_deref()
            && let Some(index) = selected_account_index(&self.accounts, Some(selector))
        {
            self.selected_account = index;
            // refresh_chats() dispatches on the selected account's local_signing
            // flag: for a local signing account it reloads chats/messages, and for
            // a public-only account it fully clears chats, messages, the
            // messages_account_id/messages_group_id targets, and the prior
            // account's subscriptions. Calling it unconditionally avoids the
            // partial-clear drift where a public-only login left stale send
            // targets pointing at the previous account/chat (issue #196).
            self.refresh_chats()?;
        }

        let signing = if local_signing {
            "local-signing"
        } else {
            "public-only"
        };
        let display_name = self
            .selected_account_row()
            .map(account_display_label)
            .or(result_display_name)
            .unwrap_or(npub);
        self.status = format!("{action} {} {signing}", shorten(&display_name, 18));
        Ok(())
    }

    fn start_stream(
        &mut self,
        stream_id: Option<String>,
        quic_candidates: Vec<String>,
    ) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let mut args = vec!["stream".to_owned(), "start".to_owned(), group_id];
        if let Some(stream_id) = stream_id {
            args.push("--stream-id".to_owned());
            args.push(stream_id);
        }
        for candidate in quic_candidates {
            args.push("--quic-candidate".to_owned());
            args.push(candidate);
        }
        let result = self.client.run_json(Some(&account_id), &args)?;
        let stream_id = value_string(&result, "stream_id").unwrap_or_else(|| "unknown".to_owned());
        let status = publish_status(
            &format!("started stream {}", shorten(&stream_id, 18)),
            &result,
        );
        self.refresh_messages()?;
        self.status = status;
        Ok(())
    }

    fn watch_stream(&mut self, stream_id: Option<String>, insecure_local: bool) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let mut args = vec![
            "stream".to_owned(),
            "watch".to_owned(),
            group_id,
            "--background".to_owned(),
        ];
        if let Some(stream_id) = stream_id {
            args.push("--stream-id".to_owned());
            args.push(stream_id);
        }
        if insecure_local {
            args.push("--insecure-local".to_owned());
        }
        let result = self.client.run_json(Some(&account_id), &args)?;
        self.refresh_daemon_status()?;
        let watch_id = value_string(&result, "watch_id").unwrap_or_else(|| "stream".to_owned());
        self.status = format!("watching stream {}", shorten(&watch_id, 24));
        Ok(())
    }

    fn finish_stream(
        &mut self,
        stream_id: String,
        transcript_hash: String,
        chunk_count: u64,
        text: String,
    ) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let args = vec![
            "stream".to_owned(),
            "finish".to_owned(),
            group_id,
            "--stream-id".to_owned(),
            stream_id.clone(),
            "--transcript-hash".to_owned(),
            transcript_hash,
            "--chunk-count".to_owned(),
            chunk_count.to_string(),
            text,
        ];
        let result = self.client.run_json(Some(&account_id), &args)?;
        let status = publish_status(
            &format!("finished stream {}", shorten(&stream_id, 18)),
            &result,
        );
        self.refresh_messages()?;
        self.status = status;
        Ok(())
    }

    fn verify_stream(
        &mut self,
        stream_id: String,
        transcript_hash: String,
        chunk_count: Option<u64>,
    ) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let mut args = vec![
            "stream".to_owned(),
            "verify".to_owned(),
            group_id,
            "--stream-id".to_owned(),
            stream_id.clone(),
            "--transcript-hash".to_owned(),
            transcript_hash,
        ];
        if let Some(chunk_count) = chunk_count {
            args.push("--chunk-count".to_owned());
            args.push(chunk_count.to_string());
        }
        let result = self.client.run_json(Some(&account_id), &args)?;
        let verified = result
            .get("verified")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.status = format!("stream {} verified={verified}", shorten(&stream_id, 18));
        Ok(())
    }

    fn refresh_daemon_status(&mut self) -> TuiResult<()> {
        let result = self.client.run_json(None, &["daemon", "status"])?;
        self.daemon = parse_daemon_view(&result);
        self.ensure_selected_chat_subscription();
        self.ensure_selected_message_subscription();
        self.ensure_selected_group_state_subscription();
        Ok(())
    }

    fn start_daemon(&mut self) -> TuiResult<()> {
        let args = vec!["daemon".to_owned(), "start".to_owned()];
        let result = self.client.run_json(None, &args)?;
        self.daemon = parse_daemon_view(&result);
        self.ensure_selected_chat_subscription();
        self.ensure_selected_message_subscription();
        self.ensure_selected_group_state_subscription();
        self.status = daemon_status_sentence(&self.daemon);
        Ok(())
    }

    fn stop_daemon(&mut self) -> TuiResult<()> {
        let result = self.client.run_json(None, &["daemon", "stop"])?;
        self.daemon = parse_daemon_view(&result);
        self.chat_subscription = None;
        self.message_subscription = None;
        self.group_state_subscription = None;
        self.status = "daemon stopped".to_owned();
        Ok(())
    }

    fn refresh_accounts(&mut self) -> TuiResult<()> {
        let result = self.client.run_json(None, &["account", "list"])?;
        let previous_account_id = self
            .selected_account_row()
            .map(|account| account.account_id.clone())
            .or_else(|| self.initial_account.clone());
        self.accounts = result
            .get("accounts")
            .and_then(Value::as_array)
            .map(|accounts| accounts.iter().filter_map(parse_account).collect())
            .unwrap_or_default();
        self.selected_account =
            selected_account_index(&self.accounts, previous_account_id.as_deref()).unwrap_or(0);
        if self.accounts.is_empty() {
            self.chats.clear();
            self.messages.clear();
            self.messages_account_id = None;
            self.messages_group_id = None;
            self.unread_counts.clear();
            self.chat_subscription = None;
            self.message_subscription = None;
            self.group_state_subscription = None;
            self.group_diagnostics = None;
            self.status = "no identities yet; create one with dm create-identity".to_owned();
            return Ok(());
        }
        self.refresh_chats()
    }

    fn refresh_chats(&mut self) -> TuiResult<()> {
        let Some(account) = self.selected_account_row().cloned() else {
            self.chats.clear();
            self.messages.clear();
            self.messages_account_id = None;
            self.messages_group_id = None;
            self.chat_subscription = None;
            self.message_subscription = None;
            self.group_state_subscription = None;
            self.group_diagnostics = None;
            self.status = "no account selected".to_owned();
            return Ok(());
        };
        if !account.local_signing {
            self.chats.clear();
            self.messages.clear();
            self.messages_account_id = None;
            self.messages_group_id = None;
            self.chat_subscription = None;
            self.message_subscription = None;
            self.group_state_subscription = None;
            self.group_diagnostics = None;
            self.status =
                "selected account is public-only; choose a local signing account".to_owned();
            return Ok(());
        }

        let previous_group_id = self.selected_chat_row().map(|chat| chat.group_id.clone());
        let mut args = vec!["chats".to_owned(), "list".to_owned()];
        if self.show_archived_chats {
            args.push("--include-archived".to_owned());
        }
        let result = self.client.run_json(Some(&account.account_id), &args)?;
        self.chats = result
            .get("chats")
            .and_then(Value::as_array)
            .map(|chats| chats.iter().filter_map(parse_chat).collect())
            .unwrap_or_default();
        retain_unread_counts_for_chats(&mut self.unread_counts, &self.chats);
        self.selected_chat =
            selected_chat_index(&self.chats, previous_group_id.as_deref()).unwrap_or(0);
        if let Err(err) = self.ensure_chat_subscription(&account.account_id) {
            self.status = format!("chat subscription failed: {err}");
        }
        if self.chats.is_empty() {
            self.messages.clear();
            self.messages_account_id = Some(account.account_id.clone());
            self.messages_group_id = None;
            self.group_state_subscription = None;
            if let Err(err) = self.ensure_message_subscription(&account.account_id) {
                self.status = format!("message subscription failed: {err}");
                return Ok(());
            }
            self.status = format!(
                "loaded account {}; no chats",
                shorten(&account_display_label(&account), 18)
            );
            return Ok(());
        }
        self.refresh_messages()
    }

    fn refresh_messages(&mut self) -> TuiResult<()> {
        let account_id = self.require_selected_local_account()?;
        let group_id = self.require_selected_group()?;
        let args = vec![
            "message".to_owned(),
            "list".to_owned(),
            "--group".to_owned(),
            group_id.clone(),
            "--limit".to_owned(),
            "50".to_owned(),
        ];
        let result = self.client.run_json(Some(&account_id), &args)?;
        self.messages = result
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| messages.iter().filter_map(parse_message).collect())
            .unwrap_or_default();
        self.messages_account_id = Some(account_id.clone());
        self.messages_group_id = Some(group_id.clone());
        self.messages_scroll = 0;
        self.unread_counts.remove(&group_id);
        sort_and_cap_messages(&mut self.messages);
        if let Err(err) = self.ensure_message_subscription(&account_id) {
            self.status = format!("message subscription failed: {err}");
            return Ok(());
        }
        let group_state_subscription_error = self
            .ensure_group_state_subscription(&account_id, &group_id)
            .err()
            .map(|err| format!("group state subscription failed: {err}"));
        if self.daemon.running && group_state_subscription_error.is_none() {
            if self
                .group_diagnostics
                .as_ref()
                .is_none_or(|diagnostics| diagnostics.group_id != group_id)
            {
                self.group_diagnostics = Some(GroupDiagnostics::unavailable(
                    &group_id,
                    "loading group state",
                ));
            }
        } else {
            self.refresh_group_diagnostics(&account_id, &group_id);
        }
        self.status = group_state_subscription_error
            .unwrap_or_else(|| format!("loaded {} message(s)", self.messages.len()));
        Ok(())
    }

    fn refresh_group_diagnostics(&mut self, account_id: &str, group_id: &str) {
        self.group_diagnostics = Some(
            match self
                .client
                .run_json(Some(account_id), &["groups", "show", group_id])
            {
                Ok(result) => parse_group_diagnostics(&result).unwrap_or_else(|| {
                    GroupDiagnostics::unavailable(
                        group_id,
                        "groups show did not return group diagnostics",
                    )
                }),
                Err(err) => GroupDiagnostics::unavailable(group_id, err.to_string()),
            },
        );
    }

    fn ensure_chat_subscription(&mut self, account_id: &str) -> TuiResult<()> {
        if !self.daemon.running {
            self.chat_subscription = None;
            return Ok(());
        }
        if self.chat_subscription.as_ref().is_some_and(|subscription| {
            subscription.account_id == account_id
                && subscription.include_archived == self.show_archived_chats
        }) {
            return Ok(());
        }

        self.chat_subscription = None;
        let args = if self.show_archived_chats {
            vec!["chats".to_owned(), "subscribe-archived".to_owned()]
        } else {
            vec!["chats".to_owned(), "subscribe".to_owned()]
        };
        let mut child = self.client.spawn_json_lines(Some(account_id), &args)?;
        let rx = spawn_subscription_reader(&mut child, "chat")?;
        self.chat_subscription = Some(ChatSubscription {
            account_id: account_id.to_owned(),
            include_archived: self.show_archived_chats,
            child,
            rx,
        });
        Ok(())
    }

    fn ensure_message_subscription(&mut self, account_id: &str) -> TuiResult<()> {
        if !self.daemon.running {
            self.message_subscription = None;
            return Ok(());
        }
        if self
            .message_subscription
            .as_ref()
            .is_some_and(|subscription| subscription.account_id == account_id)
        {
            return Ok(());
        }

        self.message_subscription = None;
        let args = message_subscription_args();
        let mut child = self.client.spawn_json_lines(Some(account_id), &args)?;
        let rx = spawn_subscription_reader(&mut child, "message")?;
        self.message_subscription = Some(MessageSubscription {
            account_id: account_id.to_owned(),
            child,
            rx,
        });
        Ok(())
    }

    fn ensure_group_state_subscription(
        &mut self,
        account_id: &str,
        group_id: &str,
    ) -> TuiResult<()> {
        if !self.daemon.running {
            self.group_state_subscription = None;
            return Ok(());
        }
        if self
            .group_state_subscription
            .as_ref()
            .is_some_and(|subscription| {
                subscription.account_id == account_id && subscription.group_id == group_id
            })
        {
            return Ok(());
        }

        self.group_state_subscription = None;
        let args = vec![
            "groups".to_owned(),
            "subscribe-state".to_owned(),
            group_id.to_owned(),
        ];
        let mut child = self.client.spawn_json_lines(Some(account_id), &args)?;
        let rx = spawn_subscription_reader(&mut child, "group state")?;
        self.group_state_subscription = Some(GroupStateSubscription {
            account_id: account_id.to_owned(),
            group_id: group_id.to_owned(),
            child,
            rx,
        });
        Ok(())
    }

    fn ensure_selected_chat_subscription(&mut self) {
        let Some(account) = self.selected_account_row().cloned() else {
            self.chat_subscription = None;
            return;
        };
        if !account.local_signing {
            self.chat_subscription = None;
            return;
        }
        if let Err(err) = self.ensure_chat_subscription(&account.account_id) {
            self.status = format!("chat subscription failed: {err}");
        }
    }

    fn ensure_selected_message_subscription(&mut self) {
        let Some(account) = self.selected_account_row().cloned() else {
            self.message_subscription = None;
            return;
        };
        if !account.local_signing {
            self.message_subscription = None;
            return;
        }
        if let Err(err) = self.ensure_message_subscription(&account.account_id) {
            self.status = format!("message subscription failed: {err}");
        }
    }

    fn ensure_selected_group_state_subscription(&mut self) {
        let Some(account) = self.selected_account_row().cloned() else {
            self.group_state_subscription = None;
            return;
        };
        if !account.local_signing {
            self.group_state_subscription = None;
            return;
        }
        let Some(group_id) = self.selected_chat_row().map(|chat| chat.group_id.clone()) else {
            self.group_state_subscription = None;
            return;
        };
        if let Err(err) = self.ensure_group_state_subscription(&account.account_id, &group_id) {
            self.status = format!("group state subscription failed: {err}");
        }
    }

    fn drain_chat_subscription(&mut self) -> bool {
        let Some(subscription) = self.chat_subscription.as_ref() else {
            return false;
        };
        let mut events = Vec::new();
        loop {
            match subscription.rx.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    events.push(SubscriptionEvent::Ended);
                    break;
                }
            }
        }
        if events.is_empty() {
            return false;
        }
        let previous_group_id = self.selected_chat_row().map(|chat| chat.group_id.clone());
        let mut chats_changed = false;
        for event in events {
            match event {
                SubscriptionEvent::Result(result) => {
                    if let Some(status) = apply_chat_subscription_result(
                        &mut self.chats,
                        &mut self.selected_chat,
                        self.show_archived_chats,
                        &result,
                    ) {
                        chats_changed = true;
                        self.status = status;
                    }
                }
                SubscriptionEvent::Error(err) => {
                    self.status = format!("chat subscription failed: {err}");
                }
                SubscriptionEvent::Ended => {
                    self.chat_subscription = None;
                    break;
                }
            }
        }
        if chats_changed {
            let selected_group_id = self.selected_chat_row().map(|chat| chat.group_id.clone());
            if previous_group_id != selected_group_id {
                self.messages.clear();
                self.messages_account_id = None;
                self.messages_group_id = None;
                self.message_subscription = None;
                self.group_state_subscription = None;
            }
            self.ensure_selected_message_subscription();
            self.ensure_selected_group_state_subscription();
        }
        true
    }

    fn drain_group_state_subscription(&mut self) -> bool {
        let Some((group_id, events)) = ({
            let Some(subscription) = self.group_state_subscription.as_ref() else {
                return false;
            };
            let mut events = Vec::new();
            loop {
                match subscription.rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        events.push(SubscriptionEvent::Ended);
                        break;
                    }
                }
            }
            if events.is_empty() {
                None
            } else {
                Some((subscription.group_id.clone(), events))
            }
        }) else {
            return false;
        };

        for event in events {
            match event {
                SubscriptionEvent::Result(result) => {
                    if let Some(update) = group_state_subscription_update(&result, &group_id) {
                        if let Some(diagnostics) = update.diagnostics {
                            self.group_diagnostics = Some(diagnostics);
                        } else {
                            self.group_diagnostics = Some(GroupDiagnostics::unavailable(
                                &update.group_id,
                                "group state update did not include diagnostics",
                            ));
                        }
                        if let Some(status) = update.status {
                            self.status = status;
                        }
                    }
                }
                SubscriptionEvent::Error(err) => {
                    self.status = format!("group state subscription failed: {err}");
                }
                SubscriptionEvent::Ended => {
                    self.group_state_subscription = None;
                    break;
                }
            }
        }
        true
    }

    fn drain_message_subscription(&mut self) -> bool {
        let Some(subscription) = self.message_subscription.as_ref() else {
            return false;
        };
        let mut events = Vec::new();
        loop {
            match subscription.rx.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    events.push(SubscriptionEvent::Ended);
                    break;
                }
            }
        }
        if events.is_empty() {
            return false;
        }
        for event in events {
            match event {
                SubscriptionEvent::Result(result) => {
                    let loaded_group_id = self.messages_group_id.clone();
                    if let Some(status) = apply_tui_subscription_result(
                        &mut self.messages,
                        &mut self.live_stream_previews,
                        &mut self.unread_counts,
                        loaded_group_id.as_deref(),
                        &result,
                    ) {
                        self.status = status;
                    }
                }
                SubscriptionEvent::Error(err) => {
                    self.status = format!("message subscription failed: {err}");
                }
                SubscriptionEvent::Ended => {
                    self.message_subscription = None;
                    break;
                }
            }
        }
        true
    }

    fn select_current_account(&mut self) -> TuiResult<()> {
        if self.accounts.is_empty() {
            return Ok(());
        }
        self.refresh_chats()
    }

    fn select_account_by_selector(&mut self, selector: &str) -> TuiResult<()> {
        let Some(index) = self
            .accounts
            .iter()
            .position(|account| account_matches(account, selector))
        else {
            return Err(TuiError::Cli(format!("account not loaded: {selector}")));
        };
        self.selected_account = index;
        self.status = format!(
            "selected account {}",
            self.selected_account_row()
                .map(|account| shorten(&account_display_label(account), 18))
                .unwrap_or_else(|| shorten(selector, 18))
        );
        self.refresh_chats()
    }

    fn select_chat_by_group_id(&mut self, group_id: &str) -> TuiResult<()> {
        let Some(index) = self.chats.iter().position(|chat| chat.group_id == group_id) else {
            return Ok(());
        };
        self.selected_chat = index;
        self.refresh_messages()
    }

    fn selected_account_row(&self) -> Option<&AccountRow> {
        self.accounts.get(self.selected_account)
    }

    fn message_account_row(&self) -> Option<&AccountRow> {
        self.messages_account_id
            .as_deref()
            .and_then(|account_id| {
                self.accounts
                    .iter()
                    .find(|account| account.account_id == account_id)
            })
            .or_else(|| self.selected_account_row())
    }

    fn selected_chat_row(&self) -> Option<&ChatRow> {
        self.chats.get(self.selected_chat)
    }

    fn message_account_id(&self) -> TuiResult<String> {
        if let Some(account_id) = &self.messages_account_id {
            return Ok(account_id.clone());
        }
        self.require_selected_local_account()
    }

    fn message_group_id(&self) -> TuiResult<String> {
        if let Some(group_id) = &self.messages_group_id {
            return Ok(group_id.clone());
        }
        self.require_selected_group()
    }

    fn require_selected_local_account(&self) -> TuiResult<String> {
        let account = self
            .selected_account_row()
            .ok_or_else(|| TuiError::Cli("no account selected".to_owned()))?;
        if !account.local_signing {
            return Err(TuiError::Cli(
                "selected account is public-only and cannot sign".to_owned(),
            ));
        }
        Ok(account.account_id.clone())
    }

    fn require_selected_group(&self) -> TuiResult<String> {
        self.selected_chat_row()
            .map(|chat| chat.group_id.clone())
            .ok_or_else(|| TuiError::Cli("no chat selected".to_owned()))
    }
}

fn parse_slash_command(input: &str) -> Result<SlashCommand, String> {
    let trimmed = input.trim();
    if !trimmed.starts_with('/') {
        return Err("slash command must start with /".to_owned());
    }
    let mut parts = split_slash_command_words(&trimmed[1..])?;
    if parts.is_empty() {
        return Err("empty slash command".to_owned());
    }
    let command = parts.remove(0);
    let rest = parts;
    match command.as_str() {
        "help" | "?" => Ok(SlashCommand::Help),
        "refresh" => Ok(SlashCommand::Refresh),
        "sync" => {
            Err("manual sync is not a TUI command; live updates come from subscriptions".to_owned())
        }
        "create-identity" => {
            if rest.is_empty() {
                Ok(SlashCommand::AccountCreate)
            } else {
                Err("/create-identity does not accept arguments".to_owned())
            }
        }
        "login" => match rest.as_slice() {
            [identity] if identity.starts_with("nsec") => {
                Ok(SlashCommand::AccountImportSecret(identity.clone()))
            }
            [identity] => Ok(SlashCommand::AccountAddPublic(identity.clone())),
            [] => Err("/login expects one nsec or npub".to_owned()),
            _ => Err("/login expects exactly one nsec or npub".to_owned()),
        },
        "account" => parse_account_command(rest),
        "daemon" => parse_daemon_command(rest),
        "chat" => parse_chat_command(rest),
        "members" => parse_members_command(rest),
        "keys" => parse_keys_command(rest),
        "profile" => parse_profile_command(rest),
        "name" => parse_profile_name_command(rest),
        "stream" => parse_stream_command(rest),
        "quit" | "q" => Ok(SlashCommand::Quit),
        other => Err(format!("unknown slash command: /{other}")),
    }
}

fn slash_command_suggestions(input: &str) -> Vec<&'static SlashCommandSuggestion> {
    if !is_slash_command_input(input) {
        return Vec::new();
    }
    SLASH_COMMAND_SUGGESTIONS
        .iter()
        .filter(|suggestion| slash_suggestion_matches(input, suggestion))
        .collect()
}

fn slash_suggestion_lines(input: &str, limit: usize) -> Vec<Line<'static>> {
    if !is_slash_command_input(input) {
        return Vec::new();
    }

    let suggestions = slash_command_suggestions(input);
    if suggestions.is_empty() {
        return vec![Line::from(Span::styled(
            "no matching commands",
            Style::default().fg(Color::DarkGray),
        ))];
    }

    suggestions
        .into_iter()
        .take(limit)
        .map(|suggestion| {
            Line::from(vec![
                Span::styled(
                    suggestion.usage,
                    Style::default()
                        .fg(FOCUS_ACCENT)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::raw(suggestion.description),
            ])
        })
        .collect()
}

fn is_slash_command_input(input: &str) -> bool {
    input.starts_with('/')
}

fn slash_suggestion_matches(input: &str, suggestion: &SlashCommandSuggestion) -> bool {
    let typed_words = input
        .to_ascii_lowercase()
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if typed_words.is_empty() {
        return true;
    }

    let literal_words = suggestion
        .usage
        .split_whitespace()
        .take_while(|word| !word.starts_with('<') && !word.starts_with('['))
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();

    for (index, typed_word) in typed_words.iter().enumerate() {
        let Some(literal_word) = literal_words.get(index) else {
            return slash_suggestion_accepts_arguments(suggestion);
        };
        if !literal_word.starts_with(typed_word) {
            return false;
        }
    }
    true
}

fn slash_suggestion_accepts_arguments(suggestion: &SlashCommandSuggestion) -> bool {
    suggestion
        .usage
        .split_whitespace()
        .any(|word| word.starts_with('<') || word.starts_with('['))
}

fn split_slash_command_words(input: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut word_started = false;

    for ch in input.chars() {
        match quote {
            Some(quote_ch) if ch == quote_ch => {
                quote = None;
                word_started = true;
            }
            Some(_) => {
                word.push(ch);
                word_started = true;
            }
            None if ch.is_whitespace() => {
                if word_started {
                    words.push(std::mem::take(&mut word));
                    word_started = false;
                }
            }
            None if matches!(ch, '"' | '\'') && !word_started => {
                quote = Some(ch);
                word_started = true;
            }
            None => {
                word.push(ch);
                word_started = true;
            }
        }
    }

    if quote.is_some() {
        return Err("unterminated quoted string".to_owned());
    }
    if word_started {
        words.push(word);
    }
    Ok(words)
}

fn parse_chat_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command, name, members @ ..] if command == "new" => Ok(SlashCommand::ChatNew {
            name: name.clone(),
            members: members.to_vec(),
        }),
        [command] if command == "new" => Err("/chat new requires a name".to_owned()),
        [command, name @ ..] if command == "rename" && !name.is_empty() => {
            Ok(SlashCommand::ChatRename(name.join(" ")))
        }
        [command] if command == "rename" => Err("/chat rename requires a name".to_owned()),
        [command, description @ ..] if command == "describe" && !description.is_empty() => {
            Ok(SlashCommand::ChatDescribe(description.join(" ")))
        }
        [command] if command == "describe" => {
            Err("/chat describe requires a description".to_owned())
        }
        [command] if command == "archive" => Ok(SlashCommand::ChatArchive),
        [command] if command == "unarchive" => Ok(SlashCommand::ChatUnarchive),
        [command] if command == "archived" => Ok(SlashCommand::ChatArchived(true)),
        [command, value] if command == "archived" => {
            parse_on_off(value).map(SlashCommand::ChatArchived)
        }
        [] => {
            Err("/chat expects new, rename, describe, archive, unarchive, or archived".to_owned())
        }
        _ => Err("/chat expects new, rename, describe, archive, unarchive, or archived".to_owned()),
    }
}

fn parse_members_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command, members @ ..] if command == "add" && !members.is_empty() => {
            Ok(SlashCommand::MembersAdd(members.to_vec()))
        }
        [command] if command == "add" => {
            Err("/members add requires at least one member".to_owned())
        }
        [command, members @ ..] if command == "remove" && !members.is_empty() => {
            Ok(SlashCommand::MembersRemove(members.to_vec()))
        }
        [command] if command == "remove" => {
            Err("/members remove requires at least one member".to_owned())
        }
        [command] if command == "list" => Ok(SlashCommand::MembersList),
        [command, ..] if command == "list" => {
            Err("/members list does not accept arguments".to_owned())
        }
        [] => Err("/members expects add, remove, or list".to_owned()),
        _ => Err("/members expects add, remove, or list".to_owned()),
    }
}

fn parse_daemon_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command] if command == "status" => Ok(SlashCommand::DaemonStatus),
        [command] if command == "start" => Ok(SlashCommand::DaemonStart),
        [command, ..] if command == "start" => {
            Err("/daemon start does not accept arguments".to_owned())
        }
        [command] if command == "stop" => Ok(SlashCommand::DaemonStop),
        [] => Err("/daemon expects status, start, or stop".to_owned()),
        [command, ..] if command == "status" => {
            Err("/daemon status does not accept arguments".to_owned())
        }
        [command, ..] if command == "stop" => {
            Err("/daemon stop does not accept arguments".to_owned())
        }
        _ => Err("/daemon expects status, start, or stop".to_owned()),
    }
}

fn parse_account_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command] if matches!(command.as_str(), "create" | "add" | "import") => {
            Err("/account only selects identities; use /create-identity or /login".to_owned())
        }
        [selector] => Ok(SlashCommand::Account(selector.clone())),
        [] => Err("/account expects a selector".to_owned()),
        _ => Err("/account expects exactly one selector".to_owned()),
    }
}

fn parse_keys_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command, account] if command == "fetch" => Ok(SlashCommand::KeysFetch(account.clone())),
        [command] if command == "rotate" => Ok(SlashCommand::KeysRotate),
        _ => Err("/keys expects 'fetch <npub-or-hex>' or 'rotate'".to_owned()),
    }
}

fn parse_profile_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command, name @ ..] if command == "name" && !name.is_empty() => {
            Ok(SlashCommand::ProfileName(name.join(" ")))
        }
        [command] if command == "name" => Err("/profile name requires a name".to_owned()),
        [] => Err("/profile expects name <display-name>".to_owned()),
        _ => Err("/profile expects name <display-name>".to_owned()),
    }
}

fn parse_profile_name_command(args: Vec<String>) -> Result<SlashCommand, String> {
    if args.is_empty() {
        return Err("/name requires a name".to_owned());
    }
    Ok(SlashCommand::ProfileName(args.join(" ")))
}

fn parse_stream_command(args: Vec<String>) -> Result<SlashCommand, String> {
    match args.as_slice() {
        [command, rest @ ..] if command == "start" => parse_stream_start(rest),
        [command, rest @ ..] if command == "watch" => parse_stream_watch(rest),
        [command] if command == "status" => Ok(SlashCommand::StreamStatus),
        [command, ..] if command == "status" => {
            Err("/stream status does not accept arguments".to_owned())
        }
        [command, stream_id, transcript_hash, chunk_count, text @ ..]
            if command == "finish" && !text.is_empty() =>
        {
            let chunk_count = chunk_count
                .parse::<u64>()
                .map_err(|_| "/stream finish chunk-count must be an integer".to_owned())?;
            Ok(SlashCommand::StreamFinish {
                stream_id: stream_id.clone(),
                transcript_hash: transcript_hash.clone(),
                chunk_count,
                text: text.join(" "),
            })
        }
        [command, ..] if command == "finish" => Err(
            "/stream finish expects <stream-id> <transcript-hash> <chunk-count> <text>".to_owned(),
        ),
        [command, stream_id, transcript_hash] if command == "verify" => {
            Ok(SlashCommand::StreamVerify {
                stream_id: stream_id.clone(),
                transcript_hash: transcript_hash.clone(),
                chunk_count: None,
            })
        }
        [command, stream_id, transcript_hash, chunk_count] if command == "verify" => {
            let chunk_count = chunk_count
                .parse::<u64>()
                .map_err(|_| "/stream verify chunk-count must be an integer".to_owned())?;
            Ok(SlashCommand::StreamVerify {
                stream_id: stream_id.clone(),
                transcript_hash: transcript_hash.clone(),
                chunk_count: Some(chunk_count),
            })
        }
        [command, ..] if command == "verify" => {
            Err("/stream verify expects <stream-id> <transcript-hash> [chunk-count]".to_owned())
        }
        rest => parse_stream_compose(rest),
    }
}

fn parse_stream_compose(args: &[String]) -> Result<SlashCommand, String> {
    let mut stream_id = None;
    let mut quic_candidates = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--stream-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err("/stream --stream-id requires a value".to_owned());
                };
                stream_id = Some(value.clone());
            }
            "--quic-candidate" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err("/stream --quic-candidate requires a value".to_owned());
                };
                quic_candidates.push(value.clone());
            }
            value if value.starts_with("--") => {
                return Err(format!("unknown /stream option: {value}"));
            }
            value => quic_candidates.push(value.to_owned()),
        }
        index += 1;
    }
    if quic_candidates.is_empty() {
        quic_candidates.push(DEFAULT_STREAM_CANDIDATE.to_owned());
    }
    Ok(SlashCommand::StreamCompose {
        stream_id,
        quic_candidates,
    })
}

fn parse_stream_start(args: &[String]) -> Result<SlashCommand, String> {
    let mut stream_id = None;
    let mut quic_candidates = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--stream-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err("/stream start --stream-id requires a value".to_owned());
                };
                stream_id = Some(value.clone());
            }
            "--quic-candidate" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err("/stream start --quic-candidate requires a value".to_owned());
                };
                quic_candidates.push(value.clone());
            }
            value if value.starts_with("--") => {
                return Err(format!("unknown /stream start option: {value}"));
            }
            value => quic_candidates.push(value.to_owned()),
        }
        index += 1;
    }
    if quic_candidates.is_empty() {
        return Err("/stream start requires at least one QUIC candidate".to_owned());
    }
    Ok(SlashCommand::StreamStart {
        stream_id,
        quic_candidates,
    })
}

fn parse_stream_watch(args: &[String]) -> Result<SlashCommand, String> {
    let mut stream_id = None;
    let mut insecure_local = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--stream-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err("/stream watch --stream-id requires a value".to_owned());
                };
                stream_id = Some(value.clone());
            }
            "--insecure-local" => insecure_local = true,
            value if value.starts_with("--") => {
                return Err(format!("unknown /stream watch option: {value}"));
            }
            value if stream_id.is_none() => stream_id = Some(value.to_owned()),
            _ => return Err("/stream watch accepts at most one stream id".to_owned()),
        }
        index += 1;
    }
    Ok(SlashCommand::StreamWatch {
        stream_id,
        insecure_local,
    })
}

fn parse_on_off(value: &str) -> Result<bool, String> {
    match value {
        "on" | "true" | "yes" => Ok(true),
        "off" | "false" | "no" => Ok(false),
        _ => Err("expected on or off".to_owned()),
    }
}

fn parse_account(value: &Value) -> Option<AccountRow> {
    Some(AccountRow {
        account_id: value_string(value, "account_id")?,
        npub: value_string(value, "npub")?,
        display_name: non_empty_value_string(value, "display_name").or_else(|| {
            value
                .get("profile")
                .and_then(profile_display_name_from_value)
        }),
        local_signing: value.get("local_signing").and_then(Value::as_bool)?,
    })
}

fn parse_chat(value: &Value) -> Option<ChatRow> {
    let profile = value.get("profile")?;
    Some(ChatRow {
        group_id: value_string(value, "group_id")?,
        name: value_string(profile, "name").unwrap_or_else(|| "unnamed".to_owned()),
        archived: value
            .get("archived")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_message(value: &Value) -> Option<MessageRow> {
    let plaintext = value_string(value, "plaintext")?;
    if value
        .get("agent_text_stream")
        .and_then(|stream| stream.get("kind"))
        .and_then(Value::as_str)
        == Some("start")
    {
        return None;
    }
    let display_text = if value.get("kind").and_then(Value::as_u64) == Some(GROUP_SYSTEM_KIND) {
        group_system_summary(value, &plaintext).unwrap_or_else(|| plaintext.clone())
    } else {
        value
            .get("agent_text_stream")
            .and_then(agent_text_stream_summary)
            .unwrap_or_else(|| plaintext.clone())
    };
    Some(MessageRow {
        message_id: value_string(value, "message_id").unwrap_or_default(),
        direction: value_string(value, "direction").unwrap_or_else(|| "received".to_owned()),
        from: value_string(value, "from").unwrap_or_else(|| "unknown".to_owned()),
        from_display_name: non_empty_value_string(value, "from_display_name"),
        plaintext,
        display_text,
        recorded_at: value
            .get("recorded_at")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        received_at: value
            .get("received_at")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn sort_messages_chronologically(messages: &mut [MessageRow]) {
    messages.sort_by(|left, right| {
        left.recorded_at
            .cmp(&right.recorded_at)
            .then_with(|| left.received_at.cmp(&right.received_at))
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
}

fn sort_and_cap_messages(messages: &mut Vec<MessageRow>) {
    sort_messages_chronologically(messages);
    cap_message_scrollback(messages);
}

fn cap_message_scrollback(messages: &mut Vec<MessageRow>) {
    if messages.len() <= TUI_MESSAGE_SCROLLBACK_LIMIT {
        return;
    }
    let excess = messages.len() - TUI_MESSAGE_SCROLLBACK_LIMIT;
    messages.drain(0..excess);
}

/// Inner app-event kind for durable group system rows (membership/admin/profile).
const GROUP_SYSTEM_KIND: u64 = 1210;

/// Friendly one-line rendering of a kind-1210 group system row from its JSON
/// content, e.g. "alice added bob". Falls back to the embedded `text` field, or
/// `None` when the content is not a parseable group system event.
fn group_system_summary(value: &Value, plaintext: &str) -> Option<String> {
    let content: Value = serde_json::from_str(plaintext).ok()?;
    let system_type = content.get("system_type").and_then(Value::as_str)?;
    let data = content.get("data");
    // `actor` is absent for unattributed changes (e.g. a convergence reorg,
    // where the committer isn't resolved). Render the passive voice then rather
    // than implying an unknown actor performed the action.
    let actor = non_empty_value_string(value, "from_display_name").or_else(|| {
        value_string(value, "from")
            .filter(|from| !from.is_empty())
            .map(|from| shorten(&from, 12))
    });
    let subject = data
        .and_then(|data| data.get("subject"))
        .and_then(Value::as_str)
        .map_or_else(|| "someone".to_owned(), |subject| shorten(subject, 12));
    let name = data
        .and_then(|data| data.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let summary = match (system_type, actor.as_deref()) {
        ("member_added", Some(actor)) => format!("{actor} added {subject}"),
        ("member_added", None) => format!("{subject} was added"),
        ("member_removed", Some(actor)) => format!("{actor} removed {subject}"),
        ("member_removed", None) => format!("{subject} was removed"),
        ("member_left", Some(actor)) => format!("{actor} left"),
        ("member_left", None) => format!("{subject} left"),
        ("admin_added", Some(actor)) => format!("{actor} made {subject} an admin"),
        ("admin_added", None) => format!("{subject} was made an admin"),
        ("admin_removed", Some(actor)) => format!("{actor} removed {subject} as admin"),
        ("admin_removed", None) => format!("{subject} is no longer an admin"),
        ("group_renamed", Some(actor)) => format!("{actor} renamed the group to \"{name}\""),
        ("group_renamed", None) => format!("the group was renamed to \"{name}\""),
        ("group_avatar_changed", Some(actor)) => format!("{actor} changed the group avatar"),
        ("group_avatar_changed", None) => "the group avatar changed".to_owned(),
        _ => content
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or(system_type)
            .to_owned(),
    };
    Some(summary)
}

fn agent_text_stream_summary(value: &Value) -> Option<String> {
    let stream_id = value_string(value, "stream_id")
        .map(|stream_id| shorten(&stream_id, 18))
        .unwrap_or_else(|| "unknown".to_owned());
    match value.get("kind").and_then(Value::as_str)? {
        "start" => {
            let route = value_string(value, "route").unwrap_or_else(|| "unknown".to_owned());
            let candidates = value
                .get("quic_candidates")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            Some(format!(
                "stream start {stream_id} route={route} candidates={candidates}"
            ))
        }
        "final" => {
            let text = value_string(value, "final_text_or_reference")
                .filter(|text| !text.is_empty())
                .unwrap_or_else(|| format!("stream final {stream_id}"));
            Some(text)
        }
        _ => None,
    }
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn non_empty_value_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn profile_display_name_from_value(value: &Value) -> Option<String> {
    non_empty_value_string(value, "display_name")
        .or_else(|| non_empty_value_string(value, "displayName"))
        .or_else(|| non_empty_value_string(value, "name"))
}

fn account_display_label(account: &AccountRow) -> String {
    account
        .display_name
        .clone()
        .unwrap_or_else(|| account.npub.clone())
}

fn message_author_label(message: &MessageRow, selected_account: Option<&AccountRow>) -> String {
    if message.direction == "sent" {
        return "me".to_owned();
    }
    if selected_account.is_some_and(|account| {
        message.from == account.account_id
            || message.from == account.npub
            || message.from == account_display_label(account)
    }) {
        return "me".to_owned();
    }
    message
        .from_display_name
        .clone()
        .unwrap_or_else(|| shorten(&message.from, 18))
}

fn stream_preview_author(message: &Value, selected_account: Option<&AccountRow>) -> String {
    let direction = value_string(message, "direction").unwrap_or_else(|| "received".to_owned());
    let from = value_string(message, "from").unwrap_or_else(|| "stream".to_owned());
    if direction == "sent" {
        return "me".to_owned();
    }
    if selected_account.is_some_and(|account| {
        from == account.account_id || from == account.npub || from == account_display_label(account)
    }) {
        return "me".to_owned();
    }
    non_empty_value_string(message, "from_display_name").unwrap_or_else(|| shorten(&from, 18))
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn selected_account_index(accounts: &[AccountRow], selector: Option<&str>) -> Option<usize> {
    selector.and_then(|selector| {
        accounts
            .iter()
            .position(|account| account_matches(account, selector))
    })
}

fn selected_chat_index(chats: &[ChatRow], group_id: Option<&str>) -> Option<usize> {
    group_id.and_then(|group_id| chats.iter().position(|chat| chat.group_id == group_id))
}

fn apply_chat_subscription_result(
    chats: &mut Vec<ChatRow>,
    selected_chat: &mut usize,
    show_archived_chats: bool,
    result: &Value,
) -> Option<String> {
    if result.get("type").and_then(Value::as_str) != Some("chat") {
        return None;
    }
    let chat = result.get("chat").and_then(parse_chat)?;
    let previous_group_id = chats.get(*selected_chat).map(|chat| chat.group_id.clone());
    upsert_chat(chats, chat, show_archived_chats);
    *selected_chat = selected_chat_index(chats, previous_group_id.as_deref())
        .unwrap_or_else(|| (*selected_chat).min(chats.len().saturating_sub(1)));
    Some(format!("live chat update: chats={}", chats.len()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupStateSubscriptionUpdate {
    group_id: String,
    status: Option<String>,
    diagnostics: Option<GroupDiagnostics>,
}

fn group_state_subscription_update(
    result: &Value,
    selected_group_id: &str,
) -> Option<GroupStateSubscriptionUpdate> {
    if result.get("type").and_then(Value::as_str) != Some("group_state") {
        return None;
    }
    let group_id = value_string(result, "group_id").or_else(|| {
        result
            .get("group")
            .and_then(|group| value_string(group, "group_id"))
    })?;
    if group_id != selected_group_id {
        return None;
    }
    let status = if result.get("trigger").and_then(Value::as_str) == Some("InitialGroupState") {
        None
    } else {
        Some(format!(
            "live group state update: {}",
            group_state_subscription_label(result, &group_id)
        ))
    };
    let diagnostics = parse_group_diagnostics(result);
    Some(GroupStateSubscriptionUpdate {
        group_id,
        status,
        diagnostics,
    })
}

fn group_state_subscription_label(result: &Value, group_id: &str) -> String {
    result
        .get("group")
        .and_then(parse_chat)
        .map(|chat| shorten(&chat.name, 18))
        .unwrap_or_else(|| shorten(group_id, 18))
}

fn upsert_chat(chats: &mut Vec<ChatRow>, chat: ChatRow, show_archived_chats: bool) {
    if chat.archived && !show_archived_chats {
        chats.retain(|existing| existing.group_id != chat.group_id);
        return;
    }
    if let Some(existing) = chats
        .iter_mut()
        .find(|existing| existing.group_id == chat.group_id)
    {
        *existing = chat;
    } else {
        chats.push(chat);
    }
}

fn account_matches(account: &AccountRow, selector: &str) -> bool {
    account.account_id == selector || account.npub == selector
}

fn move_index(current: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    let max = len.saturating_sub(1) as isize;
    (current as isize + delta).clamp(0, max) as usize
}

// `scrollback` counts lines up from the bottom (0 keeps the newest pinned). Returns the
// clamped scrollback and the top-line offset to hand to `Paragraph::scroll`.
fn messages_scroll_offsets(total: u16, viewport: u16, scrollback: u16) -> (u16, u16) {
    let max_scroll = total.saturating_sub(viewport);
    let clamped = scrollback.min(max_scroll);
    (clamped, max_scroll - clamped)
}

fn publish_status(action: &str, result: &Value) -> String {
    let published = result
        .get("published")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    format!("{action}; published={published}")
}

fn parse_daemon_view(value: &Value) -> DaemonView {
    DaemonView {
        running: value
            .get("running")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        pid: value.get("pid").and_then(Value::as_u64),
        last_runtime_activity: value
            .get("last_runtime_activity")
            .and_then(parse_daemon_runtime_activity_view),
        stream_watches: value
            .get("stream_watches")
            .and_then(Value::as_array)
            .map(|watches| {
                watches
                    .iter()
                    .filter_map(parse_daemon_stream_watch)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn parse_daemon_runtime_activity_view(value: &Value) -> Option<DaemonRuntimeActivityView> {
    Some(DaemonRuntimeActivityView {
        accounts: value.get("accounts").and_then(Value::as_u64)?,
        events: value.get("events").and_then(Value::as_u64).unwrap_or(0),
        joined_groups: value
            .get("joined_groups")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        messages: value.get("messages").and_then(Value::as_u64).unwrap_or(0),
        errors: value
            .get("errors")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
    })
}

fn parse_daemon_stream_watch(value: &Value) -> Option<DaemonStreamWatchView> {
    Some(DaemonStreamWatchView {
        watch_id: value_string(value, "watch_id")?,
        group_id: value_string(value, "group_id")?,
        stream_id: value_string(value, "stream_id"),
        status: value_string(value, "status").unwrap_or_else(|| "unknown".to_owned()),
        text: value_string(value, "text"),
        transcript_hash: value_string(value, "transcript_hash"),
        chunk_count: value.get("chunk_count").and_then(Value::as_u64),
        error: value_string(value, "error"),
    })
}

fn apply_tui_subscription_result(
    messages: &mut Vec<MessageRow>,
    live_previews: &mut Vec<LiveStreamPreview>,
    unread_counts: &mut HashMap<String, usize>,
    selected_group_id: Option<&str>,
    result: &Value,
) -> Option<String> {
    if is_initial_subscription_result(result) {
        return None;
    }
    if let Some(group_id) = subscription_result_group_id(result)
        && Some(group_id.as_str()) != selected_group_id
    {
        if subscription_result_counts_as_unread(result) {
            let unread_count = unread_counts.entry(group_id.clone()).or_default();
            *unread_count += 1;
            let status = Some(format!(
                "unread message in {}; count={}",
                shorten(&group_id, 18),
                unread_count
            ));
            let _ = apply_subscription_result(messages, live_previews, result, true);
            return status;
        }
        let _ = apply_subscription_result(messages, live_previews, result, true);
        return None;
    }
    apply_subscription_result(messages, live_previews, result, false)
}

fn is_initial_subscription_result(result: &Value) -> bool {
    matches!(
        result.get("trigger").and_then(Value::as_str),
        Some("InitialMessage" | "InitialAgentStreamWatch")
    )
}

fn subscription_result_group_id(result: &Value) -> Option<String> {
    match result.get("type").and_then(Value::as_str) {
        Some(
            "message" | "reaction" | "message_delete" | "media" | "agent_stream_start"
            | "agent_stream_final",
        ) => result
            .get("message")
            .and_then(|message| value_string(message, "group_id")),
        Some("agent_stream_delta") => result
            .get("agent_stream_delta")
            .and_then(|delta| value_string(delta, "group_id")),
        Some("stream_preview") => result
            .get("stream_preview")
            .and_then(|preview| value_string(preview, "group_id")),
        _ => None,
    }
}

fn subscription_result_counts_as_unread(result: &Value) -> bool {
    matches!(
        result.get("type").and_then(Value::as_str),
        Some("message" | "reaction" | "media" | "agent_stream_final")
    )
}

fn apply_subscription_result(
    messages: &mut Vec<MessageRow>,
    live_previews: &mut Vec<LiveStreamPreview>,
    result: &Value,
    suppress_message_append: bool,
) -> Option<String> {
    match result.get("type").and_then(Value::as_str) {
        Some("message" | "reaction" | "message_delete" | "media" | "agent_stream_final") => {
            let message_value = result.get("message")?;
            if result.get("type").and_then(Value::as_str) == Some("agent_stream_final")
                && let Some(stream_id) = message_value
                    .get("agent_text_stream")
                    .and_then(|stream| value_string(stream, "stream_id"))
            {
                let group_id = value_string(message_value, "group_id");
                remove_live_stream_preview(live_previews, group_id.as_deref(), &stream_id);
            }
            if suppress_message_append {
                return None;
            }
            let message = parse_message(message_value)?;
            upsert_message(messages, message);
            sort_and_cap_messages(messages);
            Some(format!("live update: messages={}", messages.len()))
        }
        Some("agent_stream_start") => {
            let message = result.get("message")?;
            let stream = message
                .get("agent_text_stream")
                .and_then(|stream| value_string(stream, "stream_id"))?;
            let group_id = value_string(message, "group_id")?;
            let author = stream_preview_author(message, None);
            upsert_live_stream_preview(
                live_previews,
                LiveStreamPreview {
                    group_id,
                    stream_id: stream.clone(),
                    author,
                    status: "streaming".to_owned(),
                    text: String::new(),
                    error: None,
                    optimistic: false,
                },
                false,
            );
            Some(format!("stream started {}", shorten(&stream, 18)))
        }
        Some("agent_stream_delta") => {
            let delta = result.get("agent_stream_delta")?;
            let group_id = value_string(delta, "group_id")?;
            let stream_id = value_string(delta, "stream_id")?;
            let text = value_string(delta, "text").unwrap_or_default();
            append_live_stream_delta(live_previews, group_id, stream_id.clone(), text);
            Some(format!("streaming {}", shorten(&stream_id, 18)))
        }
        Some("stream_preview") => {
            let preview = result.get("stream_preview")?;
            let group_id = value_string(preview, "group_id")?;
            let stream_id =
                value_string(preview, "stream_id").or_else(|| value_string(preview, "watch_id"))?;
            let status = value_string(preview, "status").unwrap_or_else(|| "streaming".to_owned());
            let text = value_string(preview, "text").unwrap_or_default();
            let error = value_string(preview, "error");
            upsert_live_stream_preview(
                live_previews,
                LiveStreamPreview {
                    group_id,
                    stream_id: stream_id.clone(),
                    author: "stream".to_owned(),
                    status: status.clone(),
                    text,
                    error,
                    optimistic: false,
                },
                true,
            );
            Some(format!("stream {status} {}", shorten(&stream_id, 18)))
        }
        _ => None,
    }
}

fn upsert_message(messages: &mut Vec<MessageRow>, message: MessageRow) {
    if !message.message_id.is_empty()
        && let Some(existing) = messages
            .iter_mut()
            .find(|existing| existing.message_id == message.message_id)
    {
        *existing = message;
        return;
    }
    messages.push(message);
}

fn append_live_stream_delta(
    live_previews: &mut Vec<LiveStreamPreview>,
    group_id: String,
    stream_id: String,
    text: String,
) {
    if let Some(preview) = live_previews
        .iter_mut()
        .find(|preview| preview.group_id == group_id && preview.stream_id == stream_id)
    {
        if preview.optimistic {
            return;
        }
        preview.status = "streaming".to_owned();
        preview.text.push_str(&text);
        cap_live_stream_text(&mut preview.text);
        preview.error = None;
        return;
    }
    let mut preview = LiveStreamPreview {
        group_id,
        stream_id,
        author: "stream".to_owned(),
        status: "streaming".to_owned(),
        text,
        error: None,
        optimistic: false,
    };
    cap_live_stream_preview(&mut preview);
    live_previews.push(preview);
    cap_live_stream_previews(live_previews);
}

fn upsert_live_stream_preview(
    live_previews: &mut Vec<LiveStreamPreview>,
    mut preview: LiveStreamPreview,
    replace_text: bool,
) {
    cap_live_stream_preview(&mut preview);
    if let Some(existing) = live_previews.iter_mut().find(|existing| {
        existing.group_id == preview.group_id && existing.stream_id == preview.stream_id
    }) {
        if existing.optimistic && !preview.optimistic && !replace_text {
            existing.status = preview.status;
            existing.error = preview.error;
            return;
        }
        existing.author = preview.author;
        existing.status = preview.status;
        existing.error = preview.error;
        existing.optimistic = preview.optimistic;
        if replace_text || existing.text.is_empty() {
            existing.text = preview.text;
        }
        cap_live_stream_preview(existing);
        return;
    }
    live_previews.push(preview);
    cap_live_stream_previews(live_previews);
}

fn cap_live_stream_previews(live_previews: &mut Vec<LiveStreamPreview>) {
    if live_previews.len() <= TUI_LIVE_STREAM_PREVIEW_LIMIT {
        return;
    }
    let excess = live_previews.len() - TUI_LIVE_STREAM_PREVIEW_LIMIT;
    live_previews.drain(0..excess);
}

fn cap_live_stream_preview(preview: &mut LiveStreamPreview) {
    cap_live_stream_text(&mut preview.text);
}

fn cap_live_stream_text(text: &mut String) {
    if text.len() <= TUI_LIVE_STREAM_TEXT_LIMIT {
        return;
    }
    let mut start = text.len() - TUI_LIVE_STREAM_TEXT_LIMIT;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text.drain(..start);
}

fn remove_live_stream_preview(
    live_previews: &mut Vec<LiveStreamPreview>,
    group_id: Option<&str>,
    stream_id: &str,
) {
    live_previews.retain(|preview| {
        if preview.stream_id != stream_id {
            return true;
        }
        if let Some(group_id) = group_id {
            return preview.group_id != group_id;
        }
        false
    });
}

fn daemon_header_label(daemon: &DaemonView) -> String {
    if !daemon.running {
        return "off".to_owned();
    }
    let mut label = daemon
        .pid
        .map(|pid| format!("on pid={pid}"))
        .unwrap_or_else(|| "on".to_owned());
    if let Some(activity) = &daemon.last_runtime_activity {
        label.push_str(&format!(
            " activity={}/{}/{}",
            activity.events, activity.joined_groups, activity.messages
        ));
        if activity.errors > 0 {
            label.push_str(&format!(" errors={}", activity.errors));
        }
    }
    let active_streams = daemon
        .stream_watches
        .iter()
        .filter(|watch| watch.status == "running")
        .count();
    if active_streams > 0 {
        label.push_str(&format!(" streams={active_streams}"));
    }
    label
}

fn daemon_status_sentence(daemon: &DaemonView) -> String {
    if !daemon.running {
        return "daemon not running".to_owned();
    }
    let activity = daemon
        .last_runtime_activity
        .as_ref()
        .map(|activity| {
            format!(
                " last-activity accounts={} events={} joined={} messages={} errors={}",
                activity.accounts,
                activity.events,
                activity.joined_groups,
                activity.messages,
                activity.errors
            )
        })
        .unwrap_or_default();
    let streams = stream_watch_status(daemon);
    let streams = if streams == "streams: none" {
        String::new()
    } else {
        format!(" {streams}")
    };
    format!("daemon running{activity}{streams}")
}

fn stream_watch_status(daemon: &DaemonView) -> String {
    if daemon.stream_watches.is_empty() {
        return "streams: none".to_owned();
    }
    let running = daemon
        .stream_watches
        .iter()
        .filter(|watch| watch.status == "running")
        .count();
    let completed = daemon
        .stream_watches
        .iter()
        .filter(|watch| watch.status == "completed")
        .count();
    let failed = daemon
        .stream_watches
        .iter()
        .filter(|watch| watch.status == "failed")
        .count();
    let latest = daemon
        .stream_watches
        .last()
        .map(|watch| {
            watch
                .stream_id
                .as_deref()
                .map(|stream_id| shorten(stream_id, 18))
                .unwrap_or_else(|| shorten(&watch.watch_id, 18))
        })
        .unwrap_or_else(|| "none".to_owned());
    format!("streams: running={running} completed={completed} failed={failed} latest={latest}")
}

fn stream_preview_lines(
    daemon: &DaemonView,
    live_previews: &[LiveStreamPreview],
    group_id: Option<&str>,
) -> Vec<Line<'static>> {
    let Some(group_id) = group_id else {
        return Vec::new();
    };
    let mut lines = live_previews
        .iter()
        .filter(|preview| preview.group_id == group_id)
        .filter_map(|preview| {
            stream_preview_line_pair(
                &preview.author,
                &preview.status,
                &preview.text,
                preview.error.as_deref(),
            )
        })
        .flatten()
        .collect::<Vec<_>>();
    lines.extend(
        daemon
            .stream_watches
            .iter()
            .filter(|watch| watch.group_id == group_id)
            .filter(|watch| {
                let Some(stream_id) = watch.stream_id.as_deref() else {
                    return true;
                };
                !live_previews
                    .iter()
                    .any(|preview| preview.group_id == group_id && preview.stream_id == stream_id)
            })
            .filter_map(|watch| {
                stream_preview_line_pair(
                    "stream",
                    &watch.status,
                    watch.text.as_deref().unwrap_or_default(),
                    watch.error.as_deref(),
                )
            })
            .flatten(),
    );
    lines
}

fn stream_preview_line_pair(
    author: &str,
    status: &str,
    text: &str,
    error: Option<&str>,
) -> Option<[Line<'static>; 2]> {
    let body = match status {
        "completed" => return None,
        "failed" => format!(
            "stream failed: {}",
            terminal_safe_text(error.unwrap_or("stream watch failed"))
        ),
        _ => {
            if text.is_empty() {
                return None;
            } else {
                terminal_safe_text(text)
            }
        }
    };
    Some([
        Line::from(""),
        Line::from(vec![
            Span::styled(
                terminal_safe_text(author),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw(": "),
            Span::raw(body),
        ]),
    ])
}

fn message_lines(
    messages: &[MessageRow],
    selected_account: Option<&AccountRow>,
) -> Vec<Line<'static>> {
    messages
        .iter()
        .flat_map(|message| {
            let author = terminal_safe_text(&message_author_label(message, selected_account));
            [
                Line::from(vec![
                    Span::styled(author, Style::default().fg(Color::Yellow)),
                    Span::raw(": "),
                    Span::raw(terminal_safe_text(&message.display_text)),
                ]),
                Line::from(""),
            ]
        })
        .collect()
}

fn unique_member_refs(members: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for member in members {
        if !member.is_empty() && !unique.iter().any(|existing| existing == &member) {
            unique.push(member);
        }
    }
    unique
}

fn member_ref_summary(members: &[String]) -> String {
    members
        .iter()
        .map(|member| shorten(&terminal_safe_text(member), 14))
        .collect::<Vec<_>>()
        .join(", ")
}

fn group_members_status(result: &Value) -> String {
    let members = result
        .get("members")
        .and_then(Value::as_array)
        .map(|members| {
            members
                .iter()
                .filter_map(|member| {
                    value_string(member, "npub").or_else(|| value_string(member, "member_id"))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if members.is_empty() {
        return "members: none".to_owned();
    }
    format!("members: {}", member_ref_summary(&members))
}

fn chat_row_line(chat: &ChatRow, selected: bool, unread_count: usize) -> Line<'static> {
    let marker = if selected { ">" } else { " " };
    let archived = if chat.archived { " archived" } else { "" };
    let mut ambient_style = Style::default();
    let mut label_style = row_label_style(selected, Color::Green);
    if unread_count > 0 {
        ambient_style = ambient_style.add_modifier(Modifier::BOLD);
        label_style = label_style.add_modifier(Modifier::BOLD);
    }
    Line::from(vec![
        Span::styled(format!("{marker} "), ambient_style),
        Span::styled(chat_label(&chat.name, unread_count, 24), label_style),
        Span::styled(archived.to_owned(), ambient_style),
    ])
}

fn chat_label(name: &str, unread_count: usize, max_len: usize) -> String {
    let name = terminal_safe_text(name);
    if unread_count == 0 {
        return shorten(&name, max_len);
    }
    shorten(&format!("{name} ({unread_count})"), max_len)
}

fn retain_unread_counts_for_chats(unread_counts: &mut HashMap<String, usize>, chats: &[ChatRow]) {
    unread_counts.retain(|group_id, _| chats.iter().any(|chat| chat.group_id == *group_id));
}

impl GroupDiagnostics {
    fn unavailable(group_id: &str, error: impl Into<String>) -> Self {
        Self {
            group_id: group_id.to_owned(),
            epoch: None,
            member_count: None,
            components: Vec::new(),
            error: Some(error.into()),
        }
    }
}

fn parse_group_diagnostics(value: &Value) -> Option<GroupDiagnostics> {
    let group = value.get("group")?;
    let group_id = value_string(group, "group_id")?;
    let mls = value.get("mls");
    Some(GroupDiagnostics {
        group_id,
        epoch: mls
            .and_then(|mls| mls.get("epoch"))
            .and_then(Value::as_u64)
            .or_else(|| group.get("epoch").and_then(Value::as_u64)),
        member_count: mls
            .and_then(|mls| mls.get("member_count"))
            .and_then(Value::as_u64)
            .or_else(|| group.get("member_count").and_then(Value::as_u64)),
        components: group_component_diagnostics(group),
        error: None,
    })
}

fn group_component_diagnostics(group: &Value) -> Vec<GroupComponentDiagnostics> {
    [
        "profile",
        "image",
        "admin_policy",
        "nostr_routing",
        "agent_text_stream",
    ]
    .into_iter()
    .filter_map(|key| {
        let component = group.get(key)?;
        Some(GroupComponentDiagnostics {
            component: value_string(component, "component").unwrap_or_else(|| key.to_owned()),
            component_id: component.get("component_id").and_then(Value::as_u64),
            data_hex: value_string(component, "data_hex").unwrap_or_default(),
        })
    })
    .collect()
}

fn status_panel_lines(status: &str, diagnostics: Option<&GroupDiagnostics>) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(terminal_safe_text(status)),
        Line::from(""),
        Line::from(""),
    ];
    let Some(diagnostics) = diagnostics else {
        lines.push(Line::from("MLS no group selected"));
        return lines;
    };
    if let Some(error) = &diagnostics.error {
        lines.push(Line::from(format!(
            "MLS group={} unavailable: {}",
            shorten(&terminal_safe_text(&diagnostics.group_id), 18),
            terminal_safe_text(error)
        )));
        return lines;
    }
    let epoch = diagnostics
        .epoch
        .map(|epoch| epoch.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let member_count = diagnostics
        .member_count
        .map(|member_count| member_count.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    lines.push(Line::from(format!(
        "MLS epoch={epoch} group={} members={member_count}",
        shorten(&terminal_safe_text(&diagnostics.group_id), 18)
    )));
    if diagnostics.components.is_empty() {
        lines.push(Line::from("components: none"));
        return lines;
    }
    lines.push(Line::from("components:"));
    lines.extend(
        diagnostics
            .components
            .iter()
            .map(group_component_diagnostics_line),
    );
    lines
}

fn group_component_diagnostics_line(component: &GroupComponentDiagnostics) -> Line<'static> {
    let id = component
        .component_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    Line::from(format!(
        "{} id={id} data={}",
        terminal_safe_text(&component.component),
        terminal_safe_text(&component.data_hex)
    ))
}

fn selected_style(selected: bool) -> Style {
    if selected {
        Style::default()
            .fg(Color::Black)
            .bg(Color::White)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    }
}

fn row_label_style(selected: bool, color: Color) -> Style {
    if selected {
        Style::default()
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(color)
    }
}

fn panel_block(title: &str, focused: bool) -> Block<'_> {
    let style = if focused {
        Style::default().fg(FOCUS_ACCENT)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    Block::default()
        .borders(Borders::ALL)
        .border_style(style)
        .title(title)
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn terminal_safe_text(value: &str) -> String {
    value.chars().filter(|ch| !ch.is_control()).collect()
}

fn shorten(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_owned();
    }
    if max_len <= 3 {
        return value.chars().take(max_len).collect();
    }
    let prefix_len = (max_len - 3) / 2;
    let suffix_len = max_len - 3 - prefix_len;
    let prefix = value.chars().take(prefix_len).collect::<String>();
    let suffix = value
        .chars()
        .rev()
        .take(suffix_len)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{prefix}...{suffix}")
}

fn composer_display_text(input: &str) -> String {
    let trimmed = input.trim();
    if let Some(command_input) = trimmed.strip_prefix('/')
        && let Ok(words) = split_slash_command_words(command_input)
        && words.first().map(String::as_str) == Some("login")
        && words.iter().skip(1).any(|word| word.starts_with("nsec"))
    {
        return "/login <hidden nsec>".to_owned();
    }
    input.to_owned()
}

fn message_subscription_args() -> Vec<String> {
    vec![
        "messages".to_owned(),
        "subscribe".to_owned(),
        "--limit".to_owned(),
        "0".to_owned(),
    ]
}

#[cfg(test)]
mod tests;
