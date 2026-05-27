//! QUIC agent text stream commands: anchor, send, watch, receive, finish, verify.
//! Compose flows (open/append/finish/cancel) are TUI helpers, not user-facing.

use std::net::SocketAddr;

use cgka_traits::app_event::{STREAM_CHUNKS_TAG, STREAM_HASH_TAG, STREAM_TAG};
use cgka_traits::{GroupId, MessageId};
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AgentTextStreamFinishRequest, MarmotApp, MarmotAppRuntime, tag_value};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use transport_quic_broker::{
    PublishTextToBroker, SubscribeTextFromBroker, publish_text_to_broker,
    subscribe_text_from_broker_with_updates,
};
use transport_quic_stream::{QuicTextStreamReceiver, SendTextStream, send_text_stream};

use std::time::Duration;

use marmot_app::{AgentStreamDelta, AppMessageQuery};

use crate::{
    AGENT_STREAM_START_LOOKBACK_LIMIT, CommandOutput, DmError, agent_text_stream_payload_value,
    broker_trust, broker_trust_name, ensure_local_signing, latest_stream_start,
    normalize_group_id_hex, normalize_hex, npub_for_account_id, parse_quic_candidate,
    resolve_account, resolve_quic_candidate_addr, stream_crypto_for_start_event,
    stream_route_label, stream_start_event_id, stream_trust, stream_trust_name,
    transcript_hash_from_hex, unix_now_seconds, unsupported_command,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum StreamCommand {
    #[command(about = "Anchor a durable agent text stream start over the MLS message path")]
    Start {
        #[arg(help = "Group id to anchor the stream in")]
        group: String,
        #[arg(long, value_name = "HEX", help = "Optional stream id to use")]
        stream_id: Option<String>,
        #[arg(
            long = "quic-candidate",
            value_name = "ADDR",
            help = "QUIC candidate URI such as quic://127.0.0.1:4450"
        )]
        quic_candidates: Vec<String>,
    },
    #[command(about = "Receive one provisional QUIC agent text stream")]
    Receive {
        #[arg(
            long,
            default_value = "127.0.0.1:4450",
            value_name = "ADDR",
            help = "Local address to bind"
        )]
        bind: SocketAddr,
        #[arg(long, value_name = "HEX", help = "Expected stream-start event id")]
        start_event_id: Option<String>,
    },
    #[command(about = "Send one provisional QUIC agent text stream")]
    Send {
        #[arg(
            long,
            help = "Use the broker protocol instead of direct QUIC stream receive"
        )]
        broker: bool,
        #[arg(long, value_name = "ADDR", help = "Remote QUIC address")]
        connect: SocketAddr,
        #[arg(
            long,
            default_value = "localhost",
            value_name = "NAME",
            help = "TLS server name"
        )]
        server_name: String,
        #[arg(
            long,
            value_name = "HEX",
            help = "Pinned server certificate DER bytes as hex"
        )]
        server_cert_der_hex: Option<String>,
        #[arg(long, help = "Trust loopback QUIC certificates for local testing")]
        insecure_local: bool,
        #[arg(long, value_name = "HEX", help = "Optional stream id to use")]
        stream_id: Option<String>,
        #[arg(long, value_name = "HEX", help = "Expected stream-start event id")]
        start_event_id: Option<String>,
        #[arg(
            long,
            default_value_t = 1024,
            value_name = "BYTES",
            help = "Maximum bytes per streamed chunk"
        )]
        chunk_bytes: usize,
        #[arg(
            long,
            default_value_t = 0,
            value_name = "MILLIS",
            help = "Delay between streamed chunks"
        )]
        chunk_delay_ms: u64,
        #[arg(
            value_name = "TEXT",
            required = true,
            allow_hyphen_values = true,
            help = "Text to stream"
        )]
        text: Vec<String>,
    },
    #[command(about = "Watch one brokered QUIC agent text stream from a durable MLS start payload")]
    Watch {
        #[arg(help = "Group id containing the stream start")]
        group: String,
        #[arg(long, value_name = "HEX", help = "Stream id to watch")]
        stream_id: Option<String>,
        #[arg(
            long,
            value_name = "HEX",
            help = "Pinned server certificate DER bytes as hex"
        )]
        server_cert_der_hex: Option<String>,
        #[arg(long, help = "Trust loopback QUIC certificates for local testing")]
        insecure_local: bool,
        #[arg(
            long,
            help = "Register the watch with the daemon and return immediately"
        )]
        background: bool,
    },
    #[command(hide = true)]
    ComposeOpen {
        group: String,
        #[arg(long, value_name = "HEX")]
        stream_id: Option<String>,
        #[arg(long = "quic-candidate", value_name = "ADDR")]
        quic_candidates: Vec<String>,
        #[arg(long)]
        insecure_local: bool,
        #[arg(long, default_value_t = 32, value_name = "BYTES")]
        chunk_bytes: usize,
    },
    #[command(hide = true)]
    ComposeAppend {
        #[arg(long, value_name = "HEX")]
        stream_id: String,
        #[arg(value_name = "TEXT", required = true, allow_hyphen_values = true)]
        text: Vec<String>,
    },
    #[command(hide = true)]
    ComposeFinish {
        #[arg(long, value_name = "HEX")]
        stream_id: String,
    },
    #[command(hide = true)]
    ComposeCancel {
        #[arg(long, value_name = "HEX")]
        stream_id: String,
    },
    #[command(about = "Commit the final agent text stream transcript over the MLS message path")]
    Finish {
        #[arg(help = "Group id containing the stream")]
        group: String,
        #[arg(long, value_name = "HEX", help = "Stream id to finish")]
        stream_id: String,
        #[arg(long, value_name = "HEX", help = "Stream-start message id")]
        start_event_id: String,
        #[arg(long, value_name = "HEX", help = "Final transcript hash")]
        transcript_hash: String,
        #[arg(long, help = "Number of streamed chunks")]
        chunk_count: u64,
        #[arg(
            value_name = "TEXT",
            required = true,
            allow_hyphen_values = true,
            help = "Final text"
        )]
        text: Vec<String>,
    },
    #[command(about = "Verify a local QUIC transcript against the durable MLS final payload")]
    Verify {
        #[arg(help = "Group id containing the stream")]
        group: String,
        #[arg(long, value_name = "HEX", help = "Stream id to verify")]
        stream_id: String,
        #[arg(long, value_name = "HEX", help = "Expected transcript hash")]
        transcript_hash: String,
        #[arg(long, help = "Expected streamed chunk count")]
        chunk_count: Option<u64>,
    },
}

pub(crate) async fn run_local(command: StreamCommand) -> Result<CommandOutput, DmError> {
    match command {
        StreamCommand::Receive {
            bind,
            start_event_id,
        } => {
            let (start_event_id, anchored) = stream_start_event_id(start_event_id)?;
            let receiver = QuicTextStreamReceiver::bind(bind)?;
            let local_addr = receiver.local_addr()?;
            let server_cert_der_hex = hex::encode(receiver.server_cert_der());
            let received = receiver.receive_once(start_event_id, None).await?;
            let stream_id = hex::encode(&received.stream_id);
            Ok(CommandOutput {
                plain: format!(
                    "received stream {stream_id} chunks={}\n{}",
                    received.chunk_count, received.text
                ),
                json: json!({
                    "local_addr": local_addr.to_string(),
                    "server_cert_der_hex": server_cert_der_hex,
                    "stream_id": stream_id,
                    "anchored": anchored,
                    "chunks": received.chunks.into_iter().map(|chunk| {
                        json!({
                            "seq": chunk.seq,
                            "record_type": chunk.record_type,
                            "flags": chunk.flags,
                            "text": chunk.text,
                        })
                    }).collect::<Vec<_>>(),
                    "text": received.text,
                    "transcript_hash": hex::encode(received.transcript_hash),
                    "chunk_count": received.chunk_count,
                }),
            })
        }
        StreamCommand::Send {
            broker,
            connect,
            server_name,
            server_cert_der_hex,
            insecure_local,
            stream_id,
            start_event_id,
            chunk_bytes,
            chunk_delay_ms,
            text,
        } => {
            if text.is_empty() {
                return Err(DmError::EmptyStreamText);
            }
            let text = text.join(" ");
            let stream_id = stream_id
                .map(hex::decode)
                .transpose()?
                .unwrap_or_else(transport_quic_stream::random_stream_id);
            let (start_event_id, anchored) = stream_start_event_id(start_event_id)?;
            if broker {
                let trust = broker_trust(connect, server_cert_der_hex, insecure_local)?;
                if !anchored {
                    return Err(DmError::MissingStreamStart);
                }
                let sent = publish_text_to_broker(PublishTextToBroker {
                    broker_addr: connect,
                    server_name: server_name.clone(),
                    trust: trust.clone(),
                    stream_id: stream_id.clone(),
                    start_event_id,
                    text: text.clone(),
                    max_chunk_bytes: chunk_bytes,
                    chunk_delay: Duration::from_millis(chunk_delay_ms),
                    crypto: None,
                })
                .await?;
                return Ok(CommandOutput {
                    plain: format!(
                        "sent brokered stream {} chunks={}",
                        hex::encode(&stream_id),
                        sent.chunk_count
                    ),
                    json: json!({
                        "brokered": true,
                        "connect": connect.to_string(),
                        "server_name": server_name,
                        "trust": broker_trust_name(&trust),
                        "stream_id": hex::encode(sent.stream_id),
                        "anchored": anchored,
                        "text_bytes": text.len(),
                        "transcript_hash": hex::encode(sent.transcript_hash),
                        "chunk_count": sent.chunk_count,
                    }),
                });
            }
            let trust = stream_trust(connect, server_cert_der_hex, insecure_local)?;
            let sent = send_text_stream(SendTextStream {
                server_addr: connect,
                server_name: server_name.clone(),
                trust: trust.clone(),
                stream_id: stream_id.clone(),
                start_event_id,
                text: text.clone(),
                max_chunk_bytes: chunk_bytes,
                chunk_delay: Duration::from_millis(chunk_delay_ms),
                crypto: None,
            })
            .await?;
            Ok(CommandOutput {
                plain: format!(
                    "sent stream {} chunks={}",
                    hex::encode(&stream_id),
                    sent.chunk_count
                ),
                json: json!({
                    "brokered": false,
                    "connect": connect.to_string(),
                    "server_name": server_name,
                    "trust": stream_trust_name(&trust),
                    "stream_id": hex::encode(sent.stream_id),
                    "anchored": anchored,
                    "text_bytes": text.len(),
                    "transcript_hash": hex::encode(sent.transcript_hash),
                    "chunk_count": sent.chunk_count,
                }),
            })
        }
        StreamCommand::Start { .. }
        | StreamCommand::Watch { .. }
        | StreamCommand::ComposeOpen { .. }
        | StreamCommand::ComposeAppend { .. }
        | StreamCommand::ComposeFinish { .. }
        | StreamCommand::ComposeCancel { .. }
        | StreamCommand::Finish { .. }
        | StreamCommand::Verify { .. } => {
            unreachable!("durable stream commands require app setup")
        }
    }
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: StreamCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: StreamCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        StreamCommand::Start {
            group,
            stream_id,
            quic_candidates,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(group)?);
            let stream_id = stream_id
                .map(hex::decode)
                .transpose()?
                .unwrap_or_else(transport_quic_stream::random_stream_id);
            let (payload, summary) = runtime
                .start_agent_text_stream(
                    &account.label,
                    &group_id,
                    &stream_id,
                    unix_now_seconds(),
                    quic_candidates,
                )
                .await?;
            let agent_text_stream =
                agent_text_stream_payload_value(payload.kind, &payload.tags, &payload.content);
            Ok(CommandOutput {
                plain: format!(
                    "started stream {} published={}",
                    hex::encode(&stream_id),
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "stream_id": hex::encode(stream_id),
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                    "agent_text_stream": agent_text_stream,
                }),
            })
        }
        StreamCommand::Watch {
            group,
            stream_id,
            server_cert_der_hex,
            insecure_local,
            background,
        } => {
            watch_with_runtime(
                account_home,
                app,
                runtime,
                StreamCommand::Watch {
                    group,
                    stream_id,
                    server_cert_der_hex,
                    insecure_local,
                    background,
                },
                account_flag,
                |_| {},
            )
            .await
        }
        StreamCommand::Send {
            broker,
            connect,
            server_name,
            server_cert_der_hex,
            insecure_local,
            stream_id,
            start_event_id,
            chunk_bytes,
            chunk_delay_ms,
            text,
        } => {
            if text.is_empty() {
                return Err(DmError::EmptyStreamText);
            }
            let text = text.join(" ");
            let start_event_id_hex = start_event_id.ok_or(DmError::MissingStreamStart)?;
            let expected_stream_id_hex =
                stream_id.map(|value| normalize_hex(&value)).transpose()?;
            let (stream_id, crypto) = stream_crypto_for_start_event(
                account_home,
                app,
                runtime,
                account_flag.as_deref(),
                None,
                expected_stream_id_hex.as_deref(),
                &start_event_id_hex,
            )
            .await?;
            let start_event_id = MessageId::new(hex::decode(normalize_hex(&start_event_id_hex)?)?);
            if broker {
                let trust = broker_trust(connect, server_cert_der_hex, insecure_local)?;
                let sent = publish_text_to_broker(PublishTextToBroker {
                    broker_addr: connect,
                    server_name: server_name.clone(),
                    trust: trust.clone(),
                    stream_id: stream_id.clone(),
                    start_event_id,
                    text: text.clone(),
                    max_chunk_bytes: chunk_bytes,
                    chunk_delay: Duration::from_millis(chunk_delay_ms),
                    crypto: Some(crypto),
                })
                .await?;
                return Ok(CommandOutput {
                    plain: format!(
                        "sent brokered stream {} chunks={}",
                        hex::encode(&stream_id),
                        sent.chunk_count
                    ),
                    json: json!({
                        "brokered": true,
                        "connect": connect.to_string(),
                        "server_name": server_name,
                        "trust": broker_trust_name(&trust),
                        "stream_id": hex::encode(sent.stream_id),
                        "anchored": true,
                        "text_bytes": text.len(),
                        "transcript_hash": hex::encode(sent.transcript_hash),
                        "chunk_count": sent.chunk_count,
                    }),
                });
            }
            let trust = stream_trust(connect, server_cert_der_hex, insecure_local)?;
            let sent = send_text_stream(SendTextStream {
                server_addr: connect,
                server_name: server_name.clone(),
                trust: trust.clone(),
                stream_id: stream_id.clone(),
                start_event_id,
                text: text.clone(),
                max_chunk_bytes: chunk_bytes,
                chunk_delay: Duration::from_millis(chunk_delay_ms),
                crypto: Some(crypto),
            })
            .await?;
            Ok(CommandOutput {
                plain: format!(
                    "sent stream {} chunks={}",
                    hex::encode(&stream_id),
                    sent.chunk_count
                ),
                json: json!({
                    "brokered": false,
                    "connect": connect.to_string(),
                    "server_name": server_name,
                    "trust": stream_trust_name(&trust),
                    "stream_id": hex::encode(sent.stream_id),
                    "anchored": true,
                    "text_bytes": text.len(),
                    "transcript_hash": hex::encode(sent.transcript_hash),
                    "chunk_count": sent.chunk_count,
                }),
            })
        }
        StreamCommand::ComposeOpen { .. }
        | StreamCommand::ComposeAppend { .. }
        | StreamCommand::ComposeFinish { .. }
        | StreamCommand::ComposeCancel { .. } => unsupported_command(
            "stream compose",
            "stream compose sessions require the daemon",
        ),
        StreamCommand::Finish {
            group,
            stream_id,
            start_event_id,
            transcript_hash,
            chunk_count,
            text,
        } => {
            if text.is_empty() {
                return Err(DmError::EmptyStreamText);
            }
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(group)?);
            let stream_id = hex::decode(stream_id)?;
            let transcript_hash = transcript_hash_from_hex(&transcript_hash)?;
            let (payload, summary) = runtime
                .finish_agent_text_stream(
                    &account.label,
                    &group_id,
                    AgentTextStreamFinishRequest {
                        stream_id: stream_id.clone(),
                        start_event_id,
                        final_text_or_reference: text.join(" "),
                        transcript_hash,
                        chunk_count,
                        finished_at: unix_now_seconds(),
                    },
                )
                .await?;
            let agent_text_stream =
                agent_text_stream_payload_value(payload.kind, &payload.tags, &payload.content);
            Ok(CommandOutput {
                plain: format!(
                    "finished stream {} published={}",
                    hex::encode(&stream_id),
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "stream_id": hex::encode(stream_id),
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                    "agent_text_stream": agent_text_stream,
                }),
            })
        }
        StreamCommand::Verify {
            group,
            stream_id,
            transcript_hash,
            chunk_count,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id_hex = normalize_group_id_hex(&group)?;
            let stream_id_hex = normalize_hex(&stream_id)?;
            let transcript_hash_hex = hex::encode(transcript_hash_from_hex(&transcript_hash)?);
            let messages = app.messages_with_query(
                &account.label,
                AppMessageQuery {
                    group_id_hex: Some(group_id_hex.clone()),
                    limit: None,
                },
            )?;
            let final_message = messages.into_iter().rev().find(|message| {
                marmot_app::is_stream_final_event(message.kind, &message.tags)
                    && tag_value(&message.tags, STREAM_TAG) == Some(stream_id_hex.as_str())
            });
            let (verified, final_message_json) = match final_message {
                Some(message) => {
                    let final_transcript_hash =
                        tag_value(&message.tags, STREAM_HASH_TAG).unwrap_or_default();
                    let final_chunk_count = tag_value(&message.tags, STREAM_CHUNKS_TAG)
                        .and_then(|count| count.parse::<u64>().ok())
                        .unwrap_or_default();
                    let transcript_hash_matches = final_transcript_hash == transcript_hash_hex;
                    let chunk_count_matches =
                        chunk_count.is_none_or(|count| count == final_chunk_count);
                    (
                        transcript_hash_matches && chunk_count_matches,
                        json!({
                            "message_id": message.message_id_hex,
                            "stream_id": stream_id_hex,
                            "transcript_hash": final_transcript_hash,
                            "chunk_count": final_chunk_count,
                            "final_text_or_reference": message.plaintext,
                            "checks": {
                                "transcript_hash": transcript_hash_matches,
                                "chunk_count": chunk_count_matches,
                            },
                        }),
                    )
                }
                None => (false, Value::Null),
            };
            Ok(CommandOutput {
                plain: format!("stream {stream_id_hex} verified={verified}"),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id_hex,
                    "stream_id": stream_id_hex,
                    "verified": verified,
                    "expected": {
                        "transcript_hash": transcript_hash_hex,
                        "chunk_count": chunk_count,
                    },
                    "final_message": final_message_json,
                }),
            })
        }
        StreamCommand::Receive { .. } => {
            unreachable!("local QUIC stream commands return before app setup")
        }
    }
}

pub(crate) async fn watch_with_runtime<F>(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: StreamCommand,
    account_flag: Option<String>,
    mut on_delta: F,
) -> Result<CommandOutput, DmError>
where
    F: FnMut(AgentStreamDelta) + Send,
{
    let StreamCommand::Watch {
        group,
        stream_id,
        server_cert_der_hex,
        insecure_local,
        background: _,
    } = command
    else {
        unreachable!("stream watch helper only accepts stream watch commands");
    };
    let account = resolve_account(account_home, account_flag.clone())?;
    ensure_local_signing(&account)?;
    app.status(&account.label)?;
    let group_id_hex = normalize_group_id_hex(&group)?;
    let expected_stream_id_hex = stream_id.map(|value| normalize_hex(&value)).transpose()?;
    let messages = app.messages_with_query(
        &account.label,
        AppMessageQuery {
            group_id_hex: Some(group_id_hex.clone()),
            limit: Some(AGENT_STREAM_START_LOOKBACK_LIMIT),
        },
    )?;
    let (start_message_id_hex, start_payload, _start_sender_hex) =
        latest_stream_start(messages, expected_stream_id_hex.as_deref())?;
    if start_message_id_hex.is_empty() {
        return Err(DmError::StreamStartNotConfirmed);
    }
    if start_payload.route != "quic" {
        return Err(DmError::UnsupportedStreamRoute(
            stream_route_label(&start_payload.route).to_owned(),
        ));
    }
    let candidate = start_payload
        .quic_candidates
        .iter()
        .find(|candidate| candidate.trim().starts_with("quic://"))
        .ok_or(DmError::MissingQuicCandidate)?;
    let candidate = parse_quic_candidate(candidate)?;
    let candidate_addr = resolve_quic_candidate_addr(&candidate).await?;
    let trust = broker_trust(candidate_addr, server_cert_der_hex, insecure_local)?;
    let stream_id_hex = start_payload.stream_id_hex.clone();
    let start_event_id = MessageId::new(hex::decode(&start_message_id_hex)?);
    let (stream_id, crypto) = stream_crypto_for_start_event(
        account_home,
        app,
        runtime,
        account_flag.as_deref(),
        Some(&group_id_hex),
        Some(&stream_id_hex),
        &start_message_id_hex,
    )
    .await?;
    let crypto = Some(crypto);
    let delta_account = account_flag.or(Some(account.account_id_hex.clone()));
    let delta_group_id = group_id_hex.clone();
    let delta_stream_id = stream_id_hex.clone();
    let received = subscribe_text_from_broker_with_updates(
        SubscribeTextFromBroker {
            broker_addr: candidate_addr,
            server_name: candidate.server_name.clone(),
            trust: trust.clone(),
            stream_id,
            start_event_id,
            crypto,
        },
        |chunk| {
            on_delta(AgentStreamDelta {
                account: delta_account.clone(),
                group_id: delta_group_id.clone(),
                stream_id: delta_stream_id.clone(),
                seq: chunk.seq,
                record_type: chunk.record_type,
                flags: chunk.flags,
                text: chunk.text.clone(),
            });
        },
    )
    .await?;
    Ok(CommandOutput {
        plain: format!(
            "received brokered stream {} chunks={}\n{}",
            hex::encode(&received.stream_id),
            received.chunk_count,
            received.text
        ),
        json: json!({
            "brokered": true,
            "candidate": candidate.original,
            "connect": candidate_addr.to_string(),
            "server_name": candidate.server_name,
            "trust": broker_trust_name(&trust),
            "stream_id": hex::encode(&received.stream_id),
            "start_message_id": start_message_id_hex,
            "chunks": received.chunks.into_iter().map(|chunk| {
                json!({
                    "seq": chunk.seq,
                    "record_type": chunk.record_type,
                    "flags": chunk.flags,
                    "text": chunk.text,
                })
            }).collect::<Vec<_>>(),
            "text": received.text,
            "transcript_hash": hex::encode(received.transcript_hash),
            "chunk_count": received.chunk_count,
        }),
    })
}
