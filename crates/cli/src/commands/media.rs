//! Encrypted media upload, download, and listing through Blossom servers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cgka_traits::GroupId;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{
    AppMessageQuery, AppMessageRecord, DEFAULT_BLOSSOM_SERVER_URL, MarmotApp, MarmotAppRuntime,
    MediaReference, MediaUploadRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CommandOutput, DmError, ensure_local_signing, normalize_group_id_hex, npub_for_account_id,
    resolve_account, write_private_file,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum MediaCommand {
    #[command(about = "Encrypt and upload a media file to Blossom")]
    Upload {
        #[arg(help = "Group id that owns the media key")]
        group: String,
        #[arg(help = "Path to the plaintext media file")]
        file_path: String,
        #[arg(long, help = "Send a kind-9 media message after upload")]
        send: bool,
        #[arg(long, help = "Caption to send with --send")]
        message: Option<String>,
        #[arg(long, value_name = "MIME", help = "Override MIME type")]
        media_type: Option<String>,
        #[arg(
            long,
            value_name = "URL",
            help = "Blossom server URL for upload",
            default_value = DEFAULT_BLOSSOM_SERVER_URL
        )]
        server: String,
    },
    #[command(about = "Download and decrypt a media file from Blossom")]
    Download {
        #[arg(help = "Group id that owns the media key")]
        group: String,
        #[arg(help = "Plaintext SHA-256 hash from media list")]
        file_hash: String,
        #[arg(
            long,
            value_name = "PATH",
            help = "Output path; defaults to the original filename"
        )]
        output: Option<String>,
    },
    #[command(about = "List media references for a group")]
    List {
        #[arg(help = "Group id to inspect")]
        group: String,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: MediaCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: MediaCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        MediaCommand::Upload {
            group,
            file_path,
            send,
            message,
            media_type,
            server,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id_hex = normalize_group_id_hex(&group)?;
            let group_id = GroupId::new(hex::decode(&group_id_hex)?);
            let path = PathBuf::from(&file_path);
            let plaintext = std::fs::read(&path)?;
            let file_name = media_file_name(&path)?;
            let media_type = media_type.unwrap_or_else(|| guess_media_type(&path).to_owned());
            let upload = runtime
                .upload_media(
                    &account.account_id_hex,
                    &group_id,
                    MediaUploadRequest {
                        file_name,
                        media_type,
                        plaintext,
                        caption: message,
                        send,
                        blossom_server: Some(server),
                    },
                )
                .await?;
            Ok(CommandOutput {
                plain: if upload.sent.is_some() {
                    format!("uploaded and sent {}", upload.reference.file_name)
                } else {
                    format!("uploaded {}", upload.reference.file_name)
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id_hex,
                    "media": media_reference_json(&upload.reference),
                    "encrypted_hash_hex": upload.encrypted_hash_hex,
                    "encrypted_size_bytes": upload.encrypted_size_bytes,
                    "sent": upload.sent.map(send_summary_json),
                }),
            })
        }
        MediaCommand::Download {
            group,
            file_hash,
            output,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id_hex = normalize_group_id_hex(&group)?;
            let group_id = GroupId::new(hex::decode(&group_id_hex)?);
            let file_hash_hex = normalize_sha256_hex(&file_hash)?;
            let messages = runtime.messages_with_query(
                &account.account_id_hex,
                AppMessageQuery {
                    group_id_hex: Some(group_id_hex.clone()),
                    limit: None,
                },
            )?;
            let reference = media_reference_for_hash(messages, &file_hash_hex)?;
            let output_path = media_output_path(output, &reference.file_name);
            let download = runtime
                .download_media(&account.account_id_hex, &group_id, reference.clone())
                .await?;
            write_private_file(&output_path, &download.plaintext)?;
            Ok(CommandOutput {
                plain: output_path.display().to_string(),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id_hex,
                    "media": media_reference_json(&reference),
                    "output_path": output_path.display().to_string(),
                    "size_bytes": download.size_bytes,
                }),
            })
        }
        MediaCommand::List { group } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id_hex = normalize_group_id_hex(&group)?;
            let messages = runtime.messages_with_query(
                &account.account_id_hex,
                AppMessageQuery {
                    group_id_hex: Some(group_id_hex.clone()),
                    limit: None,
                },
            )?;
            let media = media_records_json(messages);
            Ok(CommandOutput {
                plain: if media.is_empty() {
                    "no media".to_owned()
                } else {
                    media
                        .iter()
                        .filter_map(|item| item.get("file_name").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id_hex,
                    "media": media,
                }),
            })
        }
    }
}

fn media_records_json(messages: Vec<AppMessageRecord>) -> Vec<Value> {
    messages
        .into_iter()
        .filter_map(|message| {
            let imeta = imeta_fields(&message.tags)?;
            let caption = (!message.plaintext.is_empty()).then(|| message.plaintext.clone());
            Some(json!({
                "message_id": message.message_id_hex,
                "direction": message.direction,
                "group_id": message.group_id_hex,
                "from": message.sender,
                "url": imeta.get("url").cloned().unwrap_or_default(),
                "file_hash_hex": imeta.get("x").cloned().unwrap_or_default(),
                "file_name": imeta.get("filename").cloned().unwrap_or_default(),
                "nonce_hex": imeta.get("n").cloned().unwrap_or_default(),
                "version": imeta.get("v").cloned().unwrap_or_default(),
                "media_type": imeta.get("m").cloned().unwrap_or_default(),
                "caption": caption,
                "recorded_at": message.recorded_at,
                "received_at": message.received_at,
            }))
        })
        .collect()
}

fn media_reference_json(reference: &MediaReference) -> Value {
    json!({
        "url": reference.url,
        "file_hash_hex": reference.file_hash_hex,
        "file_name": reference.file_name,
        "nonce_hex": reference.nonce_hex,
        "version": reference.version,
        "media_type": reference.media_type,
    })
}

fn send_summary_json(summary: marmot_app::SendSummary) -> Value {
    json!({
        "published": summary.published,
        "message_ids": summary.message_ids,
    })
}

fn media_reference_for_hash(
    messages: Vec<AppMessageRecord>,
    file_hash_hex: &str,
) -> Result<MediaReference, DmError> {
    messages
        .into_iter()
        .filter_map(|message| imeta_fields(&message.tags))
        .find(|imeta| imeta.get("x").map(String::as_str) == Some(file_hash_hex))
        .map(media_reference_from_imeta)
        .transpose()?
        .ok_or_else(|| DmError::MediaReferenceNotFound(file_hash_hex.to_owned()))
}

fn media_reference_from_imeta(imeta: HashMap<String, String>) -> Result<MediaReference, DmError> {
    let required = |key: &'static str| {
        imeta
            .get(key)
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .ok_or(DmError::InvalidMediaReference(key.to_owned()))
    };
    Ok(MediaReference {
        url: required("url")?,
        file_hash_hex: required("x")?,
        nonce_hex: required("n")?,
        file_name: required("filename")?,
        media_type: required("m")?,
        version: required("v")?,
    })
}

fn normalize_sha256_hex(value: &str) -> Result<String, DmError> {
    let decoded = hex::decode(value)?;
    if decoded.len() != 32 {
        return Err(DmError::InvalidMediaReference(
            "file hash must be 32 bytes".to_owned(),
        ));
    }
    Ok(hex::encode(decoded))
}

fn media_file_name(path: &Path) -> Result<String, DmError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| DmError::InvalidMediaReference("file name".to_owned()))
}

fn media_output_path(output: Option<String>, file_name: &str) -> PathBuf {
    output.map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from(
            Path::new(file_name)
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .unwrap_or("media.bin"),
        )
    })
}

fn guess_media_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("heic") => "image/heic",
        Some("mp4") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("mp3") => "audio/mpeg",
        Some("m4a") => "audio/mp4",
        Some("wav") => "audio/wav",
        Some("ogg") => "audio/ogg",
        Some("txt") => "text/plain",
        Some("pdf") => "application/pdf",
        _ => "application/octet-stream",
    }
}

/// Parse a NIP-92 `imeta` tag (kind-9 media) into its `key value` fields.
/// Returns `None` if the message has no `imeta` tag.
fn imeta_fields(tags: &[Vec<String>]) -> Option<HashMap<String, String>> {
    let imeta = tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some("imeta"))?;
    let fields = imeta
        .iter()
        .skip(1)
        .filter_map(|field| field.split_once(' '))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
    Some(fields)
}
