//! IPC via Unix domain socket.
//!
//! The daemon listens on `$XDG_RUNTIME_DIR/ashell.sock`.
//! The same binary acts as a client via `ashell msg <command>`.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use crate::xdg;
use anyhow::{Context, Result, anyhow};
use clap::Subcommand;
use iced::Subscription;
use serde::{Deserialize, Serialize};

use iced::futures::StreamExt;

/// Maximum bytes to read from a client connection.
const MAX_REQUEST_LEN: u64 = 4096;

/// IPC command that can be sent to the daemon.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum IpcCommand {
    /// Toggle bar visibility
    ToggleVisibility,
    VolumeUp {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    VolumeDown {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    VolumeToggleMute {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    MicrophoneUp {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    MicrophoneDown {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    MicrophoneToggleMute {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    BrightnessUp {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    BrightnessDown {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    ToggleAirplaneMode {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
    ToggleIdleInhibitor {
        #[arg(long)]
        #[serde(default)]
        no_osd: bool,
    },
}

impl IpcCommand {
    pub fn no_osd(&self) -> bool {
        match self {
            IpcCommand::ToggleVisibility => false,
            IpcCommand::VolumeUp { no_osd }
            | IpcCommand::VolumeDown { no_osd }
            | IpcCommand::VolumeToggleMute { no_osd }
            | IpcCommand::MicrophoneUp { no_osd }
            | IpcCommand::MicrophoneDown { no_osd }
            | IpcCommand::MicrophoneToggleMute { no_osd }
            | IpcCommand::BrightnessUp { no_osd }
            | IpcCommand::BrightnessDown { no_osd }
            | IpcCommand::ToggleAirplaneMode { no_osd }
            | IpcCommand::ToggleIdleInhibitor { no_osd } => *no_osd,
        }
    }
}

pub fn socket_path() -> PathBuf {
    match xdg::get_runtime_dir() {
        Some(dir) => dir.join("ashell.sock"),
        None => {
            let uid = unsafe { libc::getuid() };
            std::env::temp_dir().join(format!("ashell-{uid}.sock"))
        }
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Run the IPC client: connect to the daemon, send a command, print the response.
pub fn run_client(cmd: &IpcCommand) -> Result<()> {
    let path = socket_path();
    let mut stream = UnixStream::connect(&path)
        .with_context(|| format!("connect to {} — is ashell running?", path.display()))?;

    stream
        .write_all(encode_request(cmd)?.as_bytes())
        .context("send command")?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut response = String::new();
    BufReader::new((&stream).take(MAX_REQUEST_LEN))
        .read_line(&mut response)
        .context("read response")?;
    let response = response.trim_end();

    if let Some(err) = response.strip_prefix("error ") {
        return Err(anyhow!("{err}"));
    }

    if !response.is_empty() {
        println!("{response}");
    }
    Ok(())
}

fn encode_request(cmd: &IpcCommand) -> Result<String> {
    let mut line = serde_json::to_string(cmd).context("serialize IPC command")?;
    line.push('\n');
    Ok(line)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

enum ListenerError {
    /// Another ashell instance is already listening on the socket.
    AlreadyRunning,
    Other(anyhow::Error),
}

/// Create the Unix listener, taking care not to steal a live server's socket.
///
/// The socket path is shared across instances, so we probe it first: a
/// successful connect means a primary is already serving and we must not
/// remove the file or bind a new listener — otherwise we'd orphan the
/// primary's fd and break `ashell msg` until it's restarted.
fn create_listener() -> std::result::Result<UnixListener, ListenerError> {
    let path = socket_path();

    match UnixStream::connect(&path) {
        Ok(_) => return Err(ListenerError::AlreadyRunning),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            if let Err(e) = std::fs::remove_file(&path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(ListenerError::Other(
                    anyhow::Error::new(e)
                        .context(format!("remove stale socket {}", path.display())),
                ));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(ListenerError::Other(
                anyhow::Error::new(e).context(format!("probe socket {}", path.display())),
            ));
        }
    }

    let listener = UnixListener::bind(&path)
        .with_context(|| format!("bind {}", path.display()))
        .map_err(ListenerError::Other)?;
    listener
        .set_nonblocking(true)
        .context("set_nonblocking")
        .map_err(ListenerError::Other)?;
    log::info!("IPC listening on {}", path.display());
    Ok(listener)
}

/// Read a single command from an accepted client connection.
fn read_request(stream: &UnixStream) -> Result<IpcCommand> {
    let mut line = String::new();
    BufReader::new(stream.take(MAX_REQUEST_LEN))
        .read_line(&mut line)
        .context("read IPC command")?;
    serde_json::from_str(line.trim()).context("parse IPC JSON command")
}

/// Write a response line to the client.
fn write_response(stream: &mut UnixStream, response: &str) {
    if let Err(e) = writeln!(stream, "{response}") {
        log::debug!("IPC write response failed: {e}");
    }
}

/// Handle a single accepted client connection.
fn handle_connection(mut stream: UnixStream) -> Option<IpcCommand> {
    match read_request(&stream) {
        Ok(cmd) => {
            write_response(&mut stream, "ok");
            Some(cmd)
        }
        Err(e) => {
            write_response(&mut stream, &format!("error {e:#}"));
            None
        }
    }
}

fn init_listener() -> Option<tokio::net::UnixListener> {
    let std_listener = match create_listener() {
        Ok(l) => l,
        Err(ListenerError::AlreadyRunning) => {
            log::warn!(
                "another ashell instance owns the IPC socket; this instance will run without IPC"
            );
            return None;
        }
        Err(ListenerError::Other(e)) => {
            log::error!("Failed to create IPC listener: {e:#}");
            return None;
        }
    };
    match tokio::net::UnixListener::from_std(std_listener) {
        Ok(l) => Some(l),
        Err(e) => {
            log::error!("Failed to convert IPC listener to tokio: {e}");
            None
        }
    }
}

/// Subscription that listens for IPC commands on the Unix socket.
pub fn subscription() -> Subscription<IpcCommand> {
    Subscription::run(|| {
        iced::futures::stream::unfold(None::<tokio::net::UnixListener>, |listener| async {
            let listener = match listener {
                Some(l) => l,
                None => init_listener()?,
            };
            let (request, listener) = match listener.accept().await {
                Ok((stream, _)) => {
                    let request = match stream.into_std() {
                        Ok(std_stream) => handle_connection(std_stream),
                        Err(e) => {
                            log::error!("IPC stream conversion error: {e}");
                            None
                        }
                    };
                    (request, listener)
                }
                Err(e) => {
                    log::error!("IPC accept error: {e}");
                    (None, listener)
                }
            };
            Some((request, Some(listener)))
        })
        .filter_map(iced::futures::future::ready)
    })
}
