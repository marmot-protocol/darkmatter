//! Per-home default paths for the `dmd` runtime artifacts (socket, pid, log).

use std::path::{Path, PathBuf};

pub fn default_socket_path(home: &Path) -> PathBuf {
    home.join("dev").join("dmd.sock")
}

pub fn default_pid_path(home: &Path) -> PathBuf {
    home.join("dev").join("dmd.pid")
}

pub fn default_log_path(home: &Path) -> PathBuf {
    home.join("dev").join("dmd.log")
}
