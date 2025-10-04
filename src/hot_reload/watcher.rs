use anyhow::{Context, Result};
use log::{info, warn};
use notify::{
    event::{EventKind, ModifyKind},
    Config, Event, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct FileWatcher {
    watcher: RecommendedWatcher,
    watched_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    pub path: PathBuf,
}

impl FileWatcher {
    pub fn new(path: impl AsRef<Path>, tx: mpsc::UnboundedSender<FileChangeEvent>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let watched_path = path.clone();

        let tx = Arc::new(tx);
        let watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    if should_trigger_reload(&event) {
                        info!("File changed: {:?}", event.paths);
                        for path in event.paths {
                            if path.extension().and_then(|s| s.to_str()) == Some("wasm") {
                                let _ = tx.send(FileChangeEvent { path });
                            }
                        }
                    }
                }
                Err(e) => warn!("File watch error: {:?}", e),
            },
            Config::default(),
        )
        .context("Failed to create file watcher")?;

        Ok(Self {
            watcher,
            watched_path,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        info!("Starting file watcher for: {:?}", self.watched_path);

        if self.watched_path.is_file() {
            let parent = self
                .watched_path
                .parent()
                .context("Failed to get parent directory")?;
            self.watcher
                .watch(parent, RecursiveMode::NonRecursive)
                .context("Failed to watch file parent directory")?;
        } else {
            self.watcher
                .watch(&self.watched_path, RecursiveMode::Recursive)
                .context("Failed to watch directory")?;
        }

        Ok(())
    }
}

fn should_trigger_reload(event: &Event) -> bool {
    matches!(
        event.kind,
        EventKind::Modify(ModifyKind::Data(_)) | EventKind::Create(_)
    )
}
