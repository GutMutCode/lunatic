use anyhow::Result;
use clap::Parser;

use crate::mode::config::ConfigManager;

#[derive(Parser, Debug)]
pub struct Args {}

pub(crate) fn start(_args: Args) -> Result<()> {
    if ConfigManager::logout_local()? {
        println!("Logged out. The local CLI credential was removed.");
    } else {
        println!("No CLI login is configured.");
    }
    Ok(())
}
