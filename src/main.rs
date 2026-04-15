//! `ferrous` CLI entry point — a thin runner around [`ferrous::cli`] and
//! [`ferrous::commands`].

use std::process::ExitCode;

use clap::Parser;
use ferrous::cli::{Cli, Command};
use ferrous::commands;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Search(args) => commands::run_search(&cli, args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ferrous: {e}");
            ExitCode::FAILURE
        }
    }
}
