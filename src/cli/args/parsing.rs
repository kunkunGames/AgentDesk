//! CLI parsing and compatibility spelling normalization.
use super::{Cli, ParseOutcome};
use clap::Parser;

pub(super) fn rewrite_legacy_args(mut args: Vec<String>) -> Vec<String> {
    if args.get(1).map(String::as_str) == Some("--emit-launchd-plist") {
        let mut rewritten = Vec::with_capacity(args.len() + 1);
        rewritten.push(args.remove(0));
        rewritten.push("emit-launchd-plist".to_string());
        rewritten.extend(args.into_iter().skip(1));
        return rewritten;
    }
    args
}

pub(crate) fn parse() -> ParseOutcome {
    match Cli::try_parse_from(rewrite_legacy_args(std::env::args().collect())) {
        Ok(cli) => match cli.command {
            Some(command) => ParseOutcome::Command {
                command,
                json: cli.json,
            },
            None => ParseOutcome::RunServer,
        },
        Err(error) => {
            if error.kind() == clap::error::ErrorKind::DisplayHelp
                || error.kind() == clap::error::ErrorKind::DisplayVersion
            {
                error.print().ok();
                std::process::exit(0);
            }
            let has_args = std::env::args().count() > 1;
            if has_args {
                error.print().ok();
                std::process::exit(1);
            }
            ParseOutcome::RunServer
        }
    }
}
