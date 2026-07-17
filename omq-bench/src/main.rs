mod bench;
mod cli;
mod coord;
mod jsonl;
mod parse;
mod process;

use clap::Parser;
use cli::{Command, RunSub};

fn main() {
    let cli = cli::Cli::parse();
    process::install_reaper();

    let result = std::panic::catch_unwind(|| match cli.command {
        Command::Run { sub } => match sub {
            RunSub::Comparisons(args) => bench::comparisons::run(args),
            RunSub::Mechanism(args) => bench::mechanism::run(args),
            RunSub::PubsubLz4(args) => bench::pubsub_lz4::run(args),
            RunSub::Compression(args) => bench::compression::run(args),
        },
    });

    process::reap_all();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
