use std::pin::Pin;

use clap::Parser;
use fred::{
    prelude::*,
    types::scan::{ScanResult, Scanner},
};
use tokio_stream::{Stream, StreamExt};

/// A collection of useful commands to work with Redis / Valkey
#[derive(Debug, Parser)]
struct Args {
    #[command(flatten)]
    redis: RedisInfo,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Args)]
struct RedisInfo {
    /// Enable cluster mode
    #[arg(short, long, action)]
    cluster: bool,

    /// Connection URL to the instance
    #[arg(short, long, default_value = "redis://localhost:6379")]
    url: String,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// List all the keys matching a pattern
    ScanKeys {
        /// Pattern to scan
        #[arg(default_value = "*")]
        pattern: String,

        /// Sort the results
        #[arg(short, long, action)]
        sorted: bool,

        /// Reverse the results
        #[arg(short, long, action)]
        reversed: bool,
    },
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let client = setup_client(&args.redis).await?;
    match args.command {
        Commands::ScanKeys {
            pattern,
            sorted,
            reversed,
        } => {
            let mut keys = scan(&client, &pattern).await?;
            if sorted {
                keys.sort();
            }
            if reversed {
                keys.reverse();
            }
            keys.iter().for_each(|key| println!("{key}"));
        }
    }
    Ok(())
}

async fn setup_client(info: &RedisInfo) -> color_eyre::Result<Client> {
    let config = if info.cluster {
        Config::from_url_clustered(&info.url)?
    } else {
        Config::from_url(&info.url)?
    };
    let client = Client::new(config, None, None, None);
    client.init().await?;
    Ok(client)
}

fn scan_stream<'a>(
    client: &'a Client,
    pattern: &'a str,
) -> color_eyre::Result<Pin<Box<dyn Stream<Item = FredResult<ScanResult>> + 'a>>> {
    if client.is_clustered() {
        Ok(Box::pin(client.scan_cluster(pattern, Some(10_000), None)))
    } else {
        Ok(Box::pin(client.scan(pattern, Some(10_000), None)))
    }
}

async fn scan(client: &Client, pattern: &str) -> color_eyre::Result<Vec<String>> {
    let mut stream = scan_stream(client, pattern)?;
    let mut result = Vec::new();
    while let Some(page) = stream.next().await {
        let mut page = page?;
        let keys = page.take_results().unwrap_or_default();
        result.extend(keys.into_iter().flat_map(|key| key.into_string()));
    }
    Ok(result)
}
