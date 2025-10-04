use std::pin::Pin;

use clap::{ArgAction, Parser};
use counter::Counter;
use fred::{
    prelude::*,
    types::scan::{ScanResult, Scanner},
};
use indicatif::{ProgressBar, ProgressStyle};
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
    ScanKeys(ScanOptions),

    /// Get all the values for keys matching a pattern
    ///
    /// Only work with string values
    AllItems {
        #[command(flatten)]
        scan_options: ScanOptions,

        /// Limit the number of items returned
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Delete all the keys matching a pattern
    DelPattern {
        #[command(flatten)]
        scan_options: ScanOptions,

        /// Execute the deletion
        #[arg(long = "no-dry-run", default_value_t=true, action = ArgAction::SetFalse)]
        dry_run: bool,
    },

    /// Count the numbers of keys with a common key structure
    ComputeStats {
        #[command(flatten)]
        scan_options: ScanOptions,

        /// Delimiter used by each group
        ///
        /// For instance, the key "abc:123:456" will belong to group "abc"
        #[arg(short, long, default_value = ":")]
        delimiter: String,

        /// Common prefix to remove before computing the stats
        ///
        /// For instance, with the prefix "abc:", key "abc:123:456" will belong to group "123"
        #[arg(long)]
        prefix: Option<String>,
    },
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let client = setup_client(&args.redis).await?;
    match args.command {
        Commands::ScanKeys(scan_options) => {
            let keys = scan(&client, &scan_options).await?;
            keys.iter().for_each(|key| println!("{key}"));
        }
        Commands::AllItems {
            scan_options,
            limit,
        } => {
            all_items(&client, &scan_options, limit).await?;
        }
        Commands::DelPattern {
            scan_options,
            dry_run,
        } => {
            del_pattern(&client, &scan_options, dry_run).await?;
        }
        Commands::ComputeStats {
            scan_options,
            delimiter,
            prefix,
        } => {
            compute_stats(&client, &scan_options, &delimiter, prefix).await?;
        }
    }
    Ok(())
}

#[derive(Debug, clap::Args)]
struct ScanOptions {
    /// Pattern to scan
    #[arg(default_value = "*")]
    pattern: String,

    /// Sort the results
    #[arg(short, long, action)]
    sorted: bool,

    /// Reverse the results
    #[arg(short, long, action)]
    reversed: bool,
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

async fn scan(client: &Client, options: &ScanOptions) -> color_eyre::Result<Vec<String>> {
    let mut stream = scan_stream(client, &options.pattern)?;
    let mut result = Vec::new();
    while let Some(page) = stream.next().await {
        let mut page = page?;
        let keys = page.take_results().unwrap_or_default();
        result.extend(keys.into_iter().flat_map(|key| key.into_string()));
    }
    if options.sorted {
        result.sort();
    }
    if options.reversed {
        result.reverse();
    }
    Ok(result)
}

async fn all_items(
    client: &Client,
    scan_options: &ScanOptions,
    limit: Option<usize>,
) -> color_eyre::Result<()> {
    let mut keys = scan(client, scan_options).await?;
    if let Some(limit) = limit {
        keys = keys.into_iter().take(limit).collect();
    }
    for chunk in keys.chunks(1000) {
        let pipe = client.pipeline();
        for key in chunk {
            let _: () = pipe.get(key).await?;
        }
        let result: Vec<String> = pipe.all().await?;
        for item in result {
            println!("{item}");
        }
    }
    Ok(())
}

async fn del_pattern(
    client: &Client,
    scan_options: &ScanOptions,
    dry_run: bool,
) -> color_eyre::Result<()> {
    let keys = scan(client, scan_options).await?;
    if dry_run {
        println!("{} keys to delete", keys.len());
        return Ok(());
    }
    let pb = ProgressBar::new(keys.len() as u64).with_style(ProgressStyle::with_template(
        "[{elapsed_precise}/{eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )?);
    pb.set_message(format!(
        "Deleting keys from pattern {}",
        scan_options.pattern
    ));

    for chunk in keys.chunks(1000) {
        let _: () = client.del(keys.to_vec()).await?;
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        pb.inc(chunk.len() as u64);
    }

    Ok(())
}

async fn compute_stats(
    client: &Client,
    scan_options: &ScanOptions,
    delimiter: &str,
    prefix: Option<String>,
) -> color_eyre::Result<()> {
    let keys = scan(client, scan_options).await?;
    let mut counter = Counter::<String>::new();
    let prefix = &prefix.unwrap_or_default();
    let mut other = 0;
    for key in keys {
        let Some(key) = key.strip_prefix(prefix) else {
            other += 1;
            continue;
        };
        if let Some((group, _)) = key.split_once(delimiter) {
            counter[&group.to_owned()] += 1;
        } else {
            counter[&"other".to_owned()] += 1;
        }
    }
    let mut b = tabled::builder::Builder::with_capacity(counter.len() + 1, 2);
    b.push_record(["prefix", "count"]);
    for (group, count) in counter.most_common() {
        b.push_record([&format!("{prefix}{group}"), &count.to_string()]);
    }
    let mut table = b.build();
    table.with(tabled::settings::Style::psql());
    println!("{table}");
    if other > 0 {
        println!("Keys not matching prefix \"{prefix}\": {other}");
    }
    Ok(())
}
