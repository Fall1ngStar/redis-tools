use clap::Parser;
use fred::prelude::*;

/// A collection of useful commands to work with Redis / Valkey
#[derive(Debug, Parser)]
struct Args {
    #[command(flatten)]
    redis: RedisInfo,
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

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let client = setup_client(&args.redis).await?;
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
