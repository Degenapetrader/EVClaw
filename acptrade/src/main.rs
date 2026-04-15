use anyhow::Result;
use clap::Parser;
use env_logger::Env;
use log::info;

use evclaw::config::{Config, RuntimeOverrides};
use evclaw::runtime::EvClawRuntime;

#[derive(Debug, Parser)]
#[command(name = "evclaw")]
#[command(about = "Directional Hyperliquid trader for DEAD_CAP and WHALE")]
struct Cli {
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Config::load(RuntimeOverrides {
        dry_run_flag: cli.dry_run,
        symbols: if cli.symbols.is_empty() {
            None
        } else {
            Some(cli.symbols)
        },
    })?;

    env_logger::Builder::from_env(Env::default().default_filter_or(cfg.log_level.clone())).init();

    if !cfg.enabled {
        info!("EVCLAW_ENABLED=false, exiting");
        return Ok(());
    }

    let mut runtime = EvClawRuntime::new(cfg).await?;
    runtime.run(cli.once).await
}
