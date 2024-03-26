use anyhow::{Ok, Result};
use dotenv;
use fern::colors::{Color, ColoredLevelConfig};
use log::{error, info, warn, LevelFilter};
use new_project::trace::mempool_watching;
use tokio;

pub fn setup_logger() -> Result<()> {
    let colors = ColoredLevelConfig {
        trace: Color::Cyan,
        debug: Color::Magenta,
        info: Color::Green,
        warn: Color::Red,
        error: Color::BrightRed,
        ..ColoredLevelConfig::new()
    };

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                chrono::Local::now().format("[%H:%M:%S]"),
                colors.color(record.level()),
                message
            ))
        })
        .chain(std::io::stdout())
        .level(log::LevelFilter::Info)
        .level_for("new-project", LevelFilter::Info)
        .apply()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    setup_logger()?;

    let weth = String::from("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    mempool_watching(weth).await?;
    Ok(())
}
