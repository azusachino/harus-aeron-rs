use std::error::Error;

use aeron_cluster::udp_runtime::{ClientConfig, run_client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let accepted = run_client(ClientConfig::from_env()?).await?;
    if accepted == 0 {
        return Err("no orders were accepted".into());
    }
    Ok(())
}
