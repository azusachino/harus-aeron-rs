use std::error::Error;

use aeron_cluster::udp_runtime::{NodeConfig, run_node};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run_node(NodeConfig::from_env()?).await?;
    Ok(())
}
