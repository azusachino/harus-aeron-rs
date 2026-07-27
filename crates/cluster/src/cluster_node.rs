//! ClusterNode lifecycle launcher.
//!
//! Mirrors `io.aeron.cluster.ClusterNode.java`.

use crate::consensus_module::ConsensusModule;
use crate::state_machine::ClusterMember;

#[derive(Debug, Clone)]
pub struct ClusterNodeContext {
    pub member_id: i32,
    pub cluster_members: Vec<ClusterMember>,
}

impl ClusterNodeContext {
    pub fn new(member_id: i32, cluster_members: Vec<ClusterMember>) -> Self {
        Self {
            member_id,
            cluster_members,
        }
    }
}

pub struct ClusterNode {
    context: ClusterNodeContext,
    consensus_module: ConsensusModule,
    is_running: bool,
}

impl ClusterNode {
    pub fn launch(context: ClusterNodeContext) -> Self {
        let consensus_module =
            ConsensusModule::new(context.member_id, context.cluster_members.clone());
        Self {
            context,
            consensus_module,
            is_running: true,
        }
    }

    pub fn member_id(&self) -> i32 {
        self.context.member_id
    }

    pub fn consensus_module(&self) -> &ConsensusModule {
        &self.consensus_module
    }

    pub fn consensus_module_mut(&mut self) -> &mut ConsensusModule {
        &mut self.consensus_module
    }

    pub fn step(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        if !self.is_running {
            return Ok(0);
        }
        self.consensus_module.do_work()
    }

    pub fn close(&mut self) {
        self.is_running = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_launch_and_step_cluster_node() {
        let members = vec![
            ClusterMember::new(0, "aeron:ipc", "aeron:ipc"),
            ClusterMember::new(1, "aeron:ipc", "aeron:ipc"),
        ];

        let ctx = ClusterNodeContext::new(0, members);
        let mut node = ClusterNode::launch(ctx);

        assert_eq!(node.member_id(), 0);
        assert!(node.step().is_ok());

        node.close();
        assert_eq!(node.step().unwrap(), 0);
    }
}
