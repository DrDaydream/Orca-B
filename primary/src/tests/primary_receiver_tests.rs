use super::*;
use crate::common::{keys, proposal, votes};
use crate::messages::GrbcPhase;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn mixed_aba_bypasses_a_full_core_queue_and_votes_are_forwarded_once() {
    let (tx_primary_messages, mut core) = channel(1);
    let (tx_consensus, mut consensus) = channel(10);
    let (tx_cert_requests, _requests) = channel(1);
    let tracker = ProposalTracker::default();
    tx_primary_messages
        .send(PrimaryInput::new(
            PrimaryMessage::Proposal(proposal()),
            &tracker,
        ))
        .await
        .unwrap();
    let handler = PrimaryReceiverHandler {
        tx_primary_messages,
        tx_consensus,
        proposal_tracker: tracker,
        tx_cert_requests,
    };
    let batch =
        VoteBatch::from_votes(vec![votes(&proposal().header, GrbcPhase::Ready).remove(0)]).unwrap();
    let echo =
        VoteBatch::from_votes(vec![votes(&proposal().header, GrbcPhase::Echo).remove(0)]).unwrap();
    let message = PrimaryMessage::Control(ControlFrame {
        grbc: vec![echo, batch],
        aba: Some(ConsensusNetworkMessage {
            author: keys()[0].0,
            messages: vec![vec![42]],
        }),
    });
    let bytes = bincode::serialize(&message).unwrap();
    let task =
        tokio::spawn(async move { handler.route(bincode::deserialize(&bytes).unwrap()).await });
    match timeout(Duration::from_secs(5), consensus.recv())
        .await
        .unwrap()
        .unwrap()
    {
        ConsensusMessage::AbaBatch(author, messages) => {
            assert_eq!(author, keys()[0].0);
            assert_eq!(messages, vec![vec![42]]);
        }
        _ => panic!("Expected ABA before Core becomes available"),
    }
    assert!(!task.is_finished());
    core.recv().await.unwrap();
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        let input = timeout(Duration::from_secs(5), core.recv())
            .await
            .unwrap()
            .unwrap();
        match input.message {
            PrimaryMessage::GrbcVoteBatch(batch) => {
                assert_eq!(batch.phase, phase);
                assert_eq!(batch.votes.len(), 1);
                batch.into_votes()[0]
                    .verify(&crate::common::committee())
                    .unwrap();
            }
            _ => panic!("Expected GRBC votes after independent ABA delivery"),
        }
    }
    task.await.unwrap();
    assert!(core.try_recv().is_err());
    assert!(consensus.try_recv().is_err());
}
