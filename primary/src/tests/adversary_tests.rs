use super::*;
use crate::common::committee;
use serde_json::json;

fn policy(mode: &str) -> Policy {
    let committee = committee();
    let keys: Vec<_> = committee.authorities.keys().copied().collect();
    Policy::parse(
        &json!({"mode": mode, "byzantine_authorities": if mode == "none" { Vec::<String>::new() } else { vec![keys[0].encode_base64()] }}).to_string(),
        keys[0],
        &committee,
    )
    .unwrap()
}

#[test]
fn fixed_membership_never_rotates_and_silence_is_round_bounded() {
    let mut policy = policy("silent");
    policy.config.start_round = 2;
    policy.config.rounds = 3;
    for round in 0..20 {
        assert_eq!(policy.silent(policy.name, round), (2..5).contains(&round));
        assert!(!policy.silent(policy.leaders[1], round));
    }
}

#[test]
fn strict_configuration_rejects_unknown_keys_modes_members_and_unsafe_limits() {
    let committee = committee();
    let name = *committee.authorities.keys().next().unwrap();
    let keys: Vec<_> = committee
        .authorities
        .keys()
        .map(PublicKey::encode_base64)
        .collect();
    for value in [
        json!({"mode":"A3"}),
        json!({"mode":"silent", "typo":1}),
        json!({"mode":"silent", "byzantine_authorities":["AA=="]}),
        json!({"mode":"silent", "byzantine_authorities":[PublicKey([77;32]).encode_base64()]}),
        json!({"mode":"network", "delay_ms":0}),
        json!({"mode":"network", "max_pending_messages":0}),
        json!({"mode":"network", "max_pending_bytes":0}),
        json!({"mode":"network", "start_round":0}),
        json!({"mode":"network", "start_round":u64::MAX, "rounds":2}),
        json!({"mode":"network", "delay_ms":u64::MAX}),
        json!({"mode":"silent"}),
        json!({"mode":"targeted"}),
        json!({"mode":"none", "byzantine_authorities":[keys[0]]}),
        json!({"mode":"targeted", "byzantine_authorities":[keys[0], keys[1]]}),
        json!({"mode":"targeted", "byzantine_authorities":[keys[0], keys[0]]}),
    ] {
        assert!(
            Policy::parse(&value.to_string(), name, &committee).is_err(),
            "accepted {}",
            value
        );
    }
}

#[test]
fn none_is_noop_and_targeted_cannot_delay_an_honest_relay() {
    let mut policy = policy("none");
    let leader = policy.leaders[1];
    for peer in &policy.leaders {
        assert!(!policy.should_delay(1, &[leader], *peer));
    }
    policy.config.mode = Mode::Targeted;
    policy.byzantine.insert(policy.leaders[0]);
    policy.name = policy.leaders[2];
    for round in 1..12 {
        for peer in &policy.leaders {
            assert!(!policy.should_delay(round, &[policy.leader(round)], *peer));
        }
    }
}

#[test]
fn target_is_actual_leader_and_recipient_groups_are_deterministic() {
    let policy = policy("targeted");
    for round in 1..12 {
        let leader = policy.leader(round);
        let recipients = policy
            .leaders
            .iter()
            .filter(|peer| policy.should_delay(round, &[leader], **peer))
            .count();
        assert!(recipients > 0 && recipients < policy.leaders.len());
        let other = policy.leaders[(round as usize + 1) % policy.leaders.len()];
        for peer in &policy.leaders {
            assert!(!policy.should_delay(round, &[other], *peer));
        }
    }
}

#[test]
fn network_can_delay_honest_evidence_but_never_outside_the_window() {
    let mut policy = policy("network");
    policy.name = policy.leaders[2];
    policy.config.start_round = 3;
    policy.config.rounds = 2;
    for round in 1..8 {
        let count = policy
            .leaders
            .iter()
            .filter(|peer| policy.should_delay(round, &[policy.leader(round)], **peer))
            .count();
        assert_eq!(count > 0, (3..5).contains(&round));
    }
}

#[test]
fn serialized_proposals_and_saved_votes_use_sender_identity_and_same_policy() {
    use crate::messages::{GrbcPhase, GrbcVote, GrbcVoteBatch};
    use crate::primary::PrimaryMessage;
    use crypto::{Hash as _, Signature};

    let mut p = crate::common::proposal();
    let mut policy = policy("targeted");
    let index = policy
        .leaders
        .iter()
        .position(|key| *key == p.header.author)
        .unwrap();
    p.header.round = if index == 0 {
        policy.leaders.len() as u64
    } else {
        index as u64
    };
    p.header.id = p.header.digest();
    let secret = crate::common::keys()
        .into_iter()
        .find(|(key, _)| *key == p.header.author)
        .unwrap()
        .1;
    p.header.signature = Signature::new(&p.header.id, &secret);
    p.verify(&committee()).unwrap();
    let recipient = policy
        .leaders
        .iter()
        .copied()
        .find(|peer| policy.should_delay(p.header.round, &[p.header.author], *peer))
        .unwrap();
    let address = committee().primary(&recipient).unwrap().primary_to_primary;
    let proposal = Bytes::from(bincode::serialize(&PrimaryMessage::Proposal(p.clone())).unwrap());
    assert!(matches!(
        policy.delivery(address, &proposal),
        Delivery::Delay(_, _)
    ));

    let mut vote = GrbcVote {
        id: p.header.id.clone(),
        round: p.header.round,
        origin: p.header.author,
        author: policy.name,
        phase: GrbcPhase::Ready,
        signature: Signature::default(),
    };
    let signing = crate::common::keys()
        .into_iter()
        .find(|(key, _)| *key == policy.name)
        .unwrap()
        .1;
    vote.signature = Signature::new(&vote.digest(), &signing);
    let votes = Bytes::from(
        bincode::serialize(&PrimaryMessage::GrbcVoteBatch(
            GrbcVoteBatch::from_votes(vec![vote]).unwrap(),
        ))
        .unwrap(),
    );
    assert!(matches!(
        policy.delivery(address, &votes),
        Delivery::Delay(_, _)
    ));
    policy.name = policy.leaders[1];
    assert!(
        matches!(policy.delivery(address, &proposal), Delivery::Send),
        "honest relay of the same signed data must stay honest in targeted mode"
    );
    policy.config.mode = Mode::Network;
    assert!(matches!(
        policy.delivery(address, &proposal),
        Delivery::Delay(_, _)
    ));
    assert!(matches!(
        policy.delivery(address, &votes),
        Delivery::Delay(_, _)
    ));
    policy.config.mode = Mode::None;
    assert!(matches!(
        policy.delivery(address, &proposal),
        Delivery::Send
    ));
    assert!(matches!(policy.delivery(address, &votes), Delivery::Send));
}

#[test]
fn default_configuration_is_a_direct_noop_and_round_zero_is_outside_window() {
    let committee = committee();
    let name = *committee.authorities.keys().next().unwrap();
    let policy = Policy::parse("{}", name, &committee).unwrap();
    let address = committee.primary(&name).unwrap().primary_to_primary;
    assert!(matches!(
        policy.delivery(address, &Bytes::new()),
        Delivery::Send
    ));
    assert!(!policy.active(0));
    assert!(policy.active(u64::MAX));
    assert_eq!(policy.config.delay_ms, 200);
    assert_eq!(policy.config.max_pending_messages, 4096);
}

#[tokio::test]
async fn honest_proposal_repair_obeys_network_delay_and_keeps_signature_bytes() {
    use crate::primary::PrimaryMessage;
    use crypto::{Hash as _, Signature};
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Instant};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut policy = policy("network");
    policy.name = policy.leaders[2];
    let mut proposal = crate::common::proposal();
    let index = policy
        .leaders
        .iter()
        .position(|key| *key == proposal.header.author)
        .unwrap();
    proposal.header.round = if index == 0 {
        policy.leaders.len() as u64
    } else {
        index as u64
    };
    proposal.header.id = proposal.header.digest();
    let secret = crate::common::keys()
        .into_iter()
        .find(|(key, _)| *key == proposal.header.author)
        .unwrap()
        .1;
    proposal.header.signature = Signature::new(&proposal.header.id, &secret);
    let recipient = policy
        .leaders
        .iter()
        .copied()
        .find(|peer| {
            *peer != policy.name
                && policy.should_delay(proposal.header.round, &[proposal.header.author], *peer)
        })
        .unwrap();
    policy.peers.insert(address, recipient);
    policy.config.delay_ms = 150;
    let mut sender =
        SimpleSender::with_schedule(Arc::new(policy), DelayBudget::new(4, 1024 * 1024));
    let wire = Bytes::from(bincode::serialize(&PrimaryMessage::Proposal(proposal)).unwrap());
    let started = Instant::now();
    sender.send(address, wire.clone()).await;
    assert!(timeout(Duration::from_millis(40), listener.accept())
        .await
        .is_err());
    let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
    let received = stream.next().await.unwrap().unwrap();
    assert_eq!(received.as_ref(), wire.as_ref());
    assert!(started.elapsed() >= Duration::from_millis(150));
    match bincode::deserialize::<PrimaryMessage>(&received).unwrap() {
        PrimaryMessage::Proposal(proposal) => proposal.verify(&committee()).unwrap(),
        _ => panic!("wrong message kind"),
    }
    stream.send(Bytes::from_static(b"Ack")).await.unwrap();
}

#[test]
fn aba_batches_split_by_instance_without_changing_messages() {
    use crate::messages::{AbaMessage, AbaMessageKind, BinaryValue};
    let one = AbaMessage {
        instance: 7,
        round: 2,
        kind: AbaMessageKind::BVal(BinaryValue::One),
    };
    let two = AbaMessage {
        instance: 8,
        round: 4,
        kind: AbaMessageKind::Aux(BinaryValue::Zero),
    };
    let input = vec![bincode::serialize(&vec![one.clone(), two.clone()]).unwrap()];
    let groups = group_aba_messages(input);
    assert_eq!(groups.len(), 2);
    assert_eq!(
        bincode::deserialize::<Vec<AbaMessage>>(&groups[0][0]).unwrap(),
        vec![one]
    );
    assert_eq!(
        bincode::deserialize::<Vec<AbaMessage>>(&groups[1][0]).unwrap(),
        vec![two]
    );
}

#[tokio::test]
async fn only_a5_delays_signed_aba_envelopes_using_the_instance_round() {
    use crate::messages::{AbaMessage, AbaMessageKind, BinaryValue, ConsensusNetworkMessage};
    let mut policy = policy("targeted");
    policy.config.start_round = 7;
    policy.config.rounds = 1;
    let recipient = policy
        .leaders
        .iter()
        .copied()
        .find(|peer| policy.should_delay(7, &[policy.leader(7)], *peer))
        .unwrap();
    let address = committee().primary(&recipient).unwrap().aba_to_aba;
    let key = crate::common::keys()
        .into_iter()
        .find(|(key, _)| *key == policy.name)
        .unwrap();
    let mut signature_service = crypto::SignatureService::new(key.1);
    let messages = vec![AbaMessage {
        instance: 7,
        round: 2,
        kind: AbaMessageKind::Aux(BinaryValue::One),
    }];
    let batches = vec![bincode::serialize(&messages).unwrap()];
    let envelope = ConsensusNetworkMessage::new(
        bincode::serialize(&batches).unwrap(),
        policy.name,
        &mut signature_service,
    )
    .await;
    envelope.verify(&committee()).unwrap();
    let wire = Bytes::from(bincode::serialize(&envelope).unwrap());
    assert!(matches!(policy.delivery(address, &wire), Delivery::Send),
            "A3 must deliver ABA normally, including from a Byzantine sender");
    for kind in [AbaMessageKind::BVal(BinaryValue::One),
                 AbaMessageKind::Aux(BinaryValue::Zero),
                 AbaMessageKind::Decide(BinaryValue::One)] {
        let messages = vec![AbaMessage { instance: 7, round: 2, kind }];
        let batches = vec![bincode::serialize(&messages).unwrap()];
        let envelope = ConsensusNetworkMessage::new(
            bincode::serialize(&batches).unwrap(), policy.name, &mut signature_service,
        ).await;
        let packet = Bytes::from(bincode::serialize(&envelope).unwrap());
        policy.config.mode = Mode::Targeted;
        assert!(matches!(policy.delivery(address, &packet), Delivery::Send));
        policy.config.mode = Mode::Network;
        assert!(matches!(policy.delivery(address, &packet), Delivery::Delay(_, _)),
                "A5 still delays each ABA message kind inside the instance window");
    }
    policy.config.mode = Mode::Silent;
    assert!(matches!(policy.delivery(address, &wire), Delivery::Send));
    policy.name = policy.leaders[1];
    assert!(!policy.schedules_local_sender());
    assert!(matches!(policy.delivery(address, &wire), Delivery::Send));
    policy.config.mode = Mode::Targeted;
    assert!(
        !policy.schedules_local_sender(),
        "honest batches must not be regrouped or re-signed"
    );
    assert!(matches!(policy.delivery(address, &wire), Delivery::Send));
    policy.config.mode = Mode::Network;
    assert!(matches!(
        policy.delivery(address, &wire),
        Delivery::Delay(_, _)
    ));
    policy.config.start_round = 2;
    assert!(matches!(policy.delivery(address, &wire), Delivery::Send));
}
