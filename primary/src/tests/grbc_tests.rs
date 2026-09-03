use super::GradeOneVotesAggregator;
use crate::common::{committee, header, votes};
#[test]
fn grade_one_votes_form_grade_two_certificate_at_quorum() {
    let committee = committee();
    let header = header();
    let mut aggregator = GradeOneVotesAggregator::new();
    let mut certificate = None;

    for vote in votes(&header) {
        certificate = aggregator.append(vote, &committee, &header).unwrap();
        if certificate.is_some() {
            break;
        }
    }

    let certificate = certificate.expect("a quorum of grade-1 votes must produce grade 2");
    assert_eq!(certificate.header.id, header.id);
    certificate.verify(&committee).unwrap();
}

#[test]
fn duplicate_grade_one_votes_are_rejected() {
    let committee = committee();
    let header = header();
    let vote = votes(&header).pop().unwrap();
    let mut aggregator = GradeOneVotesAggregator::new();

    assert!(aggregator.append(vote.clone(), &committee, &header).is_ok());
    assert!(aggregator.append(vote, &committee, &header).is_err());
}

#[test]
fn grade_one_vote_is_bound_to_one_header() {
    let committee = committee();
    let header = header();
    let mut vote = votes(&header).pop().unwrap();
    vote.id = Default::default();
    let mut aggregator = GradeOneVotesAggregator::new();

    assert!(aggregator.append(vote, &committee, &header).is_err());
}
