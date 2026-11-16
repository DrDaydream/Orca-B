// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        Digest(Sha512::digest(self).as_slice()[..32].try_into().unwrap())
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

#[test]
fn import_export_public_key() {
    let (public_key, _) = keys().pop().unwrap();
    let export = public_key.encode_base64();
    let import = PublicKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let (_, secret_key) = keys().pop().unwrap();
    let export = secret_key.encode_base64();
    let import = SecretKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    let bad_message: &[u8] = b"Bad message!";
    let digest = bad_message.digest();
    assert!(signature.verify(&digest, &public_key).is_err());
}

#[test]
fn fixed_key_verifier_keeps_authentication_strict() {
    let pairs = keys();
    let verifier = SignatureVerifier::new(pairs.iter().take(2).map(|(key, _)| *key));
    let digest = Digest([7; 32]);
    let signature = Signature::new(&digest, &pairs[0].1);

    for _ in 0..3 {
        verifier.verify(&signature, &digest, &pairs[0].0).unwrap();
        assert!(verifier
            .verify(&signature, &Digest([8; 32]), &pairs[0].0)
            .is_err());
        assert!(verifier.verify(&signature, &digest, &pairs[1].0).is_err());
        assert!(verifier
            .verify(&Signature::default(), &digest, &pairs[0].0)
            .is_err());
    }
    let outsider = Signature::new(&digest, &pairs[2].1);
    assert!(outsider.verify(&digest, &pairs[2].0).is_ok());
    assert!(verifier.verify(&outsider, &digest, &pairs[2].0).is_err());

    // A decodable point is not sufficient: keep verify_strict's rejection of
    // small-order keys, even for an identity R with scalar zero.
    let mut identity = [0; 32];
    identity[0] = 1;
    let weak_key = PublicKey(identity);
    let weak_signature = Signature {
        part1: identity,
        part2: [0; 32],
    };
    let verifier = SignatureVerifier::new([weak_key]);
    assert!(weak_signature.verify(&digest, &weak_key).is_err());
    assert!(verifier
        .verify(&weak_signature, &digest, &weak_key)
        .is_err());
}

#[test]
fn invalid_configured_key_does_not_disable_valid_members() {
    let (public, secret) = keys().pop().unwrap();
    let invalid = (0u8..=255)
        .map(|byte| PublicKey([byte; 32]))
        .find(|key| dalek::PublicKey::from_bytes(&key.0).is_err())
        .unwrap();
    let verifier = SignatureVerifier::new([public, invalid]);
    let digest = Digest([9; 32]);
    let signature = Signature::new(&digest, &secret);
    verifier.verify(&signature, &digest, &public).unwrap();
    assert!(verifier.verify(&signature, &digest, &invalid).is_err());
}

#[test]
fn verify_valid_batch() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let signatures: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_ok());
}

#[test]
fn verify_invalid_batch() {
    // Make 2 valid signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let mut signatures: Vec<_> = (0..2)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Add an invalid signature.
    let (public_key, _) = keys.pop().unwrap();
    signatures.push((public_key, Signature::default()));

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest.clone()).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}
