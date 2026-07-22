use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CustomExtension, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType, SubjectPublicKeyInfo,
};
use x509_parser::{
    certification_request::X509CertificationRequest,
    cri_attributes::ParsedCriAttribute,
    extensions::{GeneralName, ParsedExtension},
    pem::parse_x509_pem,
    prelude::FromDer,
};

use crate::{CertAttrs, SUBJECT_DIR_ATTRS};

pub static TEST_ROOT_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBnDCCAUGgAwIBAgIIR5Hk+O5RdOgwCgYIKoZIzj0EAwIwKTEQMA4GA1UEAwwH
Um9vdCBDQTEVMBMGA1UECgwMTHVuYXRpYyBJbmMuMCAXDTc1MDEwMTAwMDAwMFoY
DzQwOTYwMTAxMDAwMDAwWjApMRAwDgYDVQQDDAdSb290IENBMRUwEwYDVQQKDAxM
dW5hdGljIEluYy4wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARlVNxYAwsmmFNc
2EMBbZZVwL8GBtnnu8IROdDd68ixc0VBjfrV0zAM344lKJcs9slsMTEofoYvMCpI
BhnSGyAFo1EwTzAdBgNVHREEFjAUghJyb290Lmx1bmF0aWMuY2xvdWQwHQYDVR0O
BBYEFOh0Ue745JFH76xErjqkW2/SbHhAMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI
zj0EAwIDSQAwRgIhAJKPv4XUZ9ej+CVgsJ+9x/CmJEcnebyWh2KntJri97nxAiEA
/KvaQE6GtYZPGFv/WYM3YEmTQ7hoOvaaAuvD27cHkaw=
-----END CERTIFICATE-----
"#;

pub static CTRL_SERVER_NAME: &str = "ctrl.lunatic.cloud";

static TEST_ROOT_KEYS: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg9ferf0du4h975Jhu
boMyGfdI+xwp7ewOulGvpTcvdpehRANCAARlVNxYAwsmmFNc2EMBbZZVwL8GBtnn
u8IROdDd68ixc0VBjfrV0zAM344lKJcs9slsMTEofoYvMCpIBhnSGyAF
-----END PRIVATE KEY-----"#;

/// A certificate authority together with the private key used to sign certificates.
///
/// `rcgen` intentionally separates issued certificates from their signing keys. This
/// wrapper keeps the existing Lunatic API explicit while retaining both PEM values for
/// transport to guest code and an [`Issuer`] for certificate generation.
pub struct CertificateAuthority {
    certificate_pem: String,
    private_key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl CertificateAuthority {
    /// Generate a fresh root authority for an ephemeral control-plane lifetime.
    ///
    /// In-memory control state must not restart with the same trust root and a reset node-ID
    /// counter, because that could make an old leaf identity active again.
    pub fn generate() -> Result<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "Lunatic Control Root");
        params
            .distinguished_name
            .push(DnType::OrganizationName, "Lunatic Inc.");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let signing_key = KeyPair::generate().context("failed to generate control root key")?;
        let certificate_pem = params
            .self_signed(&signing_key)
            .context("failed to generate control root certificate")?
            .pem();
        Self::from_pem(certificate_pem, &signing_key.serialize_pem())
    }

    pub fn from_pem(certificate_pem: impl Into<String>, private_key_pem: &str) -> Result<Self> {
        let certificate_pem = certificate_pem.into();
        let signing_key = KeyPair::from_pem(private_key_pem)
            .context("failed to parse certificate authority private key")?;
        let private_key_pem = signing_key.serialize_pem();
        let issuer = Issuer::from_ca_cert_pem(&certificate_pem, signing_key)
            .context("failed to parse certificate authority certificate")?;

        Ok(Self {
            certificate_pem,
            private_key_pem,
            issuer,
        })
    }

    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    pub fn issuer(&self) -> &Issuer<'static, KeyPair> {
        &self.issuer
    }
}

/// A freshly generated certificate identity that can create a CSR or be signed locally.
pub struct CertificateRequest {
    params: CertificateParams,
    signing_key: KeyPair,
}

impl CertificateRequest {
    pub fn new(params: CertificateParams) -> Result<Self> {
        let signing_key = KeyPair::generate().context("failed to generate certificate key pair")?;
        Ok(Self {
            params,
            signing_key,
        })
    }

    pub fn serialize_request_pem(&self) -> Result<String> {
        self.params
            .serialize_request(&self.signing_key)
            .context("failed to generate certificate signing request")?
            .pem()
            .context("failed to serialize certificate signing request")
    }

    pub fn serialize_private_key_pem(&self) -> String {
        self.signing_key.serialize_pem()
    }

    pub fn serialize_private_key_der(&self) -> Vec<u8> {
        self.signing_key.serialize_der()
    }

    pub fn signed_by(&self, authority: &CertificateAuthority) -> Result<Certificate> {
        self.params
            .signed_by(&self.signing_key, authority.issuer())
            .context("failed to sign certificate")
    }

    pub fn serialize_pem_with_signer(&self, authority: &CertificateAuthority) -> Result<String> {
        Ok(self.signed_by(authority)?.pem())
    }
}

pub fn test_root_cert() -> Result<CertificateAuthority> {
    CertificateAuthority::from_pem(TEST_ROOT_CERT, TEST_ROOT_KEYS)
}

pub fn root_cert(ca_cert: &str, ca_keys: &str) -> Result<CertificateAuthority> {
    let ca_cert_pem = std::fs::read_to_string(Path::new(ca_cert))
        .with_context(|| format!("failed to read CA certificate from {ca_cert}"))?;
    let ca_keys_pem = std::fs::read_to_string(Path::new(ca_keys))
        .with_context(|| format!("failed to read CA private key from {ca_keys}"))?;
    CertificateAuthority::from_pem(ca_cert_pem, &ca_keys_pem)
}

/// Sign a node CSR after replacing any caller-controlled Lunatic attributes.
///
/// A CSR can contain arbitrary requested extensions. In particular, it must
/// not be allowed to choose its own numeric node identity or permissions. This
/// helper strips every reserved Lunatic attribute extension before inserting
/// the control plane's authoritative values.
pub fn sign_node_certificate(
    csr_pem: &str,
    authority: &CertificateAuthority,
    expected_dns_name: &str,
    attrs: &CertAttrs,
) -> Result<String> {
    sign_node_certificate_inner(csr_pem, authority, Some(expected_dns_name), attrs)
}

/// Preserve the legacy guest host function while applying the controlled leaf profile.
///
/// Network-facing registration paths must use [`sign_node_certificate`] with the registered node
/// name. This compatibility helper trusts the CSR name and therefore must remain confined to
/// already-privileged guest code.
pub fn sign_node_certificate_using_csr_name(
    csr_pem: &str,
    authority: &CertificateAuthority,
    attrs: &CertAttrs,
) -> Result<String> {
    sign_node_certificate_inner(csr_pem, authority, None, attrs)
}

fn sign_node_certificate_inner(
    csr_pem: &str,
    authority: &CertificateAuthority,
    expected_dns_name: Option<&str>,
    attrs: &CertAttrs,
) -> Result<String> {
    if let Some(node_id) = attrs.node_id {
        anyhow::ensure!(
            (1..=crate::distributed::MAX_NODE_ID).contains(&node_id),
            "node certificate identity is outside the compact distributed ID range"
        );
    }
    let (pem_remainder, pem) = parse_x509_pem(csr_pem.as_bytes())
        .map_err(|error| anyhow::anyhow!(error.to_string()))
        .context("failed to parse node certificate signing request PEM")?;
    anyhow::ensure!(
        pem_remainder.iter().all(u8::is_ascii_whitespace),
        "node certificate signing request contains trailing PEM data"
    );
    let (remainder, request) = X509CertificationRequest::from_der(&pem.contents)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
        .context("failed to parse node certificate signing request DER")?;
    anyhow::ensure!(
        remainder.is_empty(),
        "node certificate signing request contains trailing DER data"
    );
    request
        .verify_signature()
        .map_err(|error| anyhow::anyhow!(error.to_string()))
        .context("failed to verify node certificate signing request signature")?;

    let request_info = &request.certification_request_info;
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    for relative_name in request_info.subject.iter() {
        let mut attributes = relative_name.iter();
        let attribute = attributes
            .next()
            .context("node CSR contains an empty distinguished-name component")?;
        anyhow::ensure!(
            attributes.next().is_none(),
            "node CSR contains a multi-valued distinguished-name component"
        );
        let oid = attribute
            .attr_type()
            .iter()
            .context("node CSR contains an invalid distinguished-name OID")?
            .collect::<Vec<_>>();
        let value = attribute
            .as_str()
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .context("node CSR contains an unsupported distinguished-name value")?;
        params
            .distinguished_name
            .push(DnType::from_oid(&oid), value.to_owned());
    }

    // The CSR controls only one DNS subject alternative name. Copying arbitrary requested
    // extensions would let a registrant request CA:true/keyCertSign and turn the root-signed
    // node leaf into an intermediate capable of minting arbitrary node identities.
    let mut requested_dns_name = None;
    for attribute in request_info.iter_attributes() {
        if let ParsedCriAttribute::ExtensionRequest(extensions) = attribute.parsed_attribute() {
            for extension in &extensions.extensions {
                let oid = extension
                    .oid
                    .iter()
                    .context("node CSR contains an invalid extension OID")?
                    .collect::<Vec<_>>();
                if oid != [2, 5, 29, 17] {
                    continue;
                }
                anyhow::ensure!(
                    requested_dns_name.is_none(),
                    "node CSR contains duplicate subject-alternative-name extensions"
                );
                let ParsedExtension::SubjectAlternativeName(subject_alt_name) =
                    extension.parsed_extension()
                else {
                    anyhow::bail!(
                        "node CSR contains an invalid subject-alternative-name extension"
                    );
                };
                anyhow::ensure!(
                    subject_alt_name.general_names.len() == 1,
                    "node CSR must contain exactly one DNS subject alternative name"
                );
                let GeneralName::DNSName(dns_name) = &subject_alt_name.general_names[0] else {
                    anyhow::bail!("node CSR subject alternative name must be a DNS name");
                };
                requested_dns_name = Some((*dns_name).to_owned());
            }
        }
    }
    let requested_dns_name = requested_dns_name
        .context("node CSR is missing its required DNS subject alternative name")?;
    anyhow::ensure!(
        !requested_dns_name.contains('*'),
        "node CSR DNS subject alternative name must not contain a wildcard"
    );
    if let Some(expected_dns_name) = expected_dns_name {
        anyhow::ensure!(
            requested_dns_name == expected_dns_name,
            "node CSR DNS subject alternative name does not match the registered node name"
        );
    }
    params.subject_alt_names =
        vec![SanType::DnsName(requested_dns_name.try_into().context(
            "node CSR contains an invalid DNS subject alternative name",
        )?)];
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    let attrs_json = serde_json::to_vec(attrs)
        .context("failed to serialize authoritative node certificate attributes")?;
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &SUBJECT_DIR_ATTRS,
            der_utf8_string(&attrs_json),
        ));

    let public_key = SubjectPublicKeyInfo::from_der(request_info.subject_pki.raw)
        .context("failed to read node CSR public key")?;

    Ok(params
        .signed_by(&public_key, authority.issuer())
        .context("failed to sign node certificate")?
        .pem())
}

fn der_utf8_string(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(value.len() + 6);
    encoded.push(0x0c); // ASN.1 UTF8String
    if value.len() < 128 {
        encoded.push(value.len() as u8);
    } else {
        let length = value.len().to_be_bytes();
        let first_non_zero = length
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(length.len() - 1);
        let length = &length[first_non_zero..];
        encoded.push(0x80 | length.len() as u8);
        encoded.extend_from_slice(length);
    }
    encoded.extend_from_slice(value);
    encoded
}

fn ctrl_cert() -> Result<CertificateRequest> {
    let mut ctrl_params = CertificateParams::new(vec![CTRL_SERVER_NAME.into()])?;
    ctrl_params
        .distinguished_name
        .push(DnType::OrganizationName, "Lunatic Inc.");
    ctrl_params
        .distinguished_name
        .push(DnType::CommonName, "Control CA");
    CertificateRequest::new(ctrl_params)
}

pub fn default_server_certificates(root_cert: &CertificateAuthority) -> Result<(String, String)> {
    let ctrl_cert = ctrl_cert()?;
    let cert_pem = ctrl_cert.serialize_pem_with_signer(root_cert)?;
    let key_pem = ctrl_cert.serialize_private_key_pem();
    Ok((cert_pem, key_pem))
}

#[cfg(test)]
mod tests {
    use rcgen::{
        BasicConstraints, CertificateParams, CustomExtension, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };
    use x509_parser::{parse_x509_certificate, pem::parse_x509_pem};

    use super::{
        default_server_certificates, sign_node_certificate, test_root_cert, CertificateAuthority,
        TEST_ROOT_CERT, TEST_ROOT_KEYS,
    };
    use crate::{CertAttrs, SUBJECT_DIR_ATTRS};

    #[test]
    fn root_authority_signs_default_server_certificate() {
        let root = test_root_cert().expect("test certificate authority should load");
        let (server_certificate_pem, server_private_key_pem) =
            default_server_certificates(&root).expect("server certificate should be generated");

        KeyPair::from_pem(&server_private_key_pem)
            .expect("generated server private key should be valid PEM");

        let (_, root_pem) = parse_x509_pem(root.certificate_pem().as_bytes())
            .expect("root certificate should be valid PEM");
        let (_, root_certificate) = parse_x509_certificate(&root_pem.contents)
            .expect("root certificate should be valid DER");
        let (_, server_pem) = parse_x509_pem(server_certificate_pem.as_bytes())
            .expect("server certificate should be valid PEM");
        let (_, server_certificate) = parse_x509_certificate(&server_pem.contents)
            .expect("server certificate should be valid DER");

        server_certificate
            .verify_signature(Some(&root_certificate.tbs_certificate.subject_pki))
            .expect("server certificate should be signed by the root authority");
    }

    #[test]
    fn malformed_authority_pem_returns_errors() {
        assert!(CertificateAuthority::from_pem("not a certificate", TEST_ROOT_KEYS).is_err());
        assert!(CertificateAuthority::from_pem(TEST_ROOT_CERT, "not a private key").is_err());
    }

    #[test]
    fn generated_authorities_rotate_the_ephemeral_trust_root() {
        let first = CertificateAuthority::generate().unwrap();
        let second = CertificateAuthority::generate().unwrap();

        assert_ne!(first.certificate_pem(), second.certificate_pem());
        assert_ne!(first.private_key_pem(), second.private_key_pem());
    }

    #[test]
    fn node_signing_requires_the_registered_dns_name_and_rejects_wildcards() {
        let root = test_root_cert().unwrap();
        let attrs = CertAttrs {
            node_id: Some(7),
            allowed_envs: Vec::new(),
            is_privileged: true,
        };
        let mismatched_csr = node_csr("different-node.example");
        let mismatch =
            sign_node_certificate(&mismatched_csr, &root, "registered-node.example", &attrs)
                .unwrap_err();
        assert!(mismatch.to_string().contains("registered node name"));

        let wildcard_csr = node_csr("*.example");
        let wildcard =
            sign_node_certificate(&wildcard_csr, &root, "*.example", &attrs).unwrap_err();
        assert!(wildcard.to_string().contains("wildcard"));

        let zero = CertAttrs {
            node_id: Some(0),
            allowed_envs: Vec::new(),
            is_privileged: true,
        };
        let zero_error = sign_node_certificate(
            &node_csr("registered-node.example"),
            &root,
            "registered-node.example",
            &zero,
        )
        .unwrap_err();
        assert!(zero_error.to_string().contains("compact distributed ID"));

        let out_of_range = CertAttrs {
            node_id: Some(crate::distributed::MAX_NODE_ID + 1),
            ..attrs
        };
        let range_error = sign_node_certificate(
            &node_csr("registered-node.example"),
            &root,
            "registered-node.example",
            &out_of_range,
        )
        .unwrap_err();
        assert!(range_error.to_string().contains("compact distributed ID"));
    }

    #[test]
    fn node_signing_replaces_csr_supplied_reserved_attributes() {
        let root = test_root_cert().expect("test certificate authority should load");
        let mut params = CertificateParams::new(vec!["node.example".into()]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::Any];
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                &SUBJECT_DIR_ATTRS,
                super::der_utf8_string(
                    br#"{"node_id":999,"allowed_envs":[],"is_privileged":true}"#,
                ),
            ));
        let key = KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key).unwrap().pem().unwrap();

        let cert_pem = sign_node_certificate(
            &csr,
            &root,
            "node.example",
            &CertAttrs {
                node_id: Some(7),
                allowed_envs: vec![11],
                is_privileged: false,
            },
        )
        .unwrap();
        let (_, pem) = parse_x509_pem(cert_pem.as_bytes()).unwrap();
        let (_, certificate) = parse_x509_certificate(&pem.contents).unwrap();
        let extension = certificate
            .extensions()
            .iter()
            .find(|extension| extension.oid.to_id_string() == "2.5.29.9")
            .expect("authoritative Lunatic attributes");
        let json = decode_der_utf8_string_for_test(extension.value);
        let attrs: CertAttrs = serde_json::from_slice(json).unwrap();

        assert_eq!(attrs.node_id, Some(7));
        assert_eq!(attrs.allowed_envs, vec![11]);
        assert!(!attrs.is_privileged);
        assert!(
            !certificate
                .basic_constraints()
                .unwrap()
                .expect("explicit CA:false constraint")
                .value
                .ca
        );
        let key_usage = certificate
            .key_usage()
            .unwrap()
            .expect("controlled node key usage")
            .value;
        assert!(key_usage.digital_signature());
        assert!(!key_usage.key_cert_sign());
        let extended_key_usage = certificate
            .extended_key_usage()
            .unwrap()
            .expect("controlled node extended key usage")
            .value;
        assert!(extended_key_usage.server_auth);
        assert!(extended_key_usage.client_auth);
        assert!(!extended_key_usage.any);
        let subject_alt_name = certificate
            .subject_alternative_name()
            .unwrap()
            .expect("node certificate must preserve its requested DNS identity");
        assert!(subject_alt_name
            .value
            .general_names
            .iter()
            .any(|name| matches!(
                name,
                x509_parser::extensions::GeneralName::DNSName("node.example")
            )));
        assert_eq!(
            certificate
                .extensions()
                .iter()
                .filter(|extension| extension.oid.to_id_string() == "2.5.29.9")
                .count(),
            1
        );
    }

    fn decode_der_utf8_string_for_test(value: &[u8]) -> &[u8] {
        assert_eq!(value.first(), Some(&0x0c));
        let first_length = value[1];
        if first_length & 0x80 == 0 {
            &value[2..2 + first_length as usize]
        } else {
            let length_bytes = (first_length & 0x7f) as usize;
            let length = value[2..2 + length_bytes]
                .iter()
                .fold(0usize, |length, byte| (length << 8) | *byte as usize);
            &value[2 + length_bytes..2 + length_bytes + length]
        }
    }

    fn node_csr(dns_name: &str) -> String {
        let params = CertificateParams::new(vec![dns_name.to_owned()]).unwrap();
        let key = KeyPair::generate().unwrap();
        params.serialize_request(&key).unwrap().pem().unwrap()
    }
}
