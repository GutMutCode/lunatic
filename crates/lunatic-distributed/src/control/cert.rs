use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{Certificate, CertificateParams, DnType, Issuer, KeyPair};

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
    use rcgen::KeyPair;
    use x509_parser::{parse_x509_certificate, pem::parse_x509_pem};

    use super::{
        default_server_certificates, test_root_cert, CertificateAuthority, TEST_ROOT_CERT,
        TEST_ROOT_KEYS,
    };

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
}
