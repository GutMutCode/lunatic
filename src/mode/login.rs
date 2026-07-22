use std::{fmt, time::Duration};

use anyhow::{anyhow, Result};
use clap::Parser;
use log::debug;
use reqwest::{header, StatusCode, Url};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::mode::{
    config::{ConfigManager, Provider},
    credential_store::CredentialStoreError,
};

#[derive(Parser)]
pub struct Args {
    #[clap(short, long)]
    provider: Option<String>,
}

impl fmt::Debug for Args {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Args")
            .field("provider", &self.provider.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Deserialize)]
struct CliLoginResponse {
    login_id: String,
}

impl Drop for CliLoginResponse {
    fn drop(&mut self) {
        self.login_id.zeroize();
    }
}

#[derive(Serialize)]
struct CliLogin {
    app_id: String,
}

static CTRL_URL: &str = "https://lunatic.cloud";
const LOGIN_RETRY_DELAY: Duration = Duration::from_secs(5);

pub(crate) async fn start(args: Args) -> Result<()> {
    let mut config_manager = ConfigManager::new()?;

    if let Some(configured_provider) = config_manager.global_config.provider.as_ref() {
        if let Some(requested_provider) = args.provider.as_deref() {
            if Provider::parse_url(requested_provider)? != configured_provider.get_url()? {
                return Err(anyhow!(
                    "Logout before changing the CLI authentication provider"
                ));
            }
        }
        if is_authenticated(&config_manager).await? {
            println!("\n\nYou are already authenticated.\n\n");
            Ok(())
        } else {
            refresh_existing_login(&mut config_manager).await
        }
    } else {
        let provider = args.provider.unwrap_or_else(|| CTRL_URL.to_string());
        new_login(provider, &mut config_manager).await
    }
}

async fn check_auth_status(
    status_url: Url,
    client: &reqwest::Client,
    retry_delay: Duration,
) -> Result<Vec<String>> {
    loop {
        let response = client
            .get(status_url.clone())
            .send()
            .await
            .map_err(|_| anyhow!("CLI login status request failed"))?;
        if response.status() == StatusCode::OK {
            let mut cookies = Vec::new();
            for value in response.headers().get_all(header::SET_COOKIE) {
                match value.to_str() {
                    Ok(value) => cookies.push(value.to_owned()),
                    Err(_) => {
                        cookies.zeroize();
                        return Err(anyhow!("CLI login returned an invalid credential"));
                    }
                }
            }
            if cookies.is_empty() {
                return Err(anyhow!("CLI login returned no credential"));
            }
            return Ok(cookies);
        }
        if [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN].contains(&response.status()) {
            debug!("CLI authentication is pending; retrying");
            tokio::time::sleep(retry_delay).await;
            continue;
        }
        return Err(anyhow!(
            "CLI login status request failed with status {}",
            response.status()
        ));
    }
}

async fn new_login(provider: String, config_manager: &mut ConfigManager) -> Result<()> {
    let provider_url = Provider::parse_url(&provider)?;
    let client = login_http_client(&provider_url)?;
    let login_url = provider_url
        .join("/api/cli/login")
        .map_err(|_| anyhow!("Invalid provider login URL"))?;
    let response = client
        .post(login_url)
        .json(&CliLogin {
            app_id: config_manager.get_app_id(),
        })
        .send()
        .await
        .map_err(|_| anyhow!("HTTP login request failed"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("HTTP login request failed with status {status}"));
    }

    let mut login = response
        .json::<CliLoginResponse>()
        .await
        .map_err(|_| anyhow!("CLI login returned an invalid response"))?;
    if login.login_id.is_empty() {
        return Err(anyhow!("CLI login returned an invalid response"));
    }

    let authentication_url = authentication_url(
        &provider_url,
        "authenticate",
        &config_manager.get_app_id(),
        &login.login_id,
    )?;
    println!(
        "\n\nPlease visit the following URL to authenticate this cli app {authentication_url}\n\n"
    );

    let status_url = login_status_url(&provider_url, &login.login_id)?;
    let cookies = check_auth_status(status_url, &client, LOGIN_RETRY_DELAY).await?;
    let login_id = std::mem::take(&mut login.login_id);
    config_manager.login(provider, login_id, cookies)
}

async fn is_authenticated(config_manager: &ConfigManager) -> Result<bool> {
    let credential = config_manager.control_credential()?;
    let login_id = Zeroizing::new(credential.login_id().to_owned());
    let provider_url = config_manager
        .global_config
        .provider
        .as_ref()
        .ok_or_else(|| anyhow!("First login by calling `lunatic login`"))?
        .get_url()?;
    let status_url = login_status_url(&provider_url, &login_id)?;
    let response = match config_manager.authenticated_get(status_url).await {
        Ok(response) => response,
        Err(error)
            if error.downcast_ref::<CredentialStoreError>()
                == Some(&CredentialStoreError::Expired) =>
        {
            return Ok(false)
        }
        Err(error) => return Err(error),
    };
    match response.status() {
        StatusCode::OK => Ok(true),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Ok(false),
        status => Err(anyhow!(
            "CLI authentication check failed with status {status}"
        )),
    }
}

async fn refresh_existing_login(config_manager: &mut ConfigManager) -> Result<()> {
    let provider = config_manager
        .global_config
        .provider
        .as_ref()
        .ok_or_else(|| anyhow!("Unexpected missing provider in `lunatic.toml`"))?
        .clone();
    let credential = config_manager.control_credential()?;
    let mut login_id = Zeroizing::new(credential.login_id().to_owned());
    let provider_url = provider.get_url()?;
    let refresh_url = authentication_url(
        &provider_url,
        "refresh",
        &config_manager.global_config.cli_app_id,
        &login_id,
    )?;
    println!("\n\nPlease visit the following URL to authenticate this cli app {refresh_url}\n\n");

    let client = login_http_client(&provider_url)?;
    let status_url = login_status_url(&provider_url, &login_id)?;
    let cookies = check_auth_status(status_url, &client, LOGIN_RETRY_DELAY).await?;
    config_manager.login(provider.name, std::mem::take(&mut *login_id), cookies)
}

fn login_http_client(provider: &Url) -> Result<reqwest::Client> {
    let builder = reqwest::ClientBuilder::new().redirect(reqwest::redirect::Policy::none());
    let builder = if provider.scheme() == "http" {
        builder.no_proxy()
    } else {
        builder
    };
    builder
        .build()
        .map_err(|_| anyhow!("Failed to build CLI login HTTP client"))
}

fn login_status_url(provider: &Url, login_id: &str) -> Result<Url> {
    let mut url = provider
        .join("/api/cli/login/")
        .map_err(|_| anyhow!("Invalid provider login URL"))?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("Invalid provider login URL"))?
        .pop_if_empty()
        .push(login_id);
    Ok(url)
}

fn authentication_url(provider: &Url, action: &str, app_id: &str, login_id: &str) -> Result<Url> {
    let mut url = provider
        .join(&format!("/cli/{action}/"))
        .map_err(|_| anyhow!("Invalid provider authentication URL"))?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("Invalid provider authentication URL"))?
        .pop_if_empty()
        .push(app_id);
    url.query_pairs_mut().append_pair("login_id", login_id);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::mode::credential_store::{ControlCredential, CredentialReference, CredentialStore};

    use super::*;

    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<CredentialReference, Vec<u8>>>);

    impl CredentialStore for MemoryStore {
        fn put(
            &self,
            reference: &CredentialReference,
            credential: &ControlCredential,
        ) -> std::result::Result<(), CredentialStoreError> {
            self.0
                .lock()
                .unwrap()
                .insert(reference.clone(), credential.encode()?.to_vec());
            Ok(())
        }

        fn get(
            &self,
            reference: &CredentialReference,
        ) -> std::result::Result<ControlCredential, CredentialStoreError> {
            let entries = self.0.lock().unwrap();
            ControlCredential::decode(
                entries
                    .get(reference)
                    .ok_or(CredentialStoreError::Missing)?,
            )
        }

        fn delete(
            &self,
            reference: &CredentialReference,
        ) -> std::result::Result<(), CredentialStoreError> {
            self.0
                .lock()
                .unwrap()
                .remove(reference)
                .map(|_| ())
                .ok_or(CredentialStoreError::Missing)
        }
    }

    async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(
                read > 0,
                "connection closed before request headers completed"
            );
            request.extend_from_slice(&chunk[..read]);
            assert!(
                request.len() <= 16 * 1024,
                "request headers exceeded test limit"
            );
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return String::from_utf8_lossy(&request).into_owned();
            }
        }
    }

    #[test]
    fn generated_login_urls_encode_untrusted_identifiers() {
        let provider = Url::parse("https://example.invalid").unwrap();
        let status = login_status_url(&provider, "login/marker?x=1").unwrap();
        assert_eq!(
            status.as_str(),
            "https://example.invalid/api/cli/login/login%2Fmarker%3Fx=1"
        );
        let browser =
            authentication_url(&provider, "authenticate", "app/marker", "login marker").unwrap();
        assert_eq!(
            browser.as_str(),
            "https://example.invalid/cli/authenticate/app%2Fmarker?login_id=login+marker"
        );
    }

    #[test]
    fn login_args_debug_redacts_provider_input() {
        let args = Args {
            provider: Some("https://user:password-marker@example.invalid".to_owned()),
        };
        let debug = format!("{args:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("password-marker"));
    }

    #[tokio::test]
    async fn existing_login_reuses_protected_cookie_for_status_check() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut socket).await.to_lowercase();
            assert!(request.starts_with("get /api/cli/login/login-marker "));
            assert!(request.contains("cookie: session=cookie-marker\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let store: Arc<dyn CredentialStore> = Arc::new(MemoryStore::default());
        let mut manager = ConfigManager::new_for_test(path, store).unwrap();
        manager
            .login(
                format!("http://{address}"),
                "login-marker".to_owned(),
                vec!["session=cookie-marker; HttpOnly".to_owned()],
            )
            .unwrap();

        assert!(is_authenticated(&manager).await.unwrap());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn expired_login_is_not_sent_during_status_check() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let store: Arc<dyn CredentialStore> = Arc::new(MemoryStore::default());
        let mut manager = ConfigManager::new_for_test(path, store).unwrap();
        manager
            .login(
                format!("http://{address}"),
                "login-marker".to_owned(),
                vec!["session=cookie-marker; Expires=Thu, 01 Jan 1970 00:00:00 GMT".to_owned()],
            )
            .unwrap();

        assert!(!is_authenticated(&manager).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn new_login_moves_cookie_from_http_response_to_store_only() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut login_socket, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut login_socket).await;
            assert!(request.starts_with("POST /api/cli/login "));
            let body = br#"{"login_id":"login-marker"}"#;
            login_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        String::from_utf8_lossy(body)
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            drop(login_socket);

            let (mut status_socket, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut status_socket).await;
            assert!(request.starts_with("GET /api/cli/login/login-marker "));
            status_socket
                .write_all(b"HTTP/1.1 200 OK\r\nSet-Cookie: session=cookie-marker; HttpOnly; Max-Age=60\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let store = Arc::new(MemoryStore::default());
        let store_trait: Arc<dyn CredentialStore> = store.clone();
        let mut manager = ConfigManager::new_for_test(path.clone(), store_trait).unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            new_login(format!("http://{address}"), &mut manager),
        )
        .await
        .expect("new login test timed out")
        .unwrap();
        server.await.unwrap();

        let file = std::fs::read_to_string(path).unwrap();
        assert!(!file.contains("login-marker"));
        assert!(!file.contains("cookie-marker"));
        assert!(file.contains("credential_ref"));
        let credential = manager.control_credential().unwrap();
        assert_eq!(credential.login_id(), "login-marker");
        assert_eq!(
            credential.cookie_header_at(u64::MAX).unwrap_err(),
            CredentialStoreError::Expired
        );
    }

    #[tokio::test]
    async fn login_status_error_does_not_echo_response_cookie() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _request = read_request_headers(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nSet-Cookie: session=cookie-marker\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let url = Url::parse(&format!("http://{address}/status/login-marker")).unwrap();
        let error = check_auth_status(url, &reqwest::Client::new(), Duration::ZERO)
            .await
            .unwrap_err();
        server.await.unwrap();
        assert_eq!(
            error.to_string(),
            "CLI login status request failed with status 500 Internal Server Error"
        );
        assert!(!format!("{error:#}").contains("cookie-marker"));
        assert!(!format!("{error:#}").contains("login-marker"));
    }

    #[tokio::test]
    async fn login_status_does_not_follow_cross_origin_redirects() {
        let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider_listener.local_addr().unwrap();
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_address = redirect_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = provider_listener.accept().await.unwrap();
            let _request = read_request_headers(&mut socket).await;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{redirect_address}/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider = Url::parse(&format!("http://{provider_address}/")).unwrap();
        let status_url = provider.join("status/login-marker").unwrap();
        let client = login_http_client(&provider).unwrap();

        let error = check_auth_status(status_url, &client, Duration::ZERO)
            .await
            .unwrap_err();
        server.await.unwrap();
        assert_eq!(
            error.to_string(),
            "CLI login status request failed with status 302 Found"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), redirect_listener.accept())
                .await
                .is_err()
        );
    }
}
