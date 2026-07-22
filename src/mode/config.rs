use std::{
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[cfg(windows)]
use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};

#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, MoveFileExW, SetFileAttributesW, BY_HANDLE_FILE_INFORMATION,
    FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_TEMPORARY, MOVEFILE_REPLACE_EXISTING,
    MOVEFILE_WRITE_THROUGH,
};

use anyhow::{anyhow, Context};
use fs2::FileExt;
use log::debug;
use reqwest::{
    header::{self, HeaderMap},
    multipart::{self, Form},
    Method, StatusCode, Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use super::credential_store::{
    unix_now, ControlCredential, CredentialReference, CredentialStore, CredentialStoreError,
    OsCredentialStore,
};

static VERSION: &str = "0.2.0";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalLunaticConfig {
    /// unique id for every installation of the cli tool
    /// used to identify "apps" during /cli/login calls
    pub cli_app_id: String,
    pub version: String,
    pub provider: Option<Provider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_credential_deletion: Option<CredentialReference>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProjectLunaticConfig {
    pub project_id: i64,
    pub project_name: String,
    pub domains: Vec<String>,
    pub app_id: i64,
    pub env_id: i64,
    pub env_vars: Option<String>,
    pub assets_dir: Option<String>,
}

#[derive(Debug)]
pub enum ConfigError {
    FileMissing(&'static str),
    TomlEncodingFailed,
    TomlDecodingFailed,
    FileWriteFailed,
    FileDurabilityUncertain,
    FileReadFailed,
    CredentialStore(CredentialStoreError),
    CredentialRollbackFailed,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FileMissing(message) => message,
            Self::TomlEncodingFailed => "Failed to encode Lunatic configuration",
            Self::TomlDecodingFailed => "Failed to decode Lunatic configuration",
            Self::FileWriteFailed => "Failed to write Lunatic configuration",
            Self::FileDurabilityUncertain => {
                "Lunatic configuration was replaced but directory durability is uncertain"
            }
            Self::FileReadFailed => "Failed to read Lunatic configuration",
            Self::CredentialStore(error) => return error.fmt(formatter),
            Self::CredentialRollbackFailed => {
                "Failed to roll back a CLI credential-store operation"
            }
        })
    }
}

impl std::error::Error for ConfigError {}

impl From<CredentialStoreError> for ConfigError {
    fn from(error: CredentialStoreError) -> Self {
        Self::CredentialStore(error)
    }
}

#[derive(Deserialize, Serialize)]
pub struct ApiError {
    pub code: String,
}

impl FileBased for ProjectLunaticConfig {
    fn get_file_path() -> Result<PathBuf, ConfigError> {
        let current_dir = match std::env::current_dir() {
            Ok(dir) => dir,
            Err(_) => return Err(ConfigError::FileMissing("Failed to find lunatic.toml in working directory and parent directories. Are you sure you're in the correct directory?")),
        };
        Ok(current_dir.join("lunatic.toml"))
    }
}

impl FileBased for GlobalLunaticConfig {
    fn get_file_path() -> Result<PathBuf, ConfigError> {
        let home_path = dirs::home_dir().ok_or(ConfigError::FileMissing(
            "Failed to resolve the user home directory",
        ))?;
        ensure_global_config_directory(&home_path)
    }
}

fn ensure_global_config_directory(home_path: &Path) -> Result<PathBuf, ConfigError> {
    let lunatic_path = home_path.join(".lunatic");
    let created = match fs::create_dir(&lunatic_path) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(_) => return Err(ConfigError::FileWriteFailed),
    };
    #[cfg(not(unix))]
    let _ = created;
    let metadata = fs::symlink_metadata(&lunatic_path).map_err(|_| ConfigError::FileWriteFailed)?;
    if !metadata.file_type().is_dir() {
        return Err(ConfigError::FileWriteFailed);
    }
    #[cfg(unix)]
    fs::set_permissions(&lunatic_path, fs::Permissions::from_mode(0o700))
        .map_err(|_| ConfigError::FileWriteFailed)?;
    #[cfg(unix)]
    if created {
        fs::File::open(home_path)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ConfigError::FileDurabilityUncertain)?;
    }
    Ok(lunatic_path.join("lunatic.toml"))
}

fn existing_global_config_is_safe(path: &Path) -> Result<bool, ConfigError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(ConfigError::FileReadFailed),
    };
    if !metadata.file_type().is_file() {
        return Err(ConfigError::FileReadFailed);
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        return Err(ConfigError::FileReadFailed);
    }
    #[cfg(windows)]
    {
        let file = fs::File::open(path).map_err(|_| ConfigError::FileReadFailed)?;
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        let succeeded =
            unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) };
        if succeeded == 0 || information.nNumberOfLinks != 1 {
            return Err(ConfigError::FileReadFailed);
        }
    }
    Ok(true)
}

#[cfg(windows)]
fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains NUL",
        ));
    }
    value.push(0);
    Ok(value)
}

#[cfg(windows)]
fn persist_private_tempfile(mut temporary: tempfile::NamedTempFile, path: &Path) -> io::Result<()> {
    let source = wide_path(temporary.path())?;
    let destination = wide_path(path)?;
    unsafe {
        if SetFileAttributesW(source.as_ptr(), FILE_ATTRIBUTE_NORMAL) == 0 {
            return Err(io::Error::last_os_error());
        }
        if MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        ) == 0
        {
            let error = io::Error::last_os_error();
            let _ = SetFileAttributesW(source.as_ptr(), FILE_ATTRIBUTE_TEMPORARY);
            return Err(error);
        }
    }
    temporary.disable_cleanup(true);
    Ok(())
}

fn open_private_config(path: &PathBuf, truncate: bool) -> io::Result<fs::File> {
    let mut options = fs::File::options();
    options
        .create(true)
        .read(true)
        .write(true)
        .truncate(truncate);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

struct ConfigTransactionLock {
    _file: fs::File,
}

impl ConfigTransactionLock {
    fn acquire(config_path: &Path) -> Result<Self, ConfigError> {
        let lock_path = config_path.with_extension("lock");
        let mut options = fs::File::options();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(lock_path)
            .map_err(|_| ConfigError::FileWriteFailed)?;
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| ConfigError::FileWriteFailed)?;
        file.lock_exclusive()
            .map_err(|_| ConfigError::FileWriteFailed)?;
        Ok(Self { _file: file })
    }
}

fn write_private_toml<T: Serialize>(path: &Path, value: &T) -> Result<(), ConfigError> {
    let parent = path.parent().ok_or(ConfigError::FileWriteFailed)?;
    let encoded = Zeroizing::new(toml::to_vec(value).map_err(|_| ConfigError::TomlEncodingFailed)?);
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|_| ConfigError::FileWriteFailed)?;
    #[cfg(unix)]
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| ConfigError::FileWriteFailed)?;
    temporary
        .write_all(&encoded)
        .and_then(|_| temporary.flush())
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|_| ConfigError::FileWriteFailed)?;
    #[cfg(windows)]
    persist_private_tempfile(temporary, path).map_err(|_| ConfigError::FileWriteFailed)?;
    #[cfg(not(windows))]
    temporary
        .persist(path)
        .map_err(|_| ConfigError::FileWriteFailed)?;
    #[cfg(unix)]
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ConfigError::FileDurabilityUncertain)?;
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    pub name: String,
    credential_ref: CredentialReference,
}

impl fmt::Debug for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Provider")
            .field("name", &"[REDACTED]")
            .field("credential_ref", &"[REDACTED]")
            .finish()
    }
}

impl Provider {
    fn new(name: String, credential_ref: CredentialReference) -> anyhow::Result<Self> {
        let provider = Self {
            name,
            credential_ref,
        };
        provider.get_url()?;
        Ok(provider)
    }

    pub fn get_url(&self) -> anyhow::Result<Url> {
        Self::parse_url(&self.name)
    }

    pub(super) fn parse_url(value: &str) -> anyhow::Result<Url> {
        let url = Url::from_str(value).map_err(|_| anyhow!("Invalid provider URL"))?;
        let loopback = match url.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        };
        if (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(anyhow!(
                "Provider URL must be an HTTPS origin (HTTP is allowed only for loopback) with no user info, path, query, or fragment"
            ));
        }
        Ok(url)
    }
}

fn ensure_same_origin(provider: &Url, target: &Url) -> anyhow::Result<()> {
    if provider.scheme() == target.scheme()
        && provider.host_str() == target.host_str()
        && provider.port_or_known_default() == target.port_or_known_default()
        && target.username().is_empty()
        && target.password().is_none()
    {
        Ok(())
    } else {
        Err(anyhow!(
            "Platform request URL must remain on the configured provider origin"
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyGlobalLunaticConfig {
    cli_app_id: String,
    #[serde(rename = "version")]
    _version: String,
    provider: Option<LegacyProvider>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyProvider {
    login_id: String,
    name: String,
    cookies: Vec<String>,
}

impl Drop for LegacyProvider {
    fn drop(&mut self) {
        self.login_id.zeroize();
        self.name.zeroize();
        self.cookies.zeroize();
    }
}

impl Default for GlobalLunaticConfig {
    fn default() -> Self {
        Self {
            version: VERSION.to_string(),
            provider: None,
            pending_credential_deletion: None,
            cli_app_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

pub(crate) trait FileBased
where
    Self: Serialize + DeserializeOwned + Default,
{
    fn get_file_path() -> Result<PathBuf, ConfigError>;

    fn from_toml_file(path: PathBuf) -> Self {
        match open_private_config(&path, false) {
            Ok(mut file) => {
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .expect("failed to read lunatic.toml");
                let toml_str =
                    String::from_utf8(buf).expect("failed to read string from lunatic.toml");
                let loaded_toml: Self = toml::from_str(&toml_str)
                    .or_else(|_e| Ok::<Self, toml::de::Error>(Self::default()))
                    .unwrap();
                loaded_toml
            }
            Err(_e) => {
                let mut file =
                    open_private_config(&path, true).expect("failed to create new lunatic.toml");
                let initial_state = Self::default();
                let encoded = toml::to_vec(&initial_state).expect("Failed to encode toml");
                file.write_all(&encoded)
                    .expect("Failed to write toml to file");
                initial_state
            }
        }
    }

    fn flush_file(&mut self) -> Result<(), ConfigError> {
        let file_path = Self::get_file_path()?;
        write_private_toml(&file_path, self)
    }
}

fn load_global_config(
    path: &Path,
    credential_store: &dyn CredentialStore,
) -> Result<GlobalLunaticConfig, ConfigError> {
    load_global_config_with_writer(path, credential_store, &|path, config| {
        write_private_toml(path, config)
    })
}

fn validate_current_global_config(config: &GlobalLunaticConfig) -> Result<(), ConfigError> {
    if let Some(provider) = &config.provider {
        provider
            .get_url()
            .map_err(|_| ConfigError::TomlDecodingFailed)?;
        provider
            .credential_ref
            .validate()
            .map_err(|_| ConfigError::TomlDecodingFailed)?;
    }
    if let Some(reference) = &config.pending_credential_deletion {
        reference
            .validate()
            .map_err(|_| ConfigError::TomlDecodingFailed)?;
        if config.provider.is_some() {
            return Err(ConfigError::TomlDecodingFailed);
        }
    }
    Ok(())
}

fn load_global_config_with_writer(
    path: &Path,
    credential_store: &dyn CredentialStore,
    write_config: &dyn Fn(&Path, &GlobalLunaticConfig) -> Result<(), ConfigError>,
) -> Result<GlobalLunaticConfig, ConfigError> {
    if !existing_global_config_is_safe(path)? {
        let initial = GlobalLunaticConfig::default();
        write_config(path, &initial)?;
        return Ok(initial);
    }

    let mut file =
        open_private_config(&path.to_path_buf(), false).map_err(|_| ConfigError::FileReadFailed)?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.read_to_end(&mut bytes)
        .map_err(|_| ConfigError::FileReadFailed)?;
    drop(file);
    let source = std::str::from_utf8(&bytes).map_err(|_| ConfigError::TomlDecodingFailed)?;

    if let Ok(mut current) = toml::from_str::<GlobalLunaticConfig>(source) {
        validate_current_global_config(&current)?;
        current.version = VERSION.to_owned();
        return Ok(current);
    }

    let mut legacy = toml::from_str::<LegacyGlobalLunaticConfig>(source)
        .map_err(|_| ConfigError::TomlDecodingFailed)?;
    let Some(mut legacy_provider) = legacy.provider.take() else {
        let current = GlobalLunaticConfig {
            cli_app_id: legacy.cli_app_id,
            version: VERSION.to_owned(),
            provider: None,
            pending_credential_deletion: None,
        };
        write_config(path, &current)?;
        return Ok(current);
    };

    let provider_name = std::mem::take(&mut legacy_provider.name);
    Provider::parse_url(&provider_name).map_err(|_| ConfigError::TomlDecodingFailed)?;
    let reference = CredentialReference::for_legacy_migration(&legacy.cli_app_id, &provider_name);
    let provider = Provider::new(provider_name, reference.clone())
        .map_err(|_| ConfigError::TomlDecodingFailed)?;
    let provider_url = provider
        .get_url()
        .map_err(|_| ConfigError::TomlDecodingFailed)?;
    let legacy_login_id = Zeroizing::new(std::mem::take(&mut legacy_provider.login_id));
    let query = Zeroizing::new(format!("login_id={}", legacy_login_id.as_str()));
    let decoded_login_id = url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "login_id")
        .map(|(_, value)| value.into_owned())
        .ok_or(ConfigError::TomlDecodingFailed)?;
    let credential = ControlCredential::from_set_cookie_headers(
        &provider_url,
        &legacy.cli_app_id,
        decoded_login_id,
        std::mem::take(&mut legacy_provider.cookies),
        unix_now(),
    )?;
    if let Err(store_error) = credential_store.put(&reference, &credential) {
        if store_error != CredentialStoreError::Failed {
            return Err(ConfigError::CredentialStore(store_error));
        }
        rollback_credential(credential_store, &reference, None)?;
        return Err(ConfigError::CredentialStore(store_error));
    }
    if verify_credential(credential_store, &reference, &credential).is_err() {
        rollback_credential(credential_store, &reference, None)?;
        return Err(ConfigError::CredentialStore(CredentialStoreError::Failed));
    }

    let current = GlobalLunaticConfig {
        cli_app_id: legacy.cli_app_id,
        version: VERSION.to_owned(),
        provider: Some(provider),
        pending_credential_deletion: None,
    };
    if let Err(write_error) = write_config(path, &current) {
        if matches!(write_error, ConfigError::FileDurabilityUncertain) {
            return Err(write_error);
        }
        rollback_credential(credential_store, &reference, None)?;
        return Err(write_error);
    }
    Ok(current)
}

fn remove_local_credential<F>(path: &Path, credential_store: F) -> Result<bool, ConfigError>
where
    F: FnOnce() -> Result<Arc<dyn CredentialStore>, ConfigError>,
{
    remove_local_credential_with_writer(path, credential_store, &write_private_toml)
}

fn remove_local_credential_with_writer<F>(
    path: &Path,
    credential_store: F,
    write_config: &dyn Fn(&Path, &GlobalLunaticConfig) -> Result<(), ConfigError>,
) -> Result<bool, ConfigError>
where
    F: FnOnce() -> Result<Arc<dyn CredentialStore>, ConfigError>,
{
    let _transaction = ConfigTransactionLock::acquire(path)?;
    if !existing_global_config_is_safe(path)? {
        return Ok(false);
    }

    let mut file =
        open_private_config(&path.to_path_buf(), false).map_err(|_| ConfigError::FileReadFailed)?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.read_to_end(&mut bytes)
        .map_err(|_| ConfigError::FileReadFailed)?;
    drop(file);
    let source = std::str::from_utf8(&bytes).map_err(|_| ConfigError::TomlDecodingFailed)?;

    if let Ok(mut current) = toml::from_str::<GlobalLunaticConfig>(source) {
        if let Some(reference) = &current.pending_credential_deletion {
            reference
                .validate()
                .map_err(|_| ConfigError::TomlDecodingFailed)?;
            if current.provider.is_some() {
                return Err(ConfigError::TomlDecodingFailed);
            }
        }
        current.version = VERSION.to_owned();
        let mut changed = false;
        let mut factory = Some(credential_store);
        let mut store: Option<Arc<dyn CredentialStore>> = None;

        if let Some(reference) = current.pending_credential_deletion.clone() {
            store = Some(factory.take().expect("credential-store factory used once")()?);
            delete_credential_verified(store.as_ref().unwrap().as_ref(), &reference)?;
            current.pending_credential_deletion = None;
            write_config(path, &current)?;
            changed = true;
        }

        let Some(provider) = current.provider.take() else {
            return Ok(changed);
        };
        changed = true;
        if provider.credential_ref.validate().is_err() {
            write_config(path, &current)?;
            return Ok(changed);
        }

        current.pending_credential_deletion = Some(provider.credential_ref.clone());
        write_config(path, &current)?;
        if store.is_none() {
            store = Some(factory.take().expect("credential-store factory used once")()?);
        }
        delete_credential_verified(store.as_ref().unwrap().as_ref(), &provider.credential_ref)?;
        current.pending_credential_deletion = None;
        write_config(path, &current)?;
        return Ok(changed);
    }

    let mut legacy = toml::from_str::<LegacyGlobalLunaticConfig>(source)
        .map_err(|_| ConfigError::TomlDecodingFailed)?;
    let Some(legacy_provider) = legacy.provider.take() else {
        return Ok(false);
    };
    let reference =
        CredentialReference::for_legacy_migration(&legacy.cli_app_id, &legacy_provider.name);
    let mut current = GlobalLunaticConfig {
        cli_app_id: legacy.cli_app_id,
        version: VERSION.to_owned(),
        provider: None,
        pending_credential_deletion: Some(reference.clone()),
    };
    // Commit plaintext removal before touching the protected copy. If the store is
    // unavailable, the opaque tombstone makes the deletion safely retryable.
    write_config(path, &current)?;
    let store = credential_store()?;
    delete_credential_verified(store.as_ref(), &reference)?;
    current.pending_credential_deletion = None;
    write_config(path, &current)?;
    Ok(true)
}

fn verify_credential(
    credential_store: &dyn CredentialStore,
    reference: &CredentialReference,
    expected: &ControlCredential,
) -> Result<(), CredentialStoreError> {
    let actual = credential_store.get(reference)?;
    let expected = expected.encode()?;
    let actual = actual.encode()?;
    if expected.as_slice() == actual.as_slice() {
        Ok(())
    } else {
        Err(CredentialStoreError::Failed)
    }
}

fn delete_credential_verified(
    credential_store: &dyn CredentialStore,
    reference: &CredentialReference,
) -> Result<(), CredentialStoreError> {
    match credential_store.delete(reference) {
        Ok(()) | Err(CredentialStoreError::Missing) => {}
        Err(error) => return Err(error),
    }
    match credential_store.get(reference) {
        Err(CredentialStoreError::Missing) => Ok(()),
        Err(CredentialStoreError::Unavailable) => Err(CredentialStoreError::Unavailable),
        Ok(_) | Err(_) => Err(CredentialStoreError::Failed),
    }
}

fn rollback_credential(
    credential_store: &dyn CredentialStore,
    reference: &CredentialReference,
    previous: Option<&ControlCredential>,
) -> Result<(), ConfigError> {
    let rollback = match previous {
        Some(previous) => credential_store
            .put(reference, previous)
            .and_then(|_| verify_credential(credential_store, reference, previous)),
        None => delete_credential_verified(credential_store, reference),
    };
    rollback.map_err(|_| ConfigError::CredentialRollbackFailed)
}

fn clear_staged_login_tombstone(
    config: &mut GlobalLunaticConfig,
    path: &Path,
    reference: &CredentialReference,
    write_config: &dyn Fn(&Path, &GlobalLunaticConfig) -> Result<(), ConfigError>,
) {
    config.pending_credential_deletion = None;
    if let Err(error) = write_config(path, config) {
        // A pre-replace failure leaves the safe tombstone on disk. A durability
        // error means replacement completed, so retain the cleared in-memory state.
        if !matches!(error, ConfigError::FileDurabilityUncertain) {
            config.pending_credential_deletion = Some(reference.clone());
        }
    }
}

pub struct ConfigManager {
    pub global_config: GlobalLunaticConfig,
    // mapping of local project to platform Project/Apps
    pub project_config: Option<ProjectLunaticConfig>,
    credential_store: Arc<dyn CredentialStore>,
    global_config_path: PathBuf,
}

impl fmt::Debug for ConfigManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigManager")
            .field("global_config", &self.global_config)
            .field("project_config", &self.project_config)
            .field("credential_store", &"[REDACTED]")
            .field("global_config_path", &"[REDACTED]")
            .finish()
    }
}

impl ConfigManager {
    pub(super) fn logout_local() -> anyhow::Result<bool> {
        let path = GlobalLunaticConfig::get_file_path()?;
        remove_local_credential(&path, || {
            Ok(Arc::new(OsCredentialStore::new()) as Arc<dyn CredentialStore>)
        })
        .map_err(anyhow::Error::new)
    }

    pub fn new() -> Result<ConfigManager, ConfigError> {
        let credential_store: Arc<dyn CredentialStore> = Arc::new(OsCredentialStore::new());
        let global_config_path = GlobalLunaticConfig::get_file_path()?;
        let _transaction = ConfigTransactionLock::acquire(&global_config_path)?;
        let global_config = load_global_config(&global_config_path, credential_store.as_ref())?;
        let project_config = ConfigManager::get_project_config().ok();

        Ok(ConfigManager {
            global_config,
            project_config,
            credential_store,
            global_config_path,
        })
    }

    fn get_http_client(&self) -> anyhow::Result<(reqwest::Client, Provider)> {
        self.get_http_client_at(unix_now())
    }

    fn get_http_client_at(
        &self,
        now_unix_seconds: u64,
    ) -> anyhow::Result<(reqwest::Client, Provider)> {
        match self.global_config.provider.clone() {
            Some(provider) => {
                let provider_url = provider.get_url()?;
                let credential = self
                    .credential_store
                    .get(&provider.credential_ref)
                    .map_err(anyhow::Error::new)?;
                credential
                    .validate_scope(&provider_url, &self.global_config.cli_app_id)
                    .map_err(anyhow::Error::new)?;
                let cookie_header = Zeroizing::new(
                    credential
                        .cookie_header_at(now_unix_seconds)
                        .map_err(anyhow::Error::new)?,
                );
                let mut headers = HeaderMap::new();
                let mut cookie_value = header::HeaderValue::from_str(&cookie_header)
                    .map_err(|_| anyhow!("CLI credential is invalid"))?;
                cookie_value.set_sensitive(true);
                headers.insert(header::COOKIE, cookie_value);
                headers.insert(
                    header::HeaderName::from_static("lunatic-cli-version"),
                    header::HeaderValue::from_str(&self.global_config.version)?,
                );
                let client_builder = reqwest::ClientBuilder::new()
                    .cookie_store(true)
                    .default_headers(headers)
                    .redirect(reqwest::redirect::Policy::none());
                let client_builder = if provider_url.scheme() == "http" {
                    client_builder.no_proxy()
                } else {
                    client_builder
                };
                let client = client_builder
                    .build()
                    .map_err(|_| anyhow!("Failed to build platform HTTP client"))?;
                Ok((client, provider))
            }
            None => Err(anyhow!("First login by calling `lunatic login`")),
        }
    }

    pub(super) async fn authenticated_get(&self, target: Url) -> anyhow::Result<reqwest::Response> {
        let (client, provider) = self.get_http_client()?;
        let provider_url = provider.get_url()?;
        ensure_same_origin(&provider_url, &target)?;
        client
            .get(target)
            .send()
            .await
            .map_err(|_| anyhow!("CLI authentication check failed"))
    }

    // quality of life function that makes all calls to platform
    pub async fn request_platform<T: DeserializeOwned, I: Serialize>(
        &self,
        method: Method,
        path: &str,
        description: &str,
        body: Option<I>,
        form_body: Option<Form>,
    ) -> anyhow::Result<(StatusCode, T)> {
        let (client, provider) = self.get_http_client()?;
        let full_url = provider
            .get_url()?
            .join(path)
            .map_err(|e| anyhow!("Failed to join url {e:?}"))?;
        ensure_same_origin(&provider.get_url()?, &full_url)?;

        let mut builder = client.request(method, full_url.clone());

        builder = if let Some(b) = &body {
            builder.json(&b)
        } else if let Some(form) = form_body {
            builder.multipart(form)
        } else {
            builder
        };

        let response = builder
            .send()
            .await
            .with_context(|| format!("Error sending HTTP {} request.", description))?;

        debug!(
            "Response from '{description}' completed with status {}",
            response.status()
        );

        let status = response.status();
        if !status.is_success() {
            if status == StatusCode::UNAUTHORIZED {
                println!("\n\nYou are not authenticated. Please login again via `lunatic login` command.\n\n");
            }
            let body = response.json::<ApiError>().await.ok();

            if body.as_ref().map(|body| body.code.as_str()) == Some("lunatic_cli_update_required") {
                println!("\n\nLunatic version missmatch. Install the latest `lunatic-runtime` version, e.g. `cargo install lunatic-runtime`.\n\n")
            }
            Err(anyhow!(
                "HTTP {description} request failed with status {status}"
            ))
        } else {
            Ok((
                response.status(),
                response
                    .json()
                    .await
                    .with_context(|| format!("Error parsing the {description} request JSON."))?,
            ))
        }
    }

    pub fn init_project(&mut self, project_config: ProjectLunaticConfig) {
        self.project_config = Some(project_config);
    }

    pub async fn upload_artefact_for_app(
        &mut self,
        app_id: &i64,
        artefact: Vec<u8>,
        filename: String,
    ) -> anyhow::Result<i64> {
        let form = multipart::Form::new().part(
            "file",
            multipart::Part::stream(artefact).file_name(filename),
        );
        let (status, new_version) = self
            .request_platform::<serde_json::Value, ()>(
                Method::POST,
                &format!("/api/apps/{app_id}/versions"),
                "upload wasm",
                None,
                Some(form),
            )
            .await?;

        if let (Some(serde_json::Value::Number(version_id)), true) =
            (new_version.get("app_version_id"), status.is_success())
        {
            version_id
                .as_i64()
                .ok_or_else(|| anyhow!("Platform returned an invalid app version id"))
        } else {
            Err(anyhow!(
                "Platform response did not include an app version id"
            ))
        }
    }

    fn get_project_config() -> Result<ProjectLunaticConfig, ConfigError> {
        let project_config_path = ProjectLunaticConfig::get_file_path()?;
        if project_config_path.exists() && project_config_path.is_file() {
            Ok(ProjectLunaticConfig::from_toml_file(project_config_path))
        } else {
            Err(ConfigError::FileMissing(
                "Project config missing `lunatic.toml`",
            ))
        }
    }

    pub(super) fn control_credential(&self) -> anyhow::Result<ControlCredential> {
        let provider = self
            .global_config
            .provider
            .as_ref()
            .ok_or_else(|| anyhow!("First login by calling `lunatic login`"))?;
        let provider_url = provider.get_url()?;
        let credential = self
            .credential_store
            .get(&provider.credential_ref)
            .map_err(anyhow::Error::new)?;
        credential
            .validate_scope(&provider_url, &self.global_config.cli_app_id)
            .map_err(anyhow::Error::new)?;
        Ok(credential)
    }

    pub(super) fn login(
        &mut self,
        provider_name: String,
        login_id: String,
        cookie_headers: Vec<String>,
    ) -> anyhow::Result<()> {
        self.login_at(provider_name, login_id, cookie_headers, unix_now())
    }

    fn login_at(
        &mut self,
        provider_name: String,
        login_id: String,
        cookie_headers: Vec<String>,
        now_unix_seconds: u64,
    ) -> anyhow::Result<()> {
        self.login_at_with_writer(
            provider_name,
            login_id,
            cookie_headers,
            now_unix_seconds,
            write_private_toml,
        )
    }

    fn login_at_with_writer<F>(
        &mut self,
        provider_name: String,
        login_id: String,
        cookie_headers: Vec<String>,
        now_unix_seconds: u64,
        write_config: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&Path, &GlobalLunaticConfig) -> Result<(), ConfigError>,
    {
        let _transaction = ConfigTransactionLock::acquire(&self.global_config_path)?;
        self.global_config =
            load_global_config(&self.global_config_path, self.credential_store.as_ref())?;
        if self.global_config.pending_credential_deletion.is_some() {
            return Err(anyhow!(
                "Complete the pending CLI credential deletion before logging in"
            ));
        }
        let previous_provider = self.global_config.provider.clone();
        let requested_url = Provider::parse_url(&provider_name)?;
        if let Some(previous) = &previous_provider {
            if previous.get_url()? != requested_url {
                return Err(anyhow!(
                    "Logout before changing the CLI authentication provider"
                ));
            }
        }
        let reference = previous_provider
            .as_ref()
            .map(|provider| provider.credential_ref.clone())
            .unwrap_or_else(CredentialReference::new);
        let previous_credential = match &previous_provider {
            Some(previous) => {
                let credential = self
                    .credential_store
                    .get(&reference)
                    .map_err(anyhow::Error::new)?;
                credential
                    .validate_scope(&previous.get_url()?, &self.global_config.cli_app_id)
                    .map_err(anyhow::Error::new)?;
                Some(credential)
            }
            None => None,
        };
        let credential = ControlCredential::from_set_cookie_headers(
            &requested_url,
            &self.global_config.cli_app_id,
            login_id,
            cookie_headers,
            now_unix_seconds,
        )
        .map_err(anyhow::Error::new)?;
        let first_login_provider = if previous_provider.is_none() {
            Some(Provider::new(provider_name, reference.clone())?)
        } else {
            None
        };

        if first_login_provider.is_some() {
            self.global_config.pending_credential_deletion = Some(reference.clone());
            if let Err(write_error) = write_config(&self.global_config_path, &self.global_config) {
                if !matches!(write_error, ConfigError::FileDurabilityUncertain) {
                    self.global_config.pending_credential_deletion = None;
                }
                return Err(write_error.into());
            }
        }

        if let Err(store_error) = self.credential_store.put(&reference, &credential) {
            if store_error == CredentialStoreError::Failed {
                rollback_credential(
                    self.credential_store.as_ref(),
                    &reference,
                    previous_credential.as_ref(),
                )?;
            }
            if previous_provider.is_none() {
                clear_staged_login_tombstone(
                    &mut self.global_config,
                    &self.global_config_path,
                    &reference,
                    &write_config,
                );
            }
            return Err(store_error.into());
        }
        if let Err(verification_error) =
            verify_credential(self.credential_store.as_ref(), &reference, &credential)
        {
            rollback_credential(
                self.credential_store.as_ref(),
                &reference,
                previous_credential.as_ref(),
            )?;
            if previous_provider.is_none() {
                clear_staged_login_tombstone(
                    &mut self.global_config,
                    &self.global_config_path,
                    &reference,
                    &write_config,
                );
            }
            return Err(verification_error.into());
        }

        if previous_provider.is_some() {
            return Ok(());
        }

        self.global_config.pending_credential_deletion = None;
        self.global_config.provider = first_login_provider;

        if let Err(write_error) = write_config(&self.global_config_path, &self.global_config) {
            if matches!(write_error, ConfigError::FileDurabilityUncertain) {
                return Err(write_error.into());
            }
            self.global_config.provider = None;
            self.global_config.pending_credential_deletion = Some(reference);
            return Err(write_error.into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn logout(&mut self) -> anyhow::Result<bool> {
        self.logout_with_writer(write_private_toml)
    }

    #[cfg(test)]
    fn logout_with_writer<F>(&mut self, write_config: F) -> anyhow::Result<bool>
    where
        F: Fn(&Path, &GlobalLunaticConfig) -> Result<(), ConfigError>,
    {
        let store = self.credential_store.clone();
        let changed = remove_local_credential_with_writer(
            &self.global_config_path,
            move || Ok(store),
            &write_config,
        )?;
        if changed {
            self.global_config.provider = None;
            self.global_config.pending_credential_deletion = None;
            self.global_config.version = VERSION.to_owned();
        }
        Ok(changed)
    }

    pub fn get_app_id(&self) -> String {
        self.global_config.cli_app_id.clone()
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        match self.project_config.as_mut() {
            Some(project_config) => project_config
                .flush_file()
                .map_err(|e| anyhow!("Failed to flush project lunatic.toml config {e:?}")),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        global_config_path: PathBuf,
        credential_store: Arc<dyn CredentialStore>,
    ) -> Result<Self, ConfigError> {
        let _transaction = ConfigTransactionLock::acquire(&global_config_path)?;
        let global_config = load_global_config(&global_config_path, credential_store.as_ref())?;
        Ok(Self {
            global_config,
            project_config: None,
            credential_store,
            global_config_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Barrier, Mutex,
        },
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[derive(Default)]
    struct TestCredentialStore {
        entries: Mutex<HashMap<CredentialReference, Vec<u8>>>,
        fail_get: AtomicBool,
        fail_put: AtomicBool,
        fail_put_after_insert: AtomicBool,
        corrupt_put_after_insert: AtomicBool,
        fail_delete: AtomicBool,
        silently_skip_delete: AtomicBool,
        put_count: AtomicUsize,
    }

    impl TestCredentialStore {
        fn as_store(self: &Arc<Self>) -> Arc<dyn CredentialStore> {
            self.clone()
        }

        fn entry_count(&self) -> usize {
            self.entries.lock().unwrap().len()
        }

        fn contains_marker(&self, marker: &[u8]) -> bool {
            self.entries
                .lock()
                .unwrap()
                .values()
                .any(|entry| entry.windows(marker.len()).any(|window| window == marker))
        }

        fn clear(&self) {
            self.entries.lock().unwrap().clear();
        }
    }

    impl CredentialStore for TestCredentialStore {
        fn put(
            &self,
            reference: &CredentialReference,
            credential: &ControlCredential,
        ) -> Result<(), CredentialStoreError> {
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(CredentialStoreError::Unavailable);
            }
            self.put_count.fetch_add(1, Ordering::SeqCst);
            let mut encoded = credential.encode()?.to_vec();
            if self.corrupt_put_after_insert.load(Ordering::SeqCst) {
                let last = encoded.last_mut().unwrap();
                *last ^= 1;
            }
            self.entries
                .lock()
                .unwrap()
                .insert(reference.clone(), encoded);
            if self.fail_put_after_insert.load(Ordering::SeqCst) {
                return Err(CredentialStoreError::Failed);
            }
            Ok(())
        }

        fn get(
            &self,
            reference: &CredentialReference,
        ) -> Result<ControlCredential, CredentialStoreError> {
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(CredentialStoreError::Unavailable);
            }
            let entries = self.entries.lock().unwrap();
            let encoded = entries
                .get(reference)
                .ok_or(CredentialStoreError::Missing)?;
            ControlCredential::decode(encoded)
        }

        fn delete(&self, reference: &CredentialReference) -> Result<(), CredentialStoreError> {
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(CredentialStoreError::Unavailable);
            }
            if self.silently_skip_delete.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.entries
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

    fn test_manager() -> (
        tempfile::TempDir,
        PathBuf,
        Arc<TestCredentialStore>,
        ConfigManager,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let store = Arc::new(TestCredentialStore::default());
        let manager = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap();
        (directory, path, store, manager)
    }

    #[test]
    fn provider_debug_redacts_url_and_reference() {
        let provider = Provider::new(
            "https://example.invalid".to_owned(),
            CredentialReference::new(),
        )
        .unwrap();

        let debug = format!("{provider:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("https://example.invalid"));
    }

    #[test]
    fn login_persists_only_an_opaque_reference() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker; HttpOnly; Path=/".to_owned()],
                100,
            )
            .unwrap();

        let file = std::fs::read_to_string(path).unwrap();
        let serialized = toml::to_vec(&manager.global_config).unwrap();
        let debug = format!("{manager:?}");
        for marker in ["login-marker", "cookie-marker", "cookies =", "login_id ="] {
            assert!(!file.contains(marker));
            assert!(!serialized
                .windows(marker.len())
                .any(|window| window == marker.as_bytes()));
            assert!(!debug.contains(marker));
        }
        assert!(file.contains("credential_ref"));
        assert!(!store.contains_marker(b"cookie-marker"));
        assert_eq!(
            manager
                .control_credential()
                .unwrap()
                .cookie_header_at(100)
                .unwrap(),
            "session=cookie-marker"
        );
        assert_eq!(store.entry_count(), 1);
    }

    #[tokio::test]
    async fn stored_session_is_reused_as_one_sanitized_cookie_header() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_directory, _path, _store, mut manager) = test_manager();
        manager
            .login_at(
                format!("http://{address}"),
                "login".to_owned(),
                vec![
                    "session=cookie-marker; HttpOnly; Path=/".to_owned(),
                    "csrf=csrf-marker; SameSite=Strict".to_owned(),
                ],
                100,
            )
            .unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut socket).await.to_lowercase();
            assert!(request.contains("cookie: session=cookie-marker; csrf=csrf-marker\r\n"));
            assert!(!request.contains("httponly"));
            assert!(!request.contains("samesite"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .await
                .unwrap();
        });

        let (client, provider) = manager.get_http_client_at(100).unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client
                .get(provider.get_url().unwrap().join("/probe").unwrap())
                .send(),
        )
        .await
        .expect("authenticated request timed out")
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn authenticated_client_does_not_follow_redirects() {
        let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider_listener.local_addr().unwrap();
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_address = redirect_listener.local_addr().unwrap();
        let (_directory, _path, _store, mut manager) = test_manager();
        manager
            .login_at(
                format!("http://{provider_address}"),
                "login".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();

        let provider_server = tokio::spawn(async move {
            let (mut socket, _) = provider_listener.accept().await.unwrap();
            let request = read_request_headers(&mut socket).await.to_lowercase();
            assert!(request.contains("cookie: session=cookie-marker\r\n"));
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

        let (client, provider) = manager.get_http_client_at(100).unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client
                .get(provider.get_url().unwrap().join("/probe").unwrap())
                .send(),
        )
        .await
        .expect("redirect refusal request timed out")
        .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        provider_server.await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            redirect_listener.accept()
        )
        .await
        .is_err());
    }

    #[test]
    fn protected_credential_is_bound_to_configured_provider_origin() {
        let (_directory, _path, _store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        manager.global_config.provider.as_mut().unwrap().name =
            "https://attacker.invalid".to_owned();

        let error = manager.get_http_client_at(100).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential is invalid");
        assert!(!format!("{error:#}").contains("cookie-marker"));
        assert!(!format!("{error:#}").contains("login-marker"));
    }

    #[tokio::test]
    async fn platform_request_rejects_a_cross_origin_path_before_network_use() {
        let (_directory, _path, _store, mut manager) = test_manager();
        manager
            .login(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
            )
            .unwrap();

        let error = manager
            .request_platform::<serde_json::Value, ()>(
                Method::GET,
                "https://attacker.invalid/steal",
                "test",
                None,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Platform request URL must remain on the configured provider origin"
        );
    }

    #[test]
    fn expired_session_fails_closed_without_exposing_marker() {
        let (_directory, _path, _store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker; Max-Age=1".to_owned()],
                100,
            )
            .unwrap();

        let error = manager.get_http_client_at(101).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential has expired");
        assert!(!format!("{error:#}").contains("cookie-marker"));
        assert!(!format!("{error:#}").contains("login-marker"));
    }

    #[test]
    fn unavailable_store_rejects_login_without_plaintext_fallback() {
        let (_directory, path, store, mut manager) = test_manager();
        store.fail_put.store(true, Ordering::SeqCst);
        let error = manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        let file = std::fs::read_to_string(path).unwrap();
        assert!(!file.contains("login-marker"));
        assert!(!file.contains("cookie-marker"));
        assert!(!file.contains("provider"));
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn login_write_then_error_deletes_partial_protected_entry() {
        let (_directory, path, store, mut manager) = test_manager();
        store.fail_put_after_insert.store(true, Ordering::SeqCst);
        let error = manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "CLI credential store operation failed");
        assert_eq!(store.entry_count(), 0);
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("credential_ref"));
    }

    #[test]
    fn login_lock_failure_does_not_create_a_protected_entry() {
        let (directory, _path, store, mut manager) = test_manager();
        drop(directory);

        let error = manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(store.entry_count(), 0);
        assert!(manager.global_config.provider.is_none());
        assert!(!format!("{error:#}").contains("cookie-marker"));
        assert!(!format!("{error:#}").contains("login-marker"));
    }

    #[test]
    fn login_staging_write_failure_does_not_create_a_protected_entry() {
        let (_directory, _path, store, mut manager) = test_manager();
        let error = manager
            .login_at_with_writer(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
                |_path, _config| Err(ConfigError::FileWriteFailed),
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(store.entry_count(), 0);
        assert_eq!(store.put_count.load(Ordering::SeqCst), 0);
        assert!(manager.global_config.provider.is_none());
        assert!(manager.global_config.pending_credential_deletion.is_none());
    }

    #[test]
    fn login_staging_durability_error_keeps_only_a_retryable_tombstone() {
        let (_directory, path, store, mut manager) = test_manager();
        let error = manager
            .login_at_with_writer(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
                |path, config| {
                    write_private_toml(path, config)?;
                    Err(ConfigError::FileDurabilityUncertain)
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Lunatic configuration was replaced but directory durability is uncertain"
        );
        assert_eq!(store.put_count.load(Ordering::SeqCst), 0);
        assert_eq!(store.entry_count(), 0);
        assert!(manager.global_config.provider.is_none());
        assert!(manager.global_config.pending_credential_deletion.is_some());
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains("pending_credential_deletion"));

        assert!(manager.logout().unwrap());
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("pending_credential_deletion"));
    }

    #[test]
    fn login_final_write_failure_keeps_a_retryable_tombstone_and_credential() {
        let (_directory, path, store, mut manager) = test_manager();
        let write_count = AtomicUsize::new(0);
        let error = manager
            .login_at_with_writer(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
                |path, config| {
                    if write_count.fetch_add(1, Ordering::SeqCst) == 0 {
                        write_private_toml(path, config)
                    } else {
                        Err(ConfigError::FileWriteFailed)
                    }
                },
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(store.entry_count(), 1);
        assert_eq!(store.put_count.load(Ordering::SeqCst), 1);
        assert!(manager.global_config.provider.is_none());
        assert!(manager.global_config.pending_credential_deletion.is_some());
        let staged = std::fs::read_to_string(&path).unwrap();
        assert!(!staged.contains("[provider]"));
        assert!(staged.contains("pending_credential_deletion"));

        assert!(manager.logout().unwrap());
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn login_durability_uncertain_keeps_committed_reference_and_credential() {
        let (_directory, path, store, mut manager) = test_manager();
        let write_count = AtomicUsize::new(0);
        let error = manager
            .login_at_with_writer(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
                |path, config| {
                    write_private_toml(path, config)?;
                    if write_count.fetch_add(1, Ordering::SeqCst) == 0 {
                        Ok(())
                    } else {
                        Err(ConfigError::FileDurabilityUncertain)
                    }
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Lunatic configuration was replaced but directory durability is uncertain"
        );
        assert_eq!(store.entry_count(), 1);
        assert!(manager.global_config.provider.is_some());
        assert!(manager.global_config.pending_credential_deletion.is_none());
        let committed = std::fs::read_to_string(path).unwrap();
        assert!(committed.contains("credential_ref"));
        assert!(!committed.contains("pending_credential_deletion"));
    }

    #[test]
    fn concurrent_first_logins_share_one_transactional_reference() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let store = Arc::new(TestCredentialStore::default());
        let first = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap();
        let second = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap();
        let barrier = Arc::new(Barrier::new(2));

        let first_barrier = barrier.clone();
        let first_thread = std::thread::spawn(move || {
            let mut manager = first;
            first_barrier.wait();
            manager
                .login_at(
                    "https://example.invalid".to_owned(),
                    "login-one".to_owned(),
                    vec!["session=cookie-one".to_owned()],
                    100,
                )
                .unwrap();
        });
        let second_thread = std::thread::spawn(move || {
            let mut manager = second;
            barrier.wait();
            manager
                .login_at(
                    "https://example.invalid".to_owned(),
                    "login-two".to_owned(),
                    vec!["session=cookie-two".to_owned()],
                    100,
                )
                .unwrap();
        });
        first_thread.join().unwrap();
        second_thread.join().unwrap();

        assert_eq!(store.entry_count(), 1);
        let reloaded = ConfigManager::new_for_test(path, store.as_store()).unwrap();
        assert!(reloaded.control_credential().is_ok());
        drop(directory);
    }

    #[test]
    fn unavailable_store_rejects_reuse_before_network_construction() {
        let (_directory, _path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        store.fail_get.store(true, Ordering::SeqCst);

        let error = manager.get_http_client_at(100).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        assert!(!format!("{error:#}").contains("cookie-marker"));
        assert!(!format!("{error:#}").contains("login-marker"));
    }

    #[test]
    fn logout_deletes_credential_and_reference() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        assert!(manager.logout().unwrap());

        assert_eq!(store.entry_count(), 0);
        let file = std::fs::read_to_string(path).unwrap();
        assert!(!file.contains("credential_ref"));
        assert!(!file.contains("cookie-marker"));
        assert_eq!(
            manager.get_http_client().unwrap_err().to_string(),
            "First login by calling `lunatic login`"
        );
        assert!(!manager.logout().unwrap());
    }

    #[test]
    fn logout_store_failure_retains_a_retry_tombstone_and_reports_failure() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        store.fail_delete.store(true, Ordering::SeqCst);
        let error = manager.logout().unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        let current = std::fs::read_to_string(path).unwrap();
        assert!(!current.contains("[provider]"));
        assert!(current.contains("pending_credential_deletion"));
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn logout_lock_failure_preserves_the_credential_and_reference() {
        let (directory, _path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        drop(directory);

        let error = manager.logout().unwrap_err();
        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(store.entry_count(), 1);
        assert!(manager.global_config.provider.is_some());
    }

    #[test]
    fn logout_config_write_failure_preserves_the_active_pair() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();

        let error = manager
            .logout_with_writer(|_path, _config| Err(ConfigError::FileWriteFailed))
            .unwrap_err();
        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(store.entry_count(), 1);
        assert!(manager.global_config.provider.is_some());
        assert!(std::fs::read_to_string(path)
            .unwrap()
            .contains("credential_ref"));
    }

    #[test]
    fn logout_accepts_an_already_missing_entry() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        store.clear();
        assert!(manager.logout().unwrap());
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("credential_ref"));
    }

    #[test]
    fn logout_detects_a_silent_delete_and_retries_from_the_tombstone() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        store.silently_skip_delete.store(true, Ordering::SeqCst);

        let error = manager.logout().unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store operation failed");
        assert_eq!(store.entry_count(), 1);
        let pending = std::fs::read_to_string(&path).unwrap();
        assert!(!pending.contains("[provider]"));
        assert!(pending.contains("pending_credential_deletion"));

        store.silently_skip_delete.store(false, Ordering::SeqCst);
        assert!(remove_local_credential(&path, || Ok(store.as_store())).unwrap());
        assert!(!remove_local_credential(&path, || Ok(store.as_store())).unwrap());
        assert_eq!(store.entry_count(), 0);
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("pending_credential_deletion"));
    }

    #[test]
    fn refresh_rollback_requires_an_exact_read_back() {
        let (_directory, _path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "first-login".to_owned(),
                vec!["session=first-cookie".to_owned()],
                100,
            )
            .unwrap();
        store.corrupt_put_after_insert.store(true, Ordering::SeqCst);

        let error = manager
            .login_at(
                "https://example.invalid".to_owned(),
                "second-login".to_owned(),
                vec!["session=second-cookie".to_owned()],
                100,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Failed to roll back a CLI credential-store operation"
        );
    }

    #[test]
    fn login_rejects_a_pending_deletion_before_putting_another_credential() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "first-login".to_owned(),
                vec!["session=first-cookie".to_owned()],
                100,
            )
            .unwrap();
        let provider = manager.global_config.provider.take().unwrap();
        manager.global_config.pending_credential_deletion = Some(provider.credential_ref);
        write_private_toml(&path, &manager.global_config).unwrap();
        let put_count = store.put_count.load(Ordering::SeqCst);

        let error = manager
            .login_at(
                "https://example.invalid".to_owned(),
                "second-login".to_owned(),
                vec!["session=second-cookie".to_owned()],
                100,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Complete the pending CLI credential deletion before logging in"
        );
        assert_eq!(store.put_count.load(Ordering::SeqCst), put_count);
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn legacy_plaintext_credential_migrates_once_and_is_scrubbed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker; HttpOnly"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        let manager = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap();

        let migrated = std::fs::read_to_string(&path).unwrap();
        assert!(!migrated.contains("login-marker"));
        assert!(!migrated.contains("cookie-marker"));
        assert!(!migrated.contains("cookies ="));
        assert!(migrated.contains("credential_ref"));
        assert_eq!(store.entry_count(), 1);
        assert_eq!(store.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager.control_credential().unwrap().login_id(),
            "login-marker"
        );

        let _reloaded = ConfigManager::new_for_test(path, store.as_store()).unwrap();
        assert_eq!(store.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn legacy_migration_store_failure_leaves_original_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        store.fail_put.store(true, Ordering::SeqCst);

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
        assert!(!error.to_string().contains("cookie-marker"));
    }

    #[test]
    fn legacy_migration_write_then_error_deletes_protected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        store.fail_put_after_insert.store(true, Ordering::SeqCst);

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store operation failed");
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn legacy_migration_unverifiable_delete_is_a_rollback_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        store.fail_get.store(true, Ordering::SeqCst);

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Failed to roll back a CLI credential-store operation"
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn legacy_migration_rollback_failure_is_explicit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        store.fail_put_after_insert.store(true, Ordering::SeqCst);
        store.fail_delete.store(true, Ordering::SeqCst);

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Failed to roll back a CLI credential-store operation"
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy);
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn legacy_migration_config_write_failure_rolls_back_protected_copy() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        let _transaction = ConfigTransactionLock::acquire(&path).unwrap();

        let error = load_global_config_with_writer(&path, store.as_ref(), &|_path, _config| {
            Err(ConfigError::FileWriteFailed)
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed to write Lunatic configuration");
        assert_eq!(std::fs::read_to_string(path).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn legacy_migration_durability_uncertain_keeps_committed_pair() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        let _transaction = ConfigTransactionLock::acquire(&path).unwrap();

        let error = load_global_config_with_writer(&path, store.as_ref(), &|path, config| {
            write_private_toml(path, config)?;
            Err(ConfigError::FileDurabilityUncertain)
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Lunatic configuration was replaced but directory durability is uncertain"
        );
        let migrated = std::fs::read_to_string(path).unwrap();
        assert!(migrated.contains("credential_ref"));
        assert!(!migrated.contains("cookie-marker"));
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn legacy_logout_scrubs_plaintext_before_store_failure_and_keeps_a_tombstone() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "not a valid provider"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let factory_called = AtomicBool::new(false);

        let error = remove_local_credential(&path, || {
            factory_called.store(true, Ordering::SeqCst);
            Err(ConfigError::CredentialStore(
                CredentialStoreError::Unavailable,
            ))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        assert!(factory_called.load(Ordering::SeqCst));
        let current = std::fs::read_to_string(path).unwrap();
        assert!(!current.contains("[provider]"));
        assert!(!current.contains("login-marker"));
        assert!(!current.contains("cookie-marker"));
        assert!(current.contains("pending_credential_deletion"));
    }

    #[test]
    fn interrupted_legacy_migration_logout_deletes_the_deterministic_copy() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&path, legacy).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        let reference =
            CredentialReference::for_legacy_migration("app-id", "https://example.invalid");
        let credential = ControlCredential::from_set_cookie_headers(
            &Provider::parse_url("https://example.invalid").unwrap(),
            "app-id",
            "login-marker".to_owned(),
            vec!["session=cookie-marker".to_owned()],
            100,
        )
        .unwrap();
        store.put(&reference, &credential).unwrap();
        store.fail_delete.store(true, Ordering::SeqCst);

        let error = remove_local_credential(&path, || Ok(store.as_store())).unwrap_err();
        assert_eq!(error.to_string(), "CLI credential store is unavailable");
        let pending = std::fs::read_to_string(&path).unwrap();
        assert!(!pending.contains("login-marker"));
        assert!(!pending.contains("cookie-marker"));
        assert!(pending.contains("pending_credential_deletion"));
        assert_eq!(store.entry_count(), 1);

        store.fail_delete.store(false, Ordering::SeqCst);
        assert!(remove_local_credential(&path, || Ok(store.as_store())).unwrap());
        assert_eq!(store.entry_count(), 0);
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("pending_credential_deletion"));
    }

    #[test]
    fn current_logout_ignores_invalid_provider_url_and_deletes_by_reference() {
        let (_directory, path, store, mut manager) = test_manager();
        manager
            .login_at(
                "https://example.invalid".to_owned(),
                "login-marker".to_owned(),
                vec!["session=cookie-marker".to_owned()],
                100,
            )
            .unwrap();
        let config = std::fs::read_to_string(&path)
            .unwrap()
            .replace("https://example.invalid", "not a valid provider");
        std::fs::write(&path, config).unwrap();

        assert!(remove_local_credential(&path, || Ok(store.as_store())).unwrap());
        assert_eq!(store.entry_count(), 0);
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("credential_ref"));
    }

    #[test]
    fn logout_clears_an_invalid_opaque_reference_without_store_access() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let current = r#"cli_app_id = "app-id"
version = "0.2.0"

[provider]
name = "not a valid provider"
credential_ref = "not-a-uuid"
"#;
        std::fs::write(&path, current).unwrap();
        let factory_called = AtomicBool::new(false);

        assert!(remove_local_credential(&path, || {
            factory_called.store(true, Ordering::SeqCst);
            Err(ConfigError::CredentialStore(
                CredentialStoreError::Unavailable,
            ))
        })
        .unwrap());
        assert!(!factory_called.load(Ordering::SeqCst));
        assert!(!std::fs::read_to_string(path)
            .unwrap()
            .contains("credential_ref"));
    }

    #[test]
    fn corrupted_global_config_fails_instead_of_regenerating_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let original = "not valid = [toml";
        std::fs::write(&path, original).unwrap();
        let store = Arc::new(TestCredentialStore::default());
        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to decode Lunatic configuration");
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn unknown_secret_field_fails_closed_and_remains_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let original = r#"cli_app_id = "app-id"
version = "0.2.0"
token = "secret-marker"
"#;
        std::fs::write(&path, original).unwrap();
        let store = Arc::new(TestCredentialStore::default());

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to decode Lunatic configuration");
        assert!(!error.to_string().contains("secret-marker"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn invalid_current_reference_fails_closed_and_remains_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let original = r#"cli_app_id = "app-id"
version = "0.2.0"

[provider]
name = "https://example.invalid"
credential_ref = "not-a-uuid"
"#;
        std::fs::write(&path, original).unwrap();
        let store = Arc::new(TestCredentialStore::default());

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to decode Lunatic configuration");
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn invalid_pending_deletion_reference_fails_closed_and_remains_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lunatic.toml");
        let original = r#"cli_app_id = "app-id"
version = "0.2.0"
pending_credential_deletion = "not-a-uuid"
"#;
        std::fs::write(&path, original).unwrap();
        let store = Arc::new(TestCredentialStore::default());

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to decode Lunatic configuration");
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn provider_url_rejects_embedded_credentials_without_echoing_them() {
        let marker = "password-marker";
        let error =
            Provider::parse_url(&format!("https://user:{marker}@example.invalid")).unwrap_err();
        assert!(!format!("{error:#}").contains(marker));
    }

    #[test]
    fn provider_url_rejects_non_root_paths() {
        let error = Provider::parse_url("https://example.invalid/token-marker").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Provider URL must be an HTTPS origin (HTTP is allowed only for loopback) with no user info, path, query, or fragment"
        );
        assert!(!error.to_string().contains("token-marker"));
    }

    #[cfg(unix)]
    #[test]
    fn global_config_symlink_is_rejected_before_migration_or_logout() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.toml");
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&target, legacy).unwrap();
        symlink(&target, &path).unwrap();
        let store = Arc::new(TestCredentialStore::default());

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to read Lunatic configuration");
        let factory_called = AtomicBool::new(false);
        let error = remove_local_credential(&path, || {
            factory_called.store(true, Ordering::SeqCst);
            Ok(store.as_store())
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed to read Lunatic configuration");
        assert!(!factory_called.load(Ordering::SeqCst));
        assert_eq!(std::fs::read_to_string(target).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn global_config_hard_link_is_rejected_before_migration_or_logout() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.toml");
        let path = directory.path().join("lunatic.toml");
        let legacy = r#"cli_app_id = "app-id"
version = "0.1.0"

[provider]
login_id = "login-marker"
name = "https://example.invalid"
cookies = ["session=cookie-marker"]
"#;
        std::fs::write(&target, legacy).unwrap();
        std::fs::hard_link(&target, &path).unwrap();
        let store = Arc::new(TestCredentialStore::default());

        let error = ConfigManager::new_for_test(path.clone(), store.as_store()).unwrap_err();
        assert_eq!(error.to_string(), "Failed to read Lunatic configuration");
        let factory_called = AtomicBool::new(false);
        let error = remove_local_credential(&path, || {
            factory_called.store(true, Ordering::SeqCst);
            Ok(store.as_store())
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed to read Lunatic configuration");
        assert!(!factory_called.load(Ordering::SeqCst));
        assert_eq!(std::fs::read_to_string(target).unwrap(), legacy);
        assert_eq!(store.entry_count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn newly_created_global_config_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::tempdir().unwrap();
        let path = ensure_global_config_directory(home.path()).unwrap();
        assert_eq!(path, home.path().join(".lunatic/lunatic.toml"));
        assert_eq!(
            std::fs::metadata(home.path().join(".lunatic"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[cfg(unix)]
    #[test]
    fn private_config_open_restricts_existing_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("lunatic-private-config-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, b"secret").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let file = super::open_private_config(&path, false).unwrap();
        assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);

        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
