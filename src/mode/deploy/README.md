# Lunatic platform CLI

This platform-related subset of lunatic CLI app is a command-line interface tool that allows you authenticate, manage, build and deploy programs to the lunatic platform.

## Getting Started

1. Before starting install [Lunatic](https://github.com/lunatic-solutions/lunatic).

    ```
    cargo install lunatic-runtime
    ```

2. Create a new account in [Lunatic Cloud](https://lunatic.cloud/).

    Then, login your Lunatic CLI and connect it with Your account.

    ```
    lunatic login
    ```

    Follow instructions displayed in Your terminal and authorize the CLI.

    The CLI stores the resulting credential in Keychain (macOS), Credential
    Manager with local persistence (Windows), or Secret Service (Linux). The global `lunatic.toml`
    contains only an opaque reference. Headless Linux sessions must provide a
    working user session D-Bus and unlocked Secret Service collection; the CLI
    fails closed and has no plaintext fallback.

    To remove the local credential and its reference:

    ```
    lunatic logout
    ```

    Logout is local only; it does not revoke the provider-side session. See
    [`docs/security/CLI_CREDENTIAL_STORAGE.md`](../../../docs/security/CLI_CREDENTIAL_STORAGE.md)
    for migration, recovery, and verification details.


3. Create a new Lunatic Rust project (skip if you have an existing one).

    ```
    # Add the WebAssemby target
    rustup target add wasm32-wasip1
    
    # Create a new Rust project
    cargo new hello-lunatic
    cd hello-lunatic

    # Initialize project for Lunatic 
    lunatic init
    ```

4. Setup Your project on the [Lunatic Cloud](https://lunatic.cloud).

    ```
    lunatic app create hello-lunatic
    ```

    This will create a `lunatic.toml` configuration file with the following content.
    ```toml
    project_id = 17
    project_name = "hello-lunatic"
    domains = ["73685543-25ce-462d-b397-21bf921873d6.lunatic.run"]
    app_id = 18
    env_id = 17
    ```

5. Deploy Your application.

    ```
    lunatic deploy
    ```
