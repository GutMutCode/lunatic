use std::{fmt::Display, sync::Arc};

use axum::{
    async_trait,
    extract::{FromRequest, FromRequestParts, Path},
    http::{self, request::Parts, HeaderMap, HeaderValue, Request},
    response::{IntoResponse, Response},
    Extension, Json,
};
use http::header;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;

use crate::server::ControlServer;

pub type ApiResponse<D> = Result<Json<D>, ApiError>;
pub type SecretApiResponse<D> = Result<(HeaderMap, Json<D>), ApiError>;

pub fn ok<D: Serialize>(data: D) -> ApiResponse<D> {
    Ok(Json(data))
}

pub fn ok_secret<D: Serialize>(data: D) -> SecretApiResponse<D> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    Ok((headers, Json(data)))
}

#[derive(Debug)]
pub enum ApiError {
    Internal,
    NotAuthenticated,
    NotAuthorized,
    Custom { code: &'static str },
}

impl ApiError {
    pub fn code(&self) -> &str {
        match self {
            ApiError::Internal => "internal",
            ApiError::NotAuthenticated => "unauthenticated",
            ApiError::NotAuthorized => "unauthorized",
            ApiError::Custom { code } => code,
        }
    }

    pub fn message(&self) -> String {
        match self {
            ApiError::Internal => "".into(),
            ApiError::NotAuthenticated => "Not authenticated".into(),
            ApiError::NotAuthorized => "Not authorized".into(),
            ApiError::Custom { .. } => "".into(),
        }
    }

    #[allow(dead_code)]
    pub fn log_internal(msg: &str, e: impl std::fmt::Debug) -> Self {
        log::error!("{}: {:?}", msg, e);
        Self::Internal
    }

    pub fn custom_code(code: &'static str) -> Self {
        Self::Custom { code }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Error ")?;
        f.write_str(self.code())?;
        let msg = self.message();
        if !msg.is_empty() {
            f.write_str(": ")?;
            f.write_str(&msg)?;
        }
        Ok(())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use http::StatusCode as S;
        use ApiError::*;

        let body = Json(json!({
            "message": self.message(),
            "code": self.code(),
        }));

        let status = match self {
            Self::Internal => S::INTERNAL_SERVER_ERROR,
            Self::NotAuthenticated => S::UNAUTHORIZED,
            Self::NotAuthorized => S::FORBIDDEN,
            Custom { .. } => S::BAD_REQUEST,
        };

        (status, body).into_response()
    }
}

pub struct JsonExtractor<T>(pub T);

#[async_trait]
impl<S, B, T> FromRequest<S, B> for JsonExtractor<T>
where
    axum::Json<T>: FromRequest<S, B, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
    B: Send + 'static,
{
    type Rejection = ApiError;

    async fn from_request(req: Request<B>, state: &S) -> Result<JsonExtractor<T>, Self::Rejection> {
        match Json::from_request(req, state).await {
            Ok(Json(value)) => Ok(JsonExtractor(value)),
            Err(_) => Err(ApiError::custom_code("invalid_data")),
        }
    }
}

pub struct PathExtractor<T>(pub T);

#[async_trait]
impl<S, T> FromRequestParts<S> for PathExtractor<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        req: &mut Parts,
        state: &S,
    ) -> Result<PathExtractor<T>, Self::Rejection> {
        match Path::from_request_parts(req, state).await {
            Ok(Path(value)) => Ok(PathExtractor(value)),
            Err(_) => Err(ApiError::custom_code("invalid_path_arg")),
        }
    }
}

#[derive(Debug)]
pub struct NodeAuth {
    pub registration_id: u64,
    pub bearer_generation: u64,
    pub node_name: uuid::Uuid,
}

#[async_trait]
impl<S> FromRequestParts<S> for NodeAuth
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(req: &mut Parts, state: &S) -> Result<NodeAuth, Self::Rejection> {
        let cs: Extension<Arc<ControlServer>> = Extension::from_request_parts(req, state)
            .await
            .map_err(|e| ApiError::log_internal("Error getting control server state", e))?;
        let auth_header = req
            .headers
            .get(header::AUTHORIZATION)
            .ok_or_else(|| ApiError::custom_code("no_auth_header"))?
            .to_str()
            .map_err(|_| ApiError::custom_code("invalid_auth_header"))?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| ApiError::custom_code("invalid_auth_token"))?;

        let node_name = req
            .headers
            .get("x-lunatic-node-name")
            .ok_or_else(|| ApiError::custom_code("no_lunatic_node_name_header"))?
            .to_str()
            .map_err(|_| ApiError::custom_code("invalid_lunatic_node_name_header"))?;

        let node_name: uuid::Uuid = node_name
            .parse()
            .map_err(|_| ApiError::custom_code("invalid_lunatic_node_name_header"))?;

        let authenticated = cs
            .authenticate(node_name, token)
            .ok_or(ApiError::NotAuthenticated)?;
        let node_auth = NodeAuth {
            registration_id: authenticated.registration_id,
            bearer_generation: authenticated.bearer_generation,
            node_name,
        };

        Ok(node_auth)
    }
}
