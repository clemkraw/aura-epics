//! AURA HTTP API.
//!
//! `router()` builds the API routes and nothing else. Composition —
//! serving a frontend, mounting under a prefix, adding auth — belongs to
//! the caller, which is why `serve()` takes a `Router` rather than building
//! one. That keeps this crate compilable and testable on its own, with no
//! dependency on anything npm produces.
//!
//! `ApiState` is deliberately minimal. The API reaches the database and
//! nothing else — not the driver, not the ingest threads, not the shared
//! buffers. Adding a field here is the moment to ask whether an immutable
//! snapshot would do instead.

pub mod error;
pub mod iocs;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
}

pub type SharedState = Arc<ApiState>;

/// The API routes, with their middleware, ready to be composed.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/iocs", get(iocs::list).post(iocs::create))
        // CatchPanicLayer turns a panicking handler into a 500 instead of
        // taking the process down with it. It needs unwinding panics, so
        // the release profile must not set `panic = "abort"` — otherwise a
        // malformed request becomes a denial of service on the archiver,
        // buffers and all.
        .layer(CatchPanicLayer::new())
        .layer(CompressionLayer::new())
        // Permissive CORS is for the Vite dev server. In production the
        // frontend is served from this same origin and this can go.
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

async fn health() -> &'static str {
    "ok"
}

/// Bind and serve `app` until `shutdown` is canceled.
///
/// Takes the router rather than building it: the binary composes the API
/// with the embedded frontend, and this crate has no business knowing that
/// a frontend exists.
pub async fn serve(
    app: Router,
    addr: SocketAddr,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("api listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}
