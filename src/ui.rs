//! Static frontend, embedded in the binary.
//!
//! This lives in the binary rather than in `aura-api` on purpose. The API
//! is a product of its own — non-BFF, consumable by a Python script — and
//! the web interface is one of its clients. Making the API carry its own
//! client inverts that relationship.
//!
//! The practical consequence matters more: `#[folder]` is resolved by the
//! compiler, so embedding inside `aura-api` would mean the crate cannot be
//! compiled or tested without running npm first. Here the coupling to
//! `web/dist` sits in the artefact that is actually being deployed, next to
//! the code that already decides the listen address and the runtime.

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::{EmbeddedFile, RustEmbed};

#[derive(RustEmbed)]
#[folder = "web/dist"]
struct Assets;

/// Attach the SPA to an API router.
///
/// The fallback only sees requests no API route matched, so `/api/...`
/// typos still return the API's own 404 rather than an HTML page.
pub fn attach(api: axum::Router) -> axum::Router {
    api.fallback(handler)
}

async fn handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => respond(path, file),
        None => {
            // A request that looks like a file — it has an extension — and
            // is missing is a genuine 404. Serving index.html for it would
            // return HTML with a 200 under a `.js` URL, which the browser
            // rejects on MIME grounds and reports as a parse error three
            // stack frames away from the real cause.
            if path.rsplit('/').next().is_some_and(|f| f.contains('.')) {
                return (StatusCode::NOT_FOUND, "not found").into_response();
            }
            // Anything else is a client-side route: /iocs, /charts, /pvs.
            // Without this, reloading the page on one of them 404s.
            match Assets::get("index.html") {
                Some(file) => respond("index.html", file),
                None => (
                    StatusCode::NOT_FOUND,
                    "web/dist is empty — run `npm run build` in web/ before `cargo build --release`",
                )
                    .into_response(),
            }
        }
    }
}

fn respond(path: &str, file: EmbeddedFile) -> Response {
    let mime = file.metadata.mimetype();

    // Vite hashes every asset filename, so those are immutable forever and
    // index.html must never be cached — otherwise an operator keeps the old
    // interface after an upgrade and nothing tells them.
    let cache = if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };

    let mut res = Response::new(Body::from(file.data.into_owned()));
    let h = res.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime).unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    res
}
