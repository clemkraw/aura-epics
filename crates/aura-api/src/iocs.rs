//! `/api/v1/iocs` — PVAccess servers, declared and observed.
//!
//! An IOC has no state of its own in AURA: `ioc_config` stores four columns
//! and nothing else. Everything this module reports is derived from the PVs
//! the crate serves, grouped on `pv_status.ioc_addr`.
//!
//! That is the right shape, not a limitation. A cached `ioc_status` row
//! would be a second source of truth able to disagree with the PVs it
//! claims to describe — and when it does, the operator believes the wrong
//! one.

use std::net::SocketAddr;
use std::str::FromStr;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SharedState;
use crate::error::ApiError;

/// pvxs listens here unless told otherwise.
pub const DEFAULT_PVA_PORT: u16 = 5075;

/* ────────────────────────────── States ────────────────────────────── */

pub mod state {
    pub const DISABLED: i16 = 0;
    pub const CONNECTING: i16 = 1;
    pub const UP: i16 = 2;
    pub const DEGRADED: i16 = 3;
    pub const DOWN: i16 = 4;
    pub const IDLE: i16 = 5;
}

/// Derived crate state.
///
/// Five values, not two. `DEGRADED` is the one that earns its place: an IOC
/// answering on TCP while a subset of its PVs sit disconnected is the
/// failure an up/down light cannot show — and it is the common one. A
/// record deleted on the IOC side, a typo in a PV name, a module that did
/// not come back after a reboot.
///
/// Computed here rather than in SQL so it can be unit-tested and so there
/// is exactly one definition. If a second consumer ever needs it, it moves
/// to `aura-store`.
pub fn derive_state(declared: bool, enabled: bool, c: &Counts) -> i16 {
    if declared && !enabled {
        return state::DISABLED;
    }
    // An unsubscribed crate has rows but no live PVs; treat it as idle
    // rather than down, since nothing is trying to connect.
    if c.total == 0 || c.total == c.unsubscribed {
        return state::IDLE;
    }

    let live = c.connected + c.archiving;
    let lost = c.disconnected + c.timeout;

    if live == 0 && lost > 0 {
        state::DOWN
    } else if live == 0 {
        state::CONNECTING
    } else if lost > 0 {
        state::DEGRADED
    } else {
        state::UP
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Counts {
    pub total: i64,
    pub archiving: i64,
    pub connected: i64,
    pub searching: i64,
    pub disconnected: i64,
    pub timeout: i64,
    pub unsubscribed: i64,
}

/* ────────────────────────────── Rows ────────────────────────────── */

/// One row of the aggregate query, before `state` is derived.
#[derive(Debug, sqlx::FromRow)]
struct IocRow {
    address: String,
    /// Null when the crate answers PVs but is absent from `ioc_config`.
    label: Option<String>,
    enabled: Option<bool>,
    created_at: Option<DateTime<Utc>>,
    declared: bool,

    pv_total: i64,
    pv_archiving: i64,
    pv_connected: i64,
    pv_searching: i64,
    pv_disconnected: i64,
    pv_timeout: i64,
    pv_unsubscribed: i64,

    events_per_s: f64,
    health: f64,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,

    disconnects_24h: i64,
    reconnects_24h: i64,
    last_disconnect_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct IocView {
    pub address: String,
    pub name: Option<String>,
    pub enabled: bool,
    pub created_at: Option<DateTime<Utc>>,
    /// False when the crate serves PVs but is absent from `ioc_config`.
    /// The interface offers to declare it; nothing else surfaces the case.
    pub declared: bool,

    pub state: i16,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,

    pub pv_total: i64,
    pub pv_archiving: i64,
    pub pv_connected: i64,
    pub pv_searching: i64,
    pub pv_disconnected: i64,
    pub pv_timeout: i64,
    pub pv_unsubscribed: i64,

    pub events_per_s: i64,
    pub health: f64,

    pub disconnects_24h: i64,
    pub reconnects_24h: i64,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl From<IocRow> for IocView {
    fn from(r: IocRow) -> Self {
        let counts = Counts {
            total: r.pv_total,
            archiving: r.pv_archiving,
            connected: r.pv_connected,
            searching: r.pv_searching,
            disconnected: r.pv_disconnected,
            timeout: r.pv_timeout,
            unsubscribed: r.pv_unsubscribed,
        };
        let enabled = r.enabled.unwrap_or(true);

        Self {
            state: derive_state(r.declared, enabled, &counts),
            address: r.address,
            name: r.label.filter(|s| !s.is_empty()),
            enabled,
            created_at: r.created_at,
            declared: r.declared,
            started_at: r.started_at,
            last_event_at: r.last_event_at,
            pv_total: r.pv_total,
            pv_archiving: r.pv_archiving,
            pv_connected: r.pv_connected,
            pv_searching: r.pv_searching,
            pv_disconnected: r.pv_disconnected,
            pv_timeout: r.pv_timeout,
            pv_unsubscribed: r.pv_unsubscribed,
            events_per_s: r.events_per_s.round() as i64,
            health: r.health,
            disconnects_24h: r.disconnects_24h,
            reconnects_24h: r.reconnects_24h,
            last_disconnect_at: r.last_disconnect_at,
            last_error: r.last_error,
        }
    }
}

/* ────────────────────────────── Query ────────────────────────────── */

/// One statement, three CTEs.
///
/// The FULL OUTER JOIN is the load-bearing part: `ioc_config` alone misses
/// crates that answer PV searches without being declared, and the
/// `pv_status` aggregate alone misses crates declared but not yet serving
/// anything. Both are real and both matter — the first is "someone plugged
/// in a crate", the second is "this one never came up".
const LIST_SQL: &str = "\
WITH agg AS (
    SELECT ioc_addr,
           count(*)                                      AS pv_total,
           count(*) FILTER (WHERE state = 3)             AS pv_archiving,
           count(*) FILTER (WHERE state = 2)             AS pv_connected,
           count(*) FILTER (WHERE state = 1)             AS pv_searching,
           count(*) FILTER (WHERE state = 4)             AS pv_disconnected,
           count(*) FILTER (WHERE state = 5)             AS pv_timeout,
           count(*) FILTER (WHERE state = 6)             AS pv_unsubscribed,
           COALESCE(sum(update_hz), 0)::float8           AS events_per_s,
           COALESCE(avg(health_score), 0)::float8        AS health,
           min(connected_at)                             AS started_at,
           max(last_event_at)                            AS last_event_at
      FROM pv_status
     WHERE ioc_addr IS NOT NULL
     GROUP BY ioc_addr
),
merged AS (
    SELECT COALESCE(c.address, a.ioc_addr)   AS address,
           c.label,
           c.enabled,
           c.created_at,
           (c.address IS NOT NULL)           AS declared,
           COALESCE(a.pv_total, 0)           AS pv_total,
           COALESCE(a.pv_archiving, 0)       AS pv_archiving,
           COALESCE(a.pv_connected, 0)       AS pv_connected,
           COALESCE(a.pv_searching, 0)       AS pv_searching,
           COALESCE(a.pv_disconnected, 0)    AS pv_disconnected,
           COALESCE(a.pv_timeout, 0)         AS pv_timeout,
           COALESCE(a.pv_unsubscribed, 0)    AS pv_unsubscribed,
           COALESCE(a.events_per_s, 0)       AS events_per_s,
           COALESCE(a.health, 0)             AS health,
           a.started_at,
           a.last_event_at
      FROM ioc_config c
      FULL OUTER JOIN agg a ON a.ioc_addr = c.address
),
ev AS (
    SELECT ioc_addr,
           count(*) FILTER (WHERE event_type = 3)        AS disconnects_24h,
           count(*) FILTER (WHERE event_type = 4)        AS reconnects_24h,
           max(ts)  FILTER (WHERE event_type = 3)        AS last_disconnect_at
      FROM pv_events
     WHERE ioc_addr IS NOT NULL
       AND ts > NOW() - INTERVAL '24 hours'
     GROUP BY ioc_addr
)
SELECT m.address, m.label, m.enabled, m.created_at, m.declared,
       m.pv_total, m.pv_archiving, m.pv_connected, m.pv_searching,
       m.pv_disconnected, m.pv_timeout, m.pv_unsubscribed,
       m.events_per_s, m.health, m.started_at, m.last_event_at,
       COALESCE(e.disconnects_24h, 0) AS disconnects_24h,
       COALESCE(e.reconnects_24h, 0)  AS reconnects_24h,
       e.last_disconnect_at,
       le.detail                      AS last_error
  FROM merged m
  LEFT JOIN ev e ON e.ioc_addr = m.address
  LEFT JOIN LATERAL (
      SELECT detail
        FROM pv_events
       WHERE ioc_addr = m.address
         AND detail IS NOT NULL
         AND event_type IN (3, 5, 7)
       ORDER BY ts DESC, event_id DESC
       LIMIT 1
  ) le ON TRUE
 WHERE ($1::text IS NULL OR m.address ILIKE $1 OR m.label ILIKE $1)
 ORDER BY COALESCE(NULLIF(m.label, ''), m.address)";

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Case-insensitive substring on label or address.
    pub q: Option<String>,
    /// Filter on the derived state.
    pub state: Option<i16>,
}

/// `GET /api/v1/iocs`
///
/// No pagination: a machine has tens of crates, not thousands, and an
/// `{items, total}` envelope here would be ceremony.
pub async fn list(
    State(st): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<IocView>>, ApiError> {
    // Escape LIKE metacharacters so a caller typing '%' searches for a
    // literal percent sign instead of matching everything.
    let pattern =
        q.q.as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                let escaped = s
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
                format!("%{escaped}%")
            });

    let rows = sqlx::query_as::<_, IocRow>(LIST_SQL)
        .bind(pattern)
        .fetch_all(&st.pool)
        .await?;

    let mut views: Vec<IocView> = rows.into_iter().map(IocView::from).collect();

    // Filtered after derivation, since `state` does not exist in SQL. At a
    // few dozen rows this costs nothing, and it keeps one definition of the
    // state instead of a second one written in the WHERE clause.
    if let Some(want) = q.state {
        views.retain(|v| v.state == want);
    }

    Ok(Json(views))
}

/* ────────────────────────────── Create ────────────────────────────── */

/// `ioc_config.label` is the column; `name` is the field on the wire.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct IocConfigRow {
    pub address: String,
    #[sqlx(rename = "label")]
    pub name: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateIoc {
    pub address: String,
    #[serde(default)]
    pub name: String,
}

/// Normalise an address to exactly what the pipeline will parse.
///
/// `main.rs` resolves declared IOCs with `.parse::<SocketAddr>().ok()` and
/// **silently drops** anything that fails. A hostname therefore produces a
/// row in `ioc_config` that is never connected, with no error anywhere —
/// the worst kind of failure, because the interface shows a declared IOC
/// that simply does nothing forever.
///
/// So the API validates with the consumer's own parser: what it accepts is
/// by construction what the orchestrator will connect to.
///
/// A bare host gets the default pvxs port rather than being rejected;
/// callers almost always mean 5075, and the resolved value is returned so
/// nothing is assumed silently. IPv6 must use the bracket form
/// (`[::1]:5075`) — a bare `fe80::1` reads as host+port, which is
/// unavoidable without brackets and is what `SocketAddr` does too.
pub fn normalize_address(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("address is required".into());
    }
    if value.chars().any(char::is_whitespace) {
        return Err("address cannot contain whitespace".into());
    }

    let candidate = match value.rfind(':') {
        // A trailing colon is an unfinished port, not an omitted one.
        Some(i) if i + 1 == value.len() => return Err("missing port after ':'".into()),
        Some(_) => value.to_string(),
        None => format!("{value}:{DEFAULT_PVA_PORT}"),
    };

    SocketAddr::from_str(&candidate)
        .map(|a| a.to_string())
        .map_err(|_| {
            format!(
                "address must be IP:PORT, e.g. 192.168.12.53:{DEFAULT_PVA_PORT} \
             (hostnames are not supported: the pipeline resolves declared \
             IOCs with SocketAddr and would ignore this entry), got {value:?}"
            )
        })
}

/// `POST /api/v1/iocs`
pub async fn create(
    State(st): State<SharedState>,
    Json(body): Json<CreateIoc>,
) -> Result<(StatusCode, Json<IocConfigRow>), ApiError> {
    let address = normalize_address(&body.address).map_err(ApiError::BadRequest)?;

    let name = body.name.trim();
    if name.chars().count() > 64 {
        return Err(ApiError::BadRequest(
            "name is limited to 64 characters".into(),
        ));
    }

    // ON CONFLICT DO NOTHING + RETURNING: a conflict comes back as zero
    // rows, so the duplicate case is a value rather than an error code to
    // match on. One round trip, no dependency on SQLSTATE strings.
    let row = sqlx::query_as::<_, IocConfigRow>(
        "INSERT INTO ioc_config (address, label) VALUES ($1, $2) \
         ON CONFLICT (address) DO NOTHING \
         RETURNING address, label, enabled, created_at",
    )
    .bind(&address)
    .bind(name)
    .fetch_optional(&st.pool)
    .await?;

    match row {
        Some(row) => {
            tracing::info!("api: declared IOC {address}");
            Ok((StatusCode::CREATED, Json(row)))
        }
        None => Err(ApiError::Conflict(format!(
            "IOC already declared: {address}"
        ))),
    }
}

/* ────────────────────────────── Tests ────────────────────────────── */

#[cfg(test)]
mod tests {
    use super::{Counts, derive_state, normalize_address, state};

    fn counts(archiving: i64, connected: i64, lost: i64, searching: i64) -> Counts {
        Counts {
            total: archiving + connected + lost + searching,
            archiving,
            connected,
            searching,
            disconnected: lost,
            timeout: 0,
            unsubscribed: 0,
        }
    }

    #[test]
    fn all_healthy_is_up() {
        assert_eq!(derive_state(true, true, &counts(15, 0, 0, 0)), state::UP);
    }

    #[test]
    fn some_lost_is_degraded() {
        // The case an up/down light cannot express, and the common one.
        assert_eq!(
            derive_state(true, true, &counts(14, 0, 1, 0)),
            state::DEGRADED
        );
    }

    #[test]
    fn none_live_is_down() {
        assert_eq!(derive_state(true, true, &counts(0, 0, 17, 0)), state::DOWN);
    }

    #[test]
    fn still_searching_is_connecting() {
        assert_eq!(
            derive_state(true, true, &counts(0, 0, 0, 5)),
            state::CONNECTING
        );
    }

    #[test]
    fn disabled_wins_over_everything() {
        assert_eq!(
            derive_state(true, false, &counts(15, 0, 0, 0)),
            state::DISABLED
        );
    }

    #[test]
    fn no_pvs_is_idle() {
        assert_eq!(derive_state(true, true, &Counts::default()), state::IDLE);
    }

    #[test]
    fn fully_unsubscribed_is_idle_not_down() {
        let c = Counts {
            total: 5,
            unsubscribed: 5,
            ..Counts::default()
        };
        assert_eq!(derive_state(true, true, &c), state::IDLE);
    }

    #[test]
    fn undeclared_crate_is_never_disabled() {
        // `enabled` is meaningless for a crate absent from ioc_config.
        assert_eq!(derive_state(false, false, &counts(3, 0, 0, 0)), state::UP);
    }

    #[test]
    fn accepts_ip_port_and_defaults_the_port() {
        assert_eq!(
            normalize_address(" 192.168.12.53:5075 ").unwrap(),
            "192.168.12.53:5075"
        );
        assert_eq!(
            normalize_address("192.168.12.53").unwrap(),
            "192.168.12.53:5075"
        );
        assert_eq!(normalize_address("[::1]:5075").unwrap(), "[::1]:5075");
    }

    #[test]
    fn rejects_what_the_pipeline_would_drop() {
        assert!(normalize_address("cryo-ioc-3.lab.local:5075").is_err());
        assert!(normalize_address("192.168.12.999:5075").is_err());
        assert!(normalize_address("192.168.12.53:99999").is_err());
        assert!(normalize_address("192.168.12.53:").is_err());
        assert!(normalize_address("   ").is_err());
        assert!(normalize_address("192.168.12.53 :5075").is_err());
    }
}
