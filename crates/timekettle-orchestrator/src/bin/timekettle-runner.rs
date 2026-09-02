//! In-pod replay runner (the k8s Job's runner container).
//!
//! One replay run, then exit — the exit code is the Job's success signal.
//! The orchestrator created the run and rendered its spec into this pod;
//! progress flows back through `POST /api/v1/runs/{id}/events`
//! (`StoreCtx::http`), stores are sidecars reached directly, and the router
//! candidate is a pod container k8s manages.
//!
//! Environment contract (rendered by the Job template / chart):
//!   TIMEKETTLE_RUN_ID              run id the orchestrator created (required)
//!   TIMEKETTLE_RUN_SPEC            the run's RunSpec as JSON (required)
//!   TIMEKETTLE_ORCHESTRATOR_URL    push-back base, e.g. http://orchestrator:8080 (required)
//!   TIMEKETTLE_API_SERVICE_TOKEN   bearer token when the orchestrator enforces one
//!   TIMEKETTLE_RUNNER_ACTOR        X-Timekettle-Actor value (default system:runner)
//!   HARNESS_STATE_DIR        pod-local state root on the SHARED workspace
//!                            volume (default /workspace/state) — the router
//!                            container reads the lookup table from here
//!   RUNNER_DATABASE_URL      sidecar pg conninfo URL (required)
//!   RUNNER_REDIS_HOST/PORT   sidecar redis (default 127.0.0.1:6379)
//!   RUNNER_ROUTER_PORT       router container port (default 8080)
//!   RUNNER_KERNEL_BIN        timekettle-kernel path (default target/release/timekettle-kernel)
//!   RUNNER_MIGRATE_CMD       whitespace-split argv run at the migrate stage
//!                            with DATABASE_URL set; unset = pre-migrated pg.
//!                            Must apply the CANDIDATE's migrations (staged from
//!                            the candidate ref), never the runner's own baked set
//!   RUNNER_EXPECTED_MIGRATIONS  the candidate's own migration versions (one per
//!                            line), derived by the executor from the candidate
//!                            ref; the runner refuses (P1) if the live schema is
//!                            not exactly this set. Unset = record-only, no gate
//!   TIMEKETTLE_S3_*                recording source credentials (see timekettle-compactor)

use timekettle_orchestrator::lifecycle::{drive_replay_in_pod, InPodOptions, StoreCtx};
use timekettle_orchestrator::{HarnessRoot, Run, RunMode, RunSpec, RunStatus};

fn required(key: &str) -> Result<String, String> {
    std::env::var(key).map_err(|_| format!("{key} unset"))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        // The migrations initContainer runs this to pull + unpack the candidate's
        // CodeBundle (its migrations/ tree at sha_C) before the runner migrates.
        Some("stage-codebundle") => stage_codebundle(&args[1..]),
        // The config initContainer runs this to merge the candidate's own
        // config UNDER the recorded one and write the single file the candidate
        // boots from. Paths come from the caller; see `layer_config`.
        Some("layer-config") => layer_config(&args[1..]),
        // No subcommand: drive one replay run (the runner container's job).
        _ => run(),
    };
    if let Err(e) = result {
        eprintln!("timekettle-runner: FAILED: {e}");
        std::process::exit(1);
    }
}

/// `timekettle-runner stage-codebundle <s3-uri> <dest-dir>` — fetch the candidate's
/// migration tar from S3 and extract it under `<dest-dir>` (so `migrations/`
/// lands at `<dest-dir>/migrations`, which the runner's migrate command targets).
/// Self-contained: no `tar`/`aws` binary needed in the image. Option B delivery.
fn stage_codebundle(args: &[String]) -> Result<(), String> {
    let uri = args
        .get(1)
        .ok_or("stage-codebundle: missing <s3-uri> argument")?;
    let dest = args
        .get(2)
        .ok_or("stage-codebundle: missing <dest-dir> argument")?;
    let n = timekettle_orchestrator::codebundle::stage_bundle(uri, std::path::Path::new(dest))?;
    eprintln!("timekettle-runner: staged CodeBundle {uri} -> {dest} ({n} entries)");
    Ok(())
}

/// `timekettle-runner layer-config --base <toml> --over <toml> --out <toml>` —
/// merge two TOML config files into the one file the candidate boots from.
///
/// `--over` wins at every key it sets; `--base` supplies only what `--over` is
/// silent about. In a replay that means `--base` is the candidate's own config
/// at its ref (staged in the CodeBundle) and `--over` is the recorded one, so
/// the recording stays authoritative and the candidate fills only the keys the
/// recording could not have known about — a key the candidate added after the
/// tape was taken, without which it may refuse to boot.
///
/// All three paths come from the CALLER. This command does not know which file
/// the system under test boots, where that system keeps its own copy, or what
/// selects between several: the Job template mounts one and stages the other,
/// so it already holds both ends and simply says them. A different system
/// passes different paths and this code is unchanged.
///
/// See `config_layer` for the merge, the provenance and the three balances it
/// asserts.
fn layer_config(args: &[String]) -> Result<(), String> {
    let mut base: Option<String> = None;
    let mut over: Option<String> = None;
    let mut out: Option<String> = None;

    let usage = "expected --base <toml> --over <toml> --out <toml>";
    let mut i = 1;
    while i < args.len() {
        let flag = args[i].as_str();
        let value = || -> Result<String, String> {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("layer-config: {flag} needs a value"))
        };
        match flag {
            "--base" => base = Some(value()?),
            "--over" => over = Some(value()?),
            "--out" => out = Some(value()?),
            other => return Err(format!("layer-config: unknown argument {other:?}; {usage}")),
        }
        i += 2;
    }

    let base = base.ok_or_else(|| format!("layer-config: --base is required; {usage}"))?;
    let over = over.ok_or_else(|| format!("layer-config: --over is required; {usage}"))?;
    let out = out.ok_or_else(|| format!("layer-config: --out is required; {usage}"))?;

    // Whatever named the delivery of `--base` in this deployment, so a missing
    // base layer points at the thing that failed to deliver it.
    let source = std::env::var("TIMEKETTLE_CODE_BUNDLE_URI")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let report = timekettle_orchestrator::config_layer::layer_config_files(
        std::path::Path::new(&base),
        std::path::Path::new(&over),
        std::path::Path::new(&out),
        source.as_deref(),
    )?;

    // Every key the merge took from the base rather than the overriding layer,
    // by name, at boot — so the next config surprise is a five-minute
    // diagnosis instead of an archaeology session.
    eprint!("{}", report.render(&base, &over));
    for (section, n) in timekettle_orchestrator::config_layer::carried_by_section(&report) {
        eprintln!("config layering: from the base layer, by section: [{section}] x{n}");
    }
    eprintln!("timekettle-runner: layered config written to {out}");
    Ok(())
}

fn run() -> Result<(), String> {
    let run_id = required("TIMEKETTLE_RUN_ID")?;
    let spec: RunSpec = serde_json::from_str(&required("TIMEKETTLE_RUN_SPEC")?)
        .map_err(|e| format!("parse TIMEKETTLE_RUN_SPEC: {e}"))?;
    if !matches!(spec.mode, RunMode::Replay) {
        return Err("timekettle-runner drives replay runs only".to_owned());
    }
    let base = required("TIMEKETTLE_ORCHESTRATOR_URL")?;
    let token = std::env::var("TIMEKETTLE_API_SERVICE_TOKEN").ok();
    let actor =
        std::env::var("TIMEKETTLE_RUNNER_ACTOR").unwrap_or_else(|_| format!("system:runner:{run_id}"));

    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
    let opts = InPodOptions {
        redis_host: env("RUNNER_REDIS_HOST", "127.0.0.1"),
        redis_port: env("RUNNER_REDIS_PORT", "6379")
            .parse()
            .map_err(|e| format!("RUNNER_REDIS_PORT: {e}"))?,
        database_url: required("RUNNER_DATABASE_URL")?,
        router_port: env("RUNNER_ROUTER_PORT", "8080")
            .parse()
            .map_err(|e| format!("RUNNER_ROUTER_PORT: {e}"))?,
        kernel_bin: env("RUNNER_KERNEL_BIN", "target/release/timekettle-kernel"),
        migrate_cmd: std::env::var("RUNNER_MIGRATE_CMD")
            .ok()
            .map(|raw| {
                raw.split_whitespace()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .filter(|argv| !argv.is_empty()),
        // The CANDIDATE's own migration versions, supplied by the executor
        // (derived from the candidate ref — its migrations/ tree at its code
        // sha), newline-separated. Unset = no gate (record the live fingerprint,
        // don't refuse). This is a PARAMETER, never a harness constant.
        expected_schema: std::env::var("RUNNER_EXPECTED_MIGRATIONS")
            .ok()
            .map(|raw| {
                let versions = raw
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                timekettle_orchestrator::SchemaFingerprint::new(versions)
            })
            .filter(|fp| fp.count() > 0),
    };

    let root = HarnessRoot::new(env("HARNESS_STATE_DIR", "/workspace/state"))
        .map_err(|e| format!("state root: {e}"))?;
    // A3/A2: emit the exact artifact contract this runner will honor. All paths
    // are derived from the shared state root + run id (one derivation), so a Job
    // template whose candidate mount or lookup path disagrees shows up here, and
    // the candidate's boot guard is this snippet verbatim.
    let contract = root.replay_contract(&run_id);
    eprintln!(
        "timekettle-runner: replay contract — lookup_table={} observed_sink={} ready={}",
        contract.lookup_table.display(),
        contract.observed_sink.display(),
        contract.ready_sentinel.display(),
    );
    eprintln!(
        "timekettle-runner: candidate must boot-wait on seed: {}",
        contract.wait_for_seed_snippet(),
    );
    // Pod-local scratch copy of the run record: set_stage/set_status write it
    // so the shared stage code runs unchanged; the AUTHORITATIVE record lives
    // on the orchestrator, fed by the push-back events.
    let mut run = Run {
        run_id: run_id.clone(),
        spec,
        status: RunStatus::Pending,
        recording_id: None,
        candidate_image: None,
        failure_reason: None,
        stage: Some("queued".to_owned()),
        step: 0,
        steps_total: 0,
        stage_updated_ms: timekettle_orchestrator::now_ms(),
    };
    timekettle_orchestrator::write_json(&root.run_path(&run_id), &run)
        .map_err(|e| format!("persist local run record: {e}"))?;

    let ctx = StoreCtx::http(&run_id, &base, token.as_deref(), &actor);
    eprintln!("timekettle-runner: run {run_id} starting (push-back → {base})");
    match drive_replay_in_pod(&root, &mut run, &ctx, &opts) {
        Ok(()) => {
            ctx.finish(true, None);
            eprintln!("timekettle-runner: run {run_id} completed");
            Ok(())
        }
        Err(e) => {
            ctx.log("failure", &e);
            ctx.finish(false, Some(&e));
            Err(e)
        }
    }
}
