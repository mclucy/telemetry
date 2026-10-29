//! Never persist request metadata or log payloads.

mod cleanup;
mod config;
mod encoding;
mod ingest;
mod models;
mod privacy;
mod queue_consumer;
mod retrieval;
mod sql;
mod util;

use worker::{
    console_error, event, Context, Env, MessageBatch, Method, Request, Response, Result,
    ScheduleContext, ScheduledEvent,
};

const SERVICE_NAME: &str = "lucy-telemetry";

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let method = request.method();
    let path = request.path();
    match dispatch(&mut request, &env, method, &path).await {
        Ok(response) => Ok(response),
        Err(error) => {
            console_error!("request_failed class={}", util::error_kind(&error));
            util::internal_error()
        }
    }
}

#[event(queue)]
pub async fn queue(batch: MessageBatch<serde_json::Value>, env: Env, _ctx: Context) -> Result<()> {
    queue_consumer::handle(batch, &env).await
}

#[event(scheduled)]
pub async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    if let Err(error) = cleanup::handle(&env).await {
        console_error!(
            "retention_cleanup_failed class={}",
            util::error_kind(&error)
        );
    }
}

async fn dispatch(
    request: &mut Request,
    env: &Env,
    method: Method,
    path: &str,
) -> Result<Response> {
    if path == "/health" {
        if method != Method::Get {
            return util::method_not_allowed("GET");
        }
        return util::json_response(
            200,
            &serde_json::json!({
                "status": "ok",
                "service": SERVICE_NAME,
            }),
        );
    }

    if path == "/v1/records" {
        if method != Method::Post {
            return util::method_not_allowed("POST");
        }
        return ingest::handle(request, env).await;
    }

    if path.starts_with("/v1/") {
        return retrieval::handle(request, env, method, path).await;
    }

    util::error_response(404, "not_found")
}
