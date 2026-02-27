/**
 * Rust Live Text-to-Speech Starter - Backend Server
 *
 * Simple WebSocket proxy to Deepgram's Live TTS API using Axum.
 * Forwards all messages (JSON and binary) bidirectionally between client and Deepgram.
 *
 * Routes:
 *   GET  /api/session                - Issue JWT session token
 *   WS   /api/live-text-to-speech    - WebSocket proxy to Deepgram TTS (auth required)
 *   GET  /api/metadata               - Project metadata from deepgram.toml
 *   GET  /health                     - Health check
 */

// ============================================================================
// DEPENDENCIES
// ============================================================================

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite};
use tower_http::cors::{Any, CorsLayer};

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Application configuration loaded from environment variables.
#[derive(Clone)]
struct Config {
    deepgram_api_key: String,
    deepgram_tts_url: String,
    port: u16,
    host: String,
    session_secret: String,
}

/// Loads configuration from environment variables with sensible defaults.
fn load_config() -> Config {
    // Load .env file (optional, won't error if missing)
    let _ = dotenvy::dotenv();

    let deepgram_api_key = env::var("DEEPGRAM_API_KEY").unwrap_or_else(|_| {
        eprintln!("ERROR: DEEPGRAM_API_KEY environment variable is required");
        eprintln!("Please copy sample.env to .env and add your API key");
        std::process::exit(1);
    });

    let port = env::var("PORT")
        .unwrap_or_else(|_| "8081".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid number");

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

    let session_secret = env::var("SESSION_SECRET").unwrap_or_else(|_| {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect();
        hex::encode(bytes)
    });

    Config {
        deepgram_api_key,
        deepgram_tts_url: "wss://api.deepgram.com/v1/speak".to_string(),
        port,
        host,
        session_secret,
    }
}

// ============================================================================
// SESSION AUTH - JWT tokens for production security
// ============================================================================

const JWT_EXPIRY_SECS: u64 = 3600; // 1 hour

/// Creates a signed JWT for session authentication.
fn generate_token(secret: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now();
    let claims = serde_json::json!({
        "iat": now.timestamp(),
        "exp": now.timestamp() + JWT_EXPIRY_SECS as i64,
    });

    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// Verifies a JWT and returns an error if invalid.
fn validate_token(token: &str, secret: &str) -> Result<(), jsonwebtoken::errors::Error> {
    let mut validation = jsonwebtoken::Validation::default();
    validation.required_spec_claims.clear();
    validation.validate_exp = true;

    jsonwebtoken::decode::<serde_json::Value>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?;

    Ok(())
}

/// Extracts and validates a JWT from WebSocket subprotocols.
/// Returns the full protocol string (e.g., "access_token.<jwt>") if valid.
fn validate_ws_token(protocols: &str, secret: &str) -> Option<String> {
    for protocol in protocols.split(',').map(|p| p.trim()) {
        if let Some(token) = protocol.strip_prefix("access_token.") {
            if validate_token(token, secret).is_ok() {
                return Some(protocol.to_string());
            }
        }
    }
    None
}

// ============================================================================
// METADATA
// ============================================================================

/// Represents the parsed deepgram.toml structure.
#[derive(Deserialize)]
struct DeepgramToml {
    meta: Option<toml::Value>,
}

// ============================================================================
// QUERY PARAMETERS
// ============================================================================

/// Query parameters for the TTS WebSocket endpoint.
#[derive(Deserialize)]
struct TtsParams {
    model: Option<String>,
    encoding: Option<String>,
    sample_rate: Option<String>,
    container: Option<String>,
}

// ============================================================================
// HTTP HANDLERS
// ============================================================================

/// Issues a signed JWT for session authentication.
async fn handle_session(State(config): State<Arc<Config>>) -> impl IntoResponse {
    match generate_token(&config.session_secret) {
        Ok(token) => {
            let body = serde_json::json!({ "token": token });
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(_) => {
            let body = serde_json::json!({
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Failed to generate token"
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
        }
    }
}

/// Returns project metadata from deepgram.toml.
async fn handle_metadata() -> impl IntoResponse {
    let content = match std::fs::read_to_string("deepgram.toml") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error reading deepgram.toml: {}", e);
            let body = serde_json::json!({
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Failed to read metadata from deepgram.toml"
            });
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
        }
    };

    let config: DeepgramToml = match toml::from_str(&content) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error parsing deepgram.toml: {}", e);
            let body = serde_json::json!({
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Failed to parse metadata from deepgram.toml"
            });
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
        }
    };

    match config.meta {
        Some(meta) => {
            let json_value = toml_to_json(meta);
            (StatusCode::OK, Json(json_value)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Missing [meta] section in deepgram.toml"
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
        }
    }
}

/// Health check endpoint.
async fn handle_health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

// ============================================================================
// WEBSOCKET PROXY
// ============================================================================

/// Handles the WebSocket upgrade request and initiates the proxy.
async fn handle_live_tts(
    State(config): State<Arc<Config>>,
    Query(params): Query<TtsParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Extract subprotocols from the upgrade request headers
    // Axum passes them through the WebSocketUpgrade
    ws.protocols(["access_token"])
        .on_upgrade(move |socket| async move {
            proxy_tts_websocket(socket, config, params).await;
        })
}

/// Handles the WebSocket upgrade with subprotocol validation.
async fn handle_live_tts_with_auth(
    State(config): State<Arc<Config>>,
    Query(params): Query<TtsParams>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Validate JWT from subprotocol header
    let protocols_header = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let valid_protocol = match validate_ws_token(protocols_header, &config.session_secret) {
        Some(p) => p,
        None => {
            eprintln!("WebSocket auth failed: invalid or missing token");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Upgrade with the accepted subprotocol
    ws.protocols([valid_protocol.clone()])
        .on_upgrade(move |socket| async move {
            proxy_tts_websocket(socket, config, params).await;
        })
}

/// Proxies WebSocket messages between the client and Deepgram's Live TTS API.
async fn proxy_tts_websocket(client_ws: WebSocket, config: Arc<Config>, params: TtsParams) {
    println!("Client connected to /api/live-text-to-speech");

    // Parse query parameters with defaults
    let model = params.model.unwrap_or_else(|| "aura-asteria-en".to_string());
    let encoding = params.encoding.unwrap_or_else(|| "linear16".to_string());
    let sample_rate = params.sample_rate.unwrap_or_else(|| "24000".to_string());
    let container = params.container.unwrap_or_else(|| "none".to_string());

    // Build Deepgram WebSocket URL with query parameters
    let deepgram_url = format!(
        "{}?model={}&encoding={}&sample_rate={}&container={}",
        config.deepgram_tts_url, model, encoding, sample_rate, container
    );

    println!(
        "Connecting to Deepgram TTS: model={}, encoding={}, sample_rate={}",
        model, encoding, sample_rate
    );

    // Create WebSocket connection to Deepgram with auth header
    let request = tungstenite::http::Request::builder()
        .uri(&deepgram_url)
        .header("Authorization", format!("Token {}", config.deepgram_api_key))
        .header("Host", "api.deepgram.com")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .expect("Failed to build Deepgram WebSocket request");

    let (deepgram_ws, _response) = match connect_async(request).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Deepgram connection failed: {}", e);
            let (mut sender, _) = client_ws.split();
            let _ = sender
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1011,
                    reason: "Deepgram connection failed".into(),
                })))
                .await;
            return;
        }
    };

    println!("Connected to Deepgram TTS API");

    // Split both WebSocket connections for bidirectional forwarding
    let (mut client_sender, mut client_receiver) = client_ws.split();
    let (mut deepgram_sender, mut deepgram_receiver) = deepgram_ws.split();

    // Forward messages: Deepgram -> Client
    let deepgram_to_client = tokio::spawn(async move {
        while let Some(msg) = deepgram_receiver.next().await {
            match msg {
                Ok(tungstenite::Message::Binary(data)) => {
                    if client_sender.send(Message::Binary(data.into())).await.is_err() {
                        eprintln!("Error forwarding binary to client");
                        break;
                    }
                }
                Ok(tungstenite::Message::Text(text)) => {
                    if client_sender.send(Message::Text(text.to_string().into())).await.is_err() {
                        eprintln!("Error forwarding text to client");
                        break;
                    }
                }
                Ok(tungstenite::Message::Close(frame)) => {
                    let close_frame = frame.map(|f| axum::extract::ws::CloseFrame {
                        code: f.code.into(),
                        reason: f.reason.to_string().into(),
                    });
                    let _ = client_sender.send(Message::Close(close_frame)).await;
                    println!("Deepgram connection closed normally");
                    break;
                }
                Ok(tungstenite::Message::Ping(data)) => {
                    let _ = client_sender.send(Message::Ping(data.into())).await;
                }
                Ok(tungstenite::Message::Pong(data)) => {
                    let _ = client_sender.send(Message::Pong(data.into())).await;
                }
                Ok(_) => {} // Ignore Frame messages
                Err(e) => {
                    eprintln!("Deepgram read error: {}", e);
                    let _ = client_sender
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1000,
                            reason: "Deepgram disconnected".into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    });

    // Forward messages: Client -> Deepgram
    let client_to_deepgram = tokio::spawn(async move {
        while let Some(msg) = client_receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if deepgram_sender
                        .send(tungstenite::Message::Binary(data.into()))
                        .await
                        .is_err()
                    {
                        eprintln!("Error forwarding binary to Deepgram");
                        break;
                    }
                }
                Ok(Message::Text(text)) => {
                    if deepgram_sender
                        .send(tungstenite::Message::Text(text.to_string().into()))
                        .await
                        .is_err()
                    {
                        eprintln!("Error forwarding text to Deepgram");
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    let _ = deepgram_sender
                        .send(tungstenite::Message::Close(Some(
                            tungstenite::protocol::CloseFrame {
                                code: tungstenite::protocol::frame::coding::CloseCode::Normal,
                                reason: "Client disconnected".into(),
                            },
                        )))
                        .await;
                    println!("Client disconnected normally");
                    break;
                }
                Ok(Message::Ping(data)) => {
                    let _ = deepgram_sender
                        .send(tungstenite::Message::Ping(data.into()))
                        .await;
                }
                Ok(Message::Pong(data)) => {
                    let _ = deepgram_sender
                        .send(tungstenite::Message::Pong(data.into()))
                        .await;
                }
                Err(e) => {
                    eprintln!("Client read error: {}", e);
                    let _ = deepgram_sender
                        .send(tungstenite::Message::Close(Some(
                            tungstenite::protocol::CloseFrame {
                                code: tungstenite::protocol::frame::coding::CloseCode::Normal,
                                reason: "Client disconnected".into(),
                            },
                        )))
                        .await;
                    break;
                }
            }
        }
    });

    // Wait for either direction to finish
    tokio::select! {
        _ = deepgram_to_client => {},
        _ = client_to_deepgram => {},
    }

    println!("WebSocket proxy session ended");
}

// ============================================================================
// HELPERS
// ============================================================================

/// Converts a TOML value to a serde_json value.
fn toml_to_json(value: toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(s) => serde_json::Value::String(s),
        toml::Value::Integer(i) => serde_json::json!(i),
        toml::Value::Float(f) => serde_json::json!(f),
        toml::Value::Boolean(b) => serde_json::Value::Bool(b),
        toml::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(toml_to_json).collect())
        }
        toml::Value::Table(table) => {
            let map: serde_json::Map<String, serde_json::Value> = table
                .into_iter()
                .map(|(k, v)| (k, toml_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        toml::Value::Datetime(dt) => serde_json::Value::String(dt.to_string()),
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() {
    let config = load_config();
    let addr = format!("{}:{}", config.host, config.port);
    let config = Arc::new(config);

    // CORS middleware
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build router
    let app = Router::new()
        .route("/api/session", get(handle_session))
        .route("/api/live-text-to-speech", get(handle_live_tts_with_auth))
        .route("/api/metadata", get(handle_metadata))
        .route("/health", get(handle_health))
        .layer(cors)
        .with_state(config.clone());

    let separator = "=".repeat(70);
    println!("{}", separator);
    println!(
        "Backend API Server running at http://localhost:{}",
        config.port
    );
    println!();
    println!("GET  /api/session");
    println!("WS   /api/live-text-to-speech (auth required)");
    println!("GET  /api/metadata");
    println!("GET  /health");
    println!("{}", separator);

    // Start server
    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind to address");

    // Graceful shutdown on SIGINT/SIGTERM
    let shutdown_signal = async {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => println!("\nSIGINT received: starting graceful shutdown..."),
            _ = sigterm.recv() => println!("\nSIGTERM received: starting graceful shutdown..."),
        }

        println!("Shutdown complete");
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .expect("Server failed");
}
