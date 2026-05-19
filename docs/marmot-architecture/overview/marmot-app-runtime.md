---
title: "Marmot App Runtime Shape"
created: 2026-05-19
updated: 2026-05-19
tags: [marmot, overview, app-runtime, daemon, tui]
status: design
---

# Marmot App Runtime Shape

`marmot-app` should become the runtime boundary for client applications. A daemon, TUI, Flutter app, or desktop app
should create a `MarmotAppRuntime` and send it intents. The runtime owns accounts, shared directory state, relay
subscriptions, projections, and typed event streams.

The lower crates already point in this direction. `cgka-engine` owns convergence and engine state. `cgka-session` owns
one account-device session. `marmot-account` coordinates a session with transport and publish confirmation.
`marmot-app` is the place where those pieces become a product runtime.

## Shape

```text
MarmotAppRuntime
  SharedServices
    account home
    shared directory cache
    Nostr relay plane
    app event hubs
    projection stores
    background maintenance tasks
  AccountManager
    MarmotAccountSession(account A)
    MarmotAccountSession(account B)
    ...
```

`MarmotAccountSession` should wrap the current account-device stack:

- account identity and secret access;
- `AccountDeviceSession`;
- per-account app projection state;
- account-scoped group, message, profile, key-package, and stream operations;
- account inbox subscription state.

`SharedServices` should hold runtime state shared by every account:

- directory/user cache;
- relay-list and KeyPackage lookup state;
- app-wide stream hubs;
- app settings and runtime config;
- the Nostr relay plane.

## Relay Plane And Adapter

`transport-nostr-adapter` remains the reusable transport mechanism. It knows how to express Marmot transport messages as
Nostr events, how to peel inbound Nostr events into transport deliveries, how to publish to endpoint sets, and how to
manage low-level relay client calls behind an injectable boundary.

The Nostr relay plane should live inside `marmot-app` as a module or subsystem. It is the runtime owner for Nostr
subscriptions and relay policy:

- keep discovery and directory subscriptions coalesced across accounts;
- keep account inbox subscriptions for signed-in accounts;
- keep group subscriptions for account/group routes;
- route inbound relay events to the right `MarmotAccountSession`;
- apply reconnect, catch-up, replay-window, and relay-safety policy;
- emit typed runtime events after account sessions ingest deliveries.

The adapter is transport plumbing. The relay plane is app-runtime orchestration.

## Daemon Boundary

`dmd` should host one `MarmotAppRuntime`. It should accept socket requests, pass intents into the runtime, and stream
runtime events back to clients.

Daemon responsibilities stay narrow: process lifecycle, socket protocol, request routing, and stream fanout.

## CLI And TUI Boundary

`dm` and the TUI should be thin clients of the daemon/runtime:

- command calls become runtime intents;
- subscription calls attach to runtime broadcast streams;
- initial UI state comes from runtime snapshots;
- live updates come from runtime events;
- `sync` can remain as a diagnostic or repair command, while normal chat and stream flows work without it.

Agent stream previews belong in the same message subscription stream as other message updates. They should appear as
typed updates such as `agent_stream_start`, `agent_stream_delta`, `stream_preview`, and `agent_stream_final`.

## What To Borrow From whitenoise-rs

The useful pattern in `whitenoise-rs` is the app runtime shape:

- one runtime object holds shared services and an account manager;
- each account has a session with scoped operations;
- relay control is a shared runtime service;
- inbound relay events flow through one processing path;
- stream managers use broadcast channels keyed by account, group, or user;
- subscription APIs return an initial snapshot plus a live receiver.

Darkmatter should copy the shape and leave the legacy weight behind. The existing Darkmatter crates give us cleaner
engine/session boundaries and per-account persistence. The runtime work should build on those crates.

## First Vertical Slice

The first slice should prove the architecture without filling in every product surface:

1. Add `MarmotAppRuntime::open`, `start`, and `shutdown`.
2. Restore local accounts into an `AccountManager`.
3. Move identity creation into runtime setup so relay lists and a fresh KeyPackage publish during `create_identity`.
4. Add a runtime event enum for group joins, messages, and agent stream updates.
5. Move live Nostr receive/ingest into `marmot-app`.
6. Change `dmd` to hold one runtime and forward requests into it.
7. Make `messages subscribe` use runtime snapshot plus broadcast receiver.
8. Keep the TUI as a daemon client.

The acceptance test should create Alice and Bob, create a group, receive Bob's group join, send a normal message, start
an agent stream, receive stream deltas, and finish the stream without calling `dm sync` or publishing keys manually.

## Crate Responsibility After The Refactor

- `crates/traits`: shared transport, engine, app payload, and event types.
- `crates/cgka-engine`: OpenMLS-backed engine and convergence.
- `crates/cgka-session`: one encrypted account-device session.
- `crates/marmot-account`: account/session coordinator for publish-before-apply and transport activation.
- `crates/marmot-app`: multi-account runtime, shared services, relay plane, projections, app events.
- `crates/transport-nostr-adapter`: reusable Nostr transport adapter and SDK bridge.
- `crates/transport-nostr-peeler`: Nostr event peeling and wrapping.
- `crates/transport-quic-stream`: transient agent text stream transport.
- `crates/transport-quic-broker`: memory-only local broker for transient stream records.
- `crates/cli`: daemon host, CLI client, and TUI client.
