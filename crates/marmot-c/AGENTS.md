# AGENTS.md - marmot-c

C bindings for the Marmot app runtime. The C-surface counterpart to
`marmot-uniffi`: both wrap the same `MarmotApp` + `MarmotAppRuntime` pair, but
this crate exposes a stable C ABI instead of UniFFI.

Read `README.md` for the human-facing overview and packaging/linking notes.

## Scope

- A tiny, stable C ABI: opaque `MarmotC *` handle + a single JSON command
  entrypoint (`marmot_c_call`). The exported symbol set does not grow as runtime
  features are added.
- An owned tokio runtime per handle; async runtime calls are driven with
  `block_on`.
- A hand-curated C header (`include/marmot.h`) that is the authoritative ABI.

## Layout

| File | Purpose |
| --- | --- |
| `src/lib.rs` | `extern "C"` entrypoints, pointer/UTF-8 safety, CString ownership, FFI tests |
| `src/runtime.rs` | `MarmotC` handle, owned tokio runtime, `MarmotCError` + status codes |
| `src/dispatch.rs` | method catalogue: request/response DTOs and the `dispatch` match |
| `include/marmot.h` | hand-curated authoritative C header |
| `marmot-c.pc.in` | pkg-config template |

## Invariants

- Keep the exported symbol set stable. Add capabilities as new `dispatch`
  methods (new `match` arms + DTOs), not new exported functions.
- Keep request/response DTOs explicit in `dispatch.rs`. Serialize internal
  records directly only when they already derive `Serialize` and are a stable
  projection; otherwise define a local DTO so the JSON contract cannot silently
  drift.
- Every `char *` handed across the ABI is owned by the caller and freed with
  `marmot_c_string_free`. Never return a borrowed or static pointer.
- Null-check every pointer argument and return `MARMOT_C_STATUS_INVALID_ARGUMENT`
  rather than dereferencing.
- Keep the status-code constants in `src/lib.rs`, `MarmotCError::code`, and
  `include/marmot.h` in sync.
- When `include/marmot.h` changes, verify it against `cbindgen --lang c
  src/lib.rs` (signatures + constants must match; formatting may differ).

## Verification

```sh
cargo fmt -p marmot-c --check
RUSTFLAGS='-D warnings' cargo check -p marmot-c --all-targets --locked
cargo clippy -p marmot-c --all-targets --locked -- -D warnings
cargo test -p marmot-c --locked
```

The FFI smoke tests install an in-memory mock keyring (`keyring_core::mock`)
before constructing a handle, mirroring `marmot-uniffi`'s smoke test, so the
real constructor path runs in headless CI without a platform secret service.
