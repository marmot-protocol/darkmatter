# marmot-c

C bindings for the Marmot app runtime — a stable, minimal C ABI over
[`marmot-app`](../marmot-app) for consumers that cannot pull in a UniFFI
runtime: embedded targets, C/C++ apps, and FFI from languages without UniFFI
support (Zig, Nim, Go, Lua, raw FFI, …).

This is the C-surface counterpart to [`marmot-uniffi`](../marmot-uniffi) (which
serves Swift/iOS and Kotlin/Android). Both wrap the same `MarmotApp` +
`MarmotAppRuntime` pair; this crate exposes it through a C ABI instead of
UniFFI.

## ABI shape

The exported surface is deliberately tiny so the hand-curated header stays
auditable and the ABI stays stable as the runtime grows:

| Symbol | Purpose |
| --- | --- |
| `marmot_c_open` / `marmot_c_free` | construct / destroy the opaque `MarmotC *` handle (owns its own tokio runtime) |
| `marmot_c_start` / `marmot_c_shutdown` / `marmot_c_is_stopping` | runtime lifecycle |
| `marmot_c_call` | single JSON command entrypoint: `(method, request_json) -> response_json` |
| `marmot_c_string_free` | release any `char *` the library returned |

Runtime capabilities are added on the Rust side as new **dispatch methods**
(`src/dispatch.rs`), not new exported functions, so adding a method never
changes the ABI. The authoritative header is hand-curated at
[`include/marmot.h`](include/marmot.h); the method catalogue lives there and in
the `dispatch` module docs.

### Why JSON-over-one-entrypoint

The UniFFI surface is ~70 async methods plus live subscription objects.
Mirroring every one as a typed `extern "C"` function would be a large,
churn-prone ABI. Instead the C ABI keeps a fixed symbol set and marshals
structured request/response bodies as JSON — most runtime records already derive
`serde::Serialize`, so the response bodies are their canonical JSON. The async
runtime is driven synchronously: each call `block_on`s the handle's owned tokio
runtime.

> Note: live subscriptions (the UniFFI `*Subscription` objects driven by
> `next()`) are not yet exposed over this ABI — the first cut covers the
> request/response surface (account/session, group ops, message send/receive,
> agent-stream anchor, timeline/chat-list storage reads). A callback- or
> poll-based subscription bridge is a natural follow-up.

## Build

```sh
cargo build -p marmot-c --release
```

Produces, under `target/release/`:

- `libmarmot_c.so` / `libmarmot_c.dylib` / `marmot_c.dll` — dynamic (`cdylib`)
- `libmarmot_c.a` — static (`staticlib`)

The C header is checked in at `crates/marmot-c/include/marmot.h`. It is
hand-curated; to regenerate/verify it against the Rust source with
[cbindgen](https://github.com/mozilla/cbindgen):

```sh
cbindgen --lang c crates/marmot-c/src/lib.rs
```

The generated output must match the checked-in header's signatures and
constants (only formatting/comment differences are expected).

## Linking

### Minimal example

```c
#include "marmot.h"
#include <stdio.h>

int main(void) {
    MarmotC *kit = NULL;
    char *err = NULL;
    if (marmot_c_open("/path/to/root", "wss://relay.example\n", &kit, &err) != MARMOT_C_STATUS_OK) {
        fprintf(stderr, "open failed: %s\n", err ? err : "(no message)");
        marmot_c_string_free(err);
        return 1;
    }

    char *resp = NULL;
    if (marmot_c_call(kit, "account.list", "{}", &resp, &err) == MARMOT_C_STATUS_OK) {
        printf("accounts: %s\n", resp);   /* e.g. "[]" */
        marmot_c_string_free(resp);
    } else {
        fprintf(stderr, "call failed: %s\n", err ? err : "(no message)");
        marmot_c_string_free(err);
    }

    marmot_c_free(kit);
    return 0;
}
```

Build (dynamic link):

```sh
cc app.c -I crates/marmot-c/include -L target/release -lmarmot_c -o app
```

Static link also needs the system libraries the runtime pulls in (TLS, SQLite,
etc.); on Linux that is typically:

```sh
cc app.c -I crates/marmot-c/include \
   target/release/libmarmot_c.a \
   -lpthread -ldl -lm -o app
```

### pkg-config

A template `marmot-c.pc.in` is provided. Substitute `@PREFIX@` and `@VERSION@`
for an install prefix:

```sh
sed -e "s|@PREFIX@|/usr/local|" -e "s|@VERSION@|0.1.0|" \
    crates/marmot-c/marmot-c.pc.in > /usr/local/lib/pkgconfig/marmot-c.pc
```

then:

```sh
cc app.c $(pkg-config --cflags --libs marmot-c) -o app
```

### CMake

```cmake
# Point these at your build/install layout.
add_library(marmot_c SHARED IMPORTED)
set_target_properties(marmot_c PROPERTIES
    IMPORTED_LOCATION       "${MARMOT_C_DIR}/lib/libmarmot_c.so"
    INTERFACE_INCLUDE_DIRECTORIES "${MARMOT_C_DIR}/include")

add_executable(app app.c)
target_link_libraries(app PRIVATE marmot_c)
```

## Memory & threading contract

- Every `char *` returned through an out-parameter (responses and error
  messages) is owned by the caller and **must** be released with
  `marmot_c_string_free()` — never `free(3)`.
- A handle may be used from multiple threads; each call blocks the calling
  thread until the underlying async work completes on the owned tokio runtime.
- All entrypoints are null-checked and return `MARMOT_C_STATUS_INVALID_ARGUMENT`
  rather than dereferencing a null pointer.

## Prior art

`marmot-protocol/mdk-c` exposes the MDK core through an analogous C surface;
this crate brings the same shape to the Dark Matter app runtime.
