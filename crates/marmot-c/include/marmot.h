/*
 * marmot.h — stable C ABI for the Marmot runtime.
 *
 * This header is the hand-curated, authoritative C surface for the `marmot-c`
 * crate (libmarmot_c). It is intentionally small: an opaque handle plus a
 * single JSON command entrypoint. Runtime capabilities are added on the Rust
 * side as new dispatch methods (see "Method catalogue" below) without changing
 * the exported symbol set, so this ABI stays stable across feature growth.
 *
 * Link against `libmarmot_c` (cdylib) or `libmarmot_c.a` (staticlib). See
 * crates/marmot-c/README.md for packaging (pkg-config / CMake) notes.
 *
 * Thread-safety: a handle may be used from multiple threads. Each call drives
 * async work to completion on the handle's owned tokio runtime and blocks the
 * calling thread until it returns.
 *
 * Memory: every `char *` returned through an out-parameter (responses and
 * error messages) is owned by the caller and MUST be released with
 * marmot_c_string_free(). Do not call free(3) on it.
 */
#ifndef MARMOT_H
#define MARMOT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Status codes returned by the entrypoints below. */
#define MARMOT_C_STATUS_OK 0               /* success */
#define MARMOT_C_STATUS_INVALID_ARGUMENT 1 /* null pointer or non-UTF-8 string */
#define MARMOT_C_STATUS_UNKNOWN_METHOD 2   /* unknown dispatch method name */
#define MARMOT_C_STATUS_JSON 3             /* request/response JSON error */
#define MARMOT_C_STATUS_RUNTIME 4          /* marmot runtime error */

/* Opaque runtime handle. Construct with marmot_c_open(), destroy with
 * marmot_c_free(). */
typedef struct MarmotC MarmotC;

/*
 * Open a Marmot runtime rooted at `root_path` (UTF-8 C string). `relay_urls`
 * is an optional newline-separated list of default relay URLs (may be NULL or
 * empty). On success returns MARMOT_C_STATUS_OK and writes the handle to
 * *handle_out. On failure returns a non-zero status, sets *handle_out to NULL,
 * and (when error_out != NULL) writes an owned error string to *error_out.
 */
int32_t marmot_c_open(const char *root_path,
                      const char *relay_urls,
                      MarmotC **handle_out,
                      char **error_out);

/*
 * Bring the runtime online (reconcile accounts, start workers, subscribe to
 * transport). Returns MARMOT_C_STATUS_OK or a non-zero status with an owned
 * *error_out message.
 */
int32_t marmot_c_start(MarmotC *handle, char **error_out);

/* Tear the runtime down. Does not free the handle. NULL is ignored. */
void marmot_c_shutdown(MarmotC *handle);

/* Returns 1 if the runtime is shutting down, 0 otherwise (or if handle is
 * NULL). */
int32_t marmot_c_is_stopping(const MarmotC *handle);

/*
 * Invoke a runtime command. `method` is a UTF-8 method name (see catalogue);
 * `request_json` is a UTF-8 JSON object (may be NULL or empty, treated as
 * "{}"). On success returns MARMOT_C_STATUS_OK and writes an owned JSON
 * response string to *response_out. On failure returns a non-zero status, sets
 * *response_out to NULL, and (when error_out != NULL) writes an owned error
 * string to *error_out.
 *
 * Both *response_out and *error_out (when set) must be released with
 * marmot_c_string_free().
 *
 * Method catalogue (request -> response JSON):
 *   account.list            {}                                   -> [AccountInfo]
 *   account.create_identity {default_relays?, bootstrap_relays?} -> AccountInfo
 *   account.login           {identity, default_relays?, bootstrap_relays?} -> AccountInfo
 *   account.remove          {account_ref}                        -> null
 *   group.create            {account_ref, name, member_refs?, description?} -> {group_id_hex}
 *   group.list              {account_ref}                        -> [AppGroupRecord]
 *   group.members           {account_ref, group_id_hex}          -> [AppGroupMemberRecord]
 *   group.mls_state         {account_ref, group_id_hex}          -> AppGroupMlsState
 *   group.invite_members    {account_ref, group_id_hex, member_refs} -> {published, message_ids}
 *   group.remove_members    {account_ref, group_id_hex, member_refs} -> {published, message_ids}
 *   message.send_text       {account_ref, group_id_hex, text}    -> {published, message_ids}
 *   message.list            {account_ref, group_id_hex?, limit?} -> [AppMessageRecord]
 *   timeline.list           {account_ref, group_id_hex?, limit?} -> TimelinePage
 *   chat.list               {account_ref, include_archived?}     -> [ChatListRow]
 *   agent_stream.start      {account_ref, group_id_hex, stream_id_hex?, quic_candidates?}
 *                                                                -> {stream_id_hex, published, message_ids}
 *
 * The structured record shapes (AccountInfo, AppGroupRecord, …) are the JSON
 * serializations of the corresponding marmot-app/storage records; see the Rust
 * docs for field-level detail.
 */
int32_t marmot_c_call(MarmotC *handle,
                      const char *method,
                      const char *request_json,
                      char **response_out,
                      char **error_out);

/* Free a char* previously returned by this library (response or error). NULL
 * is ignored. Do not call free(3) on these pointers. */
void marmot_c_string_free(char *ptr);

/* Free a handle returned by marmot_c_open(); implicitly shuts the runtime
 * down. NULL is ignored. */
void marmot_c_free(MarmotC *handle);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MARMOT_H */
