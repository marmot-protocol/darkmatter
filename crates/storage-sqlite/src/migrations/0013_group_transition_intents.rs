use crate::SqliteResultExt;
use cgka_traits::storage::StorageResult;
use rusqlite::Transaction;

pub(crate) fn apply(tx: &Transaction<'_>) -> StorageResult<()> {
    tx.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS cgka_group_transition_intents (
    group_id BLOB PRIMARY KEY REFERENCES cgka_groups(id) ON DELETE CASCADE,
    snapshot_name TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
    FOREIGN KEY (group_id, snapshot_name)
        REFERENCES cgka_group_snapshots(group_id, name)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cgka_group_transition_intents_created
    ON cgka_group_transition_intents (created_at_unix_seconds, snapshot_name);
"#,
    )
    .storage()
}
