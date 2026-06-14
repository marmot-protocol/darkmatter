use crate::{SqliteAccountStorage, SqliteResultExt};
use cgka_traits::storage::{GroupTransitionIntent, MessageStorage, StorageError, StorageResult};
use cgka_traits::types::GroupId;
use rusqlite::params;

pub(super) fn record(
    store: &SqliteAccountStorage,
    group_id: &GroupId,
    snapshot_name: &str,
) -> StorageResult<()> {
    store
        .lock()?
        .execute(
            "INSERT INTO cgka_group_transition_intents (
                group_id, snapshot_name, created_at_unix_seconds
             )
             VALUES (?1, ?2, CAST(strftime('%s', 'now') AS INTEGER))
             ON CONFLICT(group_id) DO UPDATE SET
                snapshot_name = excluded.snapshot_name,
                created_at_unix_seconds = excluded.created_at_unix_seconds",
            params![group_id.as_slice(), snapshot_name],
        )
        .storage()?;
    Ok(())
}

pub(super) fn clear(
    store: &SqliteAccountStorage,
    group_id: &GroupId,
    snapshot_name: &str,
) -> StorageResult<()> {
    store
        .lock()?
        .execute(
            "DELETE FROM cgka_group_transition_intents
             WHERE group_id = ?1 AND snapshot_name = ?2",
            params![group_id.as_slice(), snapshot_name],
        )
        .storage()?;
    Ok(())
}

pub(super) fn list(store: &SqliteAccountStorage) -> StorageResult<Vec<GroupTransitionIntent>> {
    let conn = store.lock()?;
    let mut stmt = conn
        .prepare(
            "SELECT group_id, snapshot_name
             FROM cgka_group_transition_intents
             ORDER BY created_at_unix_seconds, snapshot_name",
        )
        .storage()?;
    let rows = stmt
        .query_map([], |row| {
            Ok(GroupTransitionIntent {
                group_id: GroupId::new(row.get::<_, Vec<u8>>(0)?),
                snapshot_name: row.get(1)?,
            })
        })
        .storage()?
        .collect::<Result<Vec<_>, _>>()
        .storage()?;
    Ok(rows)
}

pub(crate) fn recover_all(store: &SqliteAccountStorage) -> StorageResult<()> {
    let intents = list(store)?;
    for intent in intents {
        store.rollback_group_to_snapshot(&intent.group_id, &intent.snapshot_name)?;
        clear(store, &intent.group_id, &intent.snapshot_name)?;
        match store.release_group_snapshot(&intent.group_id, &intent.snapshot_name) {
            Ok(()) | Err(StorageError::SnapshotMissing(_)) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}
