//! 会话列表与按需复制（路径 B：生成新 id，云端可正常同步）。
//!
//! 对照 server.py `current_user_uid` / `list_sessions_for_user` /
//! `_find_project_jsonl` / `copy_session_to_user` / `_register_edge_sync_mapping` /
//! `copy_sessions_for_switch` / `backup_workbuddy_db` / `workbuddy_db_path`。
//!
//! WorkBuddy 5.x 数据三件套（缺一不可）：
//!   1) 正文：`~/.workbuddy/projects/{workspace}/{cid}.jsonl`（JSONL 含 sessionId 字段）
//!   2) 元数据：`~/.workbuddy/workbuddy.db` sessions 表（id = conversation id = UUID）
//!   3) 云端映射：`~/.workbuddy/edge-sync-mapping-v2.db` edge_sync_mapping
//!      （session_id=conversation_id，msg_channel=convmsg:{uid} 决定云端归属）

use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::modules::auth_file;
use crate::modules::config::{
    backup_dir, home_dir, is_workbuddy_ai_account, now_ms, now_secs, utc_iso,
};

/// 会话数据所属的 WorkBuddy 发行版。认证、数据库、正文目录和 edge-sync
/// 必须始终使用同一发行版，不能拿国际版 UID 去查国内数据库。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionEdition {
    Cn,
    Ai,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionContext {
    pub uid: String,
    pub edition: SessionEdition,
}

impl SessionEdition {
    fn is_international(self) -> bool {
        matches!(self, Self::Ai)
    }
}

/// 打开数据库并设置 busy_timeout（对照 Python `sqlite3.connect(timeout=5)`）。
fn open_db(path: &Path, read_only: bool) -> Option<Connection> {
    let conn = if read_only {
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?
    } else {
        Connection::open(path).ok()?
    };
    let _ = conn.busy_timeout(Duration::from_secs(5));
    Some(conn)
}

fn edition_for_auth_root(root: &Value) -> SessionEdition {
    let domain = root
        .get("domain")
        .and_then(Value::as_str)
        .or_else(|| {
            root.get("auth")
                .and_then(|auth| auth.get("domain"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            root.get("account")
                .and_then(|account| account.get("domain"))
                .and_then(Value::as_str)
        });
    let probe = json!({"domain": domain});
    if is_workbuddy_ai_account(&probe) {
        SessionEdition::Ai
    } else {
        SessionEdition::Cn
    }
}

fn edition_for_account(account: &Value) -> SessionEdition {
    if is_workbuddy_ai_account(account) {
        SessionEdition::Ai
    } else {
        SessionEdition::Cn
    }
}

fn current_edition() -> SessionEdition {
    auth_file::read_auth_file()
        .map(|root| edition_for_auth_root(&root))
        .unwrap_or(SessionEdition::Cn)
}

fn data_root_for_edition(edition: SessionEdition) -> PathBuf {
    home_dir().join(if edition.is_international() {
        ".workbuddy-ai"
    } else {
        ".workbuddy"
    })
}

fn workbuddy_db_path_for_edition(edition: SessionEdition) -> PathBuf {
    data_root_for_edition(edition).join("workbuddy.db")
}

pub fn workbuddy_db_path() -> PathBuf {
    workbuddy_db_path_for_edition(current_edition())
}

fn projects_root_for_edition(edition: SessionEdition) -> PathBuf {
    data_root_for_edition(edition).join("projects")
}

fn destination_project_jsonl(
    source: &Path,
    source_edition: SessionEdition,
    target_edition: SessionEdition,
    new_cid: &str,
) -> Option<PathBuf> {
    let source_projects = projects_root_for_edition(source_edition);
    let relative = source.strip_prefix(source_projects).ok()?;
    let workspace = relative.parent()?;
    Some(
        projects_root_for_edition(target_edition)
            .join(workspace)
            .join(format!("{new_cid}.jsonl")),
    )
}

fn edge_sync_candidates(edition: SessionEdition) -> Vec<PathBuf> {
    let root = data_root_for_edition(edition);
    [
        "edge-sync-mapping-v4.db",
        "edge-sync-mapping-v3.db",
        "edge-sync-mapping-v2.db",
    ]
    .into_iter()
    .map(|name| root.join(name))
    .filter(|path| path.is_file())
    .collect()
}

/// 选择当前发行版实际存在的 edge-sync 数据库。
/// 新版本会保留旧版本文件，因此优先使用最近修改且包含目标账号映射的数据库。
fn edge_sync_db_path_for_edition(
    edition: SessionEdition,
    target_uid: Option<&str>,
) -> Option<PathBuf> {
    let mut candidates = edge_sync_candidates(edition);
    candidates.sort_by_key(|path| {
        std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::UNIX_EPOCH)
    });
    candidates.reverse();

    if let Some(uid) = target_uid {
        let channel = format!("convmsg:{uid}");
        if let Some(path) = candidates.iter().find(|path| {
            open_db(path, true)
                .and_then(|conn| {
                    conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM edge_sync_mapping WHERE msg_channel = ?1)",
                        [channel.as_str()],
                        |row| row.get::<_, i64>(0),
                    )
                    .ok()
                })
                .is_some_and(|exists| exists != 0)
        }) {
            return Some(path.clone());
        }
    }
    candidates.into_iter().next()
}

/// 当前认证账号的 UID 与发行版上下文；认证、数据库和正文目录必须保持一致。
pub fn current_session_context() -> Option<SessionContext> {
    let auth = auth_file::read_auth_file()?;
    let uid = auth
        .get("account")
        .and_then(|a| a.get("uid"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();
    Some(SessionContext {
        uid,
        edition: edition_for_auth_root(&auth),
    })
}

/// 当前认证账号的 uid（认证文件 account.uid）。
pub fn current_user_uid() -> Option<String> {
    current_session_context().map(|context| context.uid)
}

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or(0)
        == 1
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
    let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info({table})")) else {
        return false;
    };
    let Ok(iter) = stmt.query_map([], |row| row.get::<_, String>(1)) else {
        return false;
    };
    let names: Vec<String> = iter.flatten().collect();
    names.iter().any(|name| name == column)
}

fn nonempty_text(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// WorkBuddy 侧栏展示名：优先 custom_title（用户改名 / 定时任务名），否则 title。
fn session_display_title(title: Option<String>, custom_title: Option<String>) -> String {
    nonempty_text(custom_title)
        .or_else(|| nonempty_text(title))
        .unwrap_or_else(|| "(无标题)".to_string())
}

/// Claw 是账号绑定的 IM 渠道工作区，复制会话行不够，目标账号也用不了。
fn is_claw_workspace(cwd: &str) -> bool {
    cwd.trim()
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.eq_ignore_ascii_case("claw"))
}

/// 列出某账号未删除的会话（workbuddy.db sessions 表，db 为准）。
///
/// `title` 为 WorkBuddy 侧栏同款展示名；`isPlayground` 对应侧栏「任务」，
/// 其余按 `cwd` 最后一段归入「空间」。
pub fn list_sessions_for_user(uid: &str) -> Value {
    list_sessions_for_user_in_edition(uid, current_edition())
}

/// 返回当前认证账号的会话列表，并携带发行版诊断信息供 UI 区分“无会话”和“查错目录”。
pub fn list_current_sessions() -> Value {
    let Some(context) = current_session_context() else {
        return json!({
            "sessions": [],
            "current": Value::Null,
            "diagnostics": {"reason": "missing_current_account"},
        });
    };
    let db = workbuddy_db_path_for_edition(context.edition);
    let sessions = list_sessions_for_user_in_edition(&context.uid, context.edition);
    let reason = if !db.is_file() {
        "database_missing"
    } else if sessions.as_array().is_some_and(|items| items.is_empty()) {
        "no_sessions_for_uid"
    } else {
        "ok"
    };
    json!({
        "sessions": sessions,
        "current": context.uid,
        "edition": if context.edition.is_international() { "ai" } else { "cn" },
        "dbPath": db.to_string_lossy(),
        "diagnostics": {"reason": reason},
    })
}

fn list_sessions_for_user_in_edition(uid: &str, edition: SessionEdition) -> Value {
    let db = workbuddy_db_path_for_edition(edition);
    if !db.is_file() {
        return json!([]);
    }
    let Some(conn) = open_db(&db, true) else {
        return json!([]);
    };
    if !table_exists(&conn, "sessions") {
        return json!([]);
    }
    let has_custom = column_exists(&conn, "sessions", "custom_title");
    let has_playground = column_exists(&conn, "sessions", "is_playground");
    let sql = match (has_custom, has_playground) {
        (true, true) => {
            "SELECT id, cwd, title, custom_title, updated_at, is_playground FROM sessions \
             WHERE user_id = ?1 AND deleted_at IS NULL ORDER BY updated_at DESC"
        }
        (true, false) => {
            "SELECT id, cwd, title, custom_title, updated_at, 0 FROM sessions \
             WHERE user_id = ?1 AND deleted_at IS NULL ORDER BY updated_at DESC"
        }
        (false, true) => {
            "SELECT id, cwd, title, NULL, updated_at, is_playground FROM sessions \
             WHERE user_id = ?1 AND deleted_at IS NULL ORDER BY updated_at DESC"
        }
        (false, false) => {
            "SELECT id, cwd, title, NULL, updated_at, 0 FROM sessions \
             WHERE user_id = ?1 AND deleted_at IS NULL ORDER BY updated_at DESC"
        }
    };
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(_) => return json!([]),
    };
    let rows = stmt.query_map([uid], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
        ))
    });

    let mut sessions: Vec<Value> = Vec::new();
    if let Ok(iter) = rows {
        for r in iter.flatten() {
            let (cid, cwd, title, custom_title, updated_at, is_playground) = r;
            let cid = cid.unwrap_or_default();
            let cwd = cwd.unwrap_or_default();
            if is_claw_workspace(&cwd) {
                continue;
            }
            sessions.push(json!({
                "id": cid,
                "title": session_display_title(title, custom_title),
                "cwd": cwd,
                "updatedAt": updated_at.unwrap_or(0),
                "hasHistory": find_project_jsonl_in_edition(&cid, edition).is_some(),
                "isPlayground": is_playground.unwrap_or(0) != 0,
            }));
        }
    }
    json!(sessions)
}

/// 在当前发行版的 projects 目录中定位会话正文。
fn find_project_jsonl_in_edition(cid: &str, edition: SessionEdition) -> Option<PathBuf> {
    let projects = data_root_for_edition(edition).join("projects");
    if !projects.is_dir() {
        return None;
    }
    let direct = projects.join(format!("{cid}.jsonl"));
    if direct.is_file() {
        return Some(direct);
    }
    for entry in std::fs::read_dir(&projects).ok()?.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let p = entry.path().join(format!("{cid}.jsonl"));
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

/// 备份 workbuddy.db（含 -wal/-shm），返回主库备份路径。对照 `backup_workbuddy_db`。
fn backup_workbuddy_db_for_edition(backup_root: &Path, edition: SessionEdition) -> Option<PathBuf> {
    let db = workbuddy_db_path_for_edition(edition);
    if !db.is_file() {
        return None;
    }
    std::fs::create_dir_all(backup_root).ok()?;
    for suffix in ["", "-wal", "-shm"] {
        let src = PathBuf::from(format!("{}{}", db.to_string_lossy(), suffix));
        if src.is_file() {
            let _ = std::fs::copy(&src, backup_root.join(format!("workbuddy.db{suffix}")));
        }
    }
    Some(backup_root.join("workbuddy.db"))
}

/// 把 source_uid 的一个会话复制为 target_uid 的新会话（路径 B：生成新 id）。
///
/// 全部按「新 id」复制一份给目标账号，源账号数据完全不动。
/// 新 id 必须用带连字符的 UUID 格式（`Uuid::new_v4().to_string()`），与官方一致；
/// 32 位无连字符形式会导致 WorkBuddy 无法识别新会话。
pub fn copy_session_to_user(
    cid: &str,
    source_uid: &str,
    target_uid: &str,
) -> Result<Value, String> {
    let edition = current_edition();
    copy_session_to_user_for_editions(cid, source_uid, target_uid, edition, edition)
}

fn copy_session_to_user_for_editions(
    cid: &str,
    source_uid: &str,
    target_uid: &str,
    source_edition: SessionEdition,
    target_edition: SessionEdition,
) -> Result<Value, String> {
    let new_cid = uuid::Uuid::new_v4().to_string();
    let source_db = workbuddy_db_path_for_edition(source_edition);
    if let Some(conn) = open_db(&source_db, true) {
        let cwd: Option<String> = conn
            .query_row(
                "SELECT cwd FROM sessions WHERE id = ?1 AND user_id = ?2",
                rusqlite::params![cid, source_uid],
                |r| r.get(0),
            )
            .ok();
        if cwd.as_deref().is_some_and(is_claw_workspace) {
            return Err("Claw 工作区绑定当前账号渠道，不支持复制".into());
        }
    }

    // 1) 复制正文 jsonl：源发行版 projects/{workspace}/{cid}.jsonl → 目标发行版同 workspace/{new_cid}.jsonl
    let mut jsonl_copied = false;
    if let Some(src_jsonl) = find_project_jsonl_in_edition(cid, source_edition) {
        if let Some(dst_jsonl) =
            destination_project_jsonl(&src_jsonl, source_edition, target_edition, &new_cid)
        {
            if let Ok(text) = std::fs::read_to_string(&src_jsonl) {
                let text = text.replace(cid, &new_cid);
                if let Some(parent) = dst_jsonl.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if std::fs::write(&dst_jsonl, text).is_ok() {
                    jsonl_copied = true;
                }
            }
        }
    }

    // 2) 备份目标发行版 db（复制前），再 INSERT 新 sessions 行
    let backup_root = backup_dir().join("sessions").join(utc_iso());
    backup_workbuddy_db_for_edition(&backup_root, target_edition);
    let target_db = workbuddy_db_path_for_edition(target_edition);
    insert_session_copy_between(
        &source_db, &target_db, &new_cid, cid, source_uid, target_uid,
    )?;

    // 3) 注册云端映射：新会话归属目标账号（msg_channel=convmsg:{target_uid}）
    let mapping_written =
        register_edge_sync_mapping_for_edition(&new_cid, target_uid, target_edition);

    Ok(json!({
        "id": cid,
        "newId": new_cid,
        "jsonlCopied": jsonl_copied,
        "mappingWritten": mapping_written,
        "backup": backup_root.to_string_lossy().to_string(),
    }))
}

/// 在 workbuddy.db 中把源会话行复制为新 id（动态列，覆盖 id/user_id/时间戳）。
///
/// db 不存在或 sessions 表不存在时静默成功（对应 Python 版跳过）。源行不存在则无操作。
fn insert_session_copy_between(
    source_db_path: &Path,
    target_db_path: &Path,
    new_cid: &str,
    cid: &str,
    source_uid: &str,
    target_uid: &str,
) -> Result<(), String> {
    if !source_db_path.is_file() || !target_db_path.is_file() {
        return Ok(());
    }
    let Some(source_conn) = open_db(source_db_path, true) else {
        return Ok(());
    };
    if !table_exists(&source_conn, "sessions") {
        return Ok(());
    }
    let Some(target_conn) = open_db(target_db_path, false) else {
        return Ok(());
    };
    if !table_exists(&target_conn, "sessions") {
        return Ok(());
    }
    let mut src_stmt = source_conn
        .prepare("SELECT * FROM sessions WHERE id = ?1 AND user_id = ?2")
        .map_err(|e| e.to_string())?;
    let source_cols: Vec<String> = src_stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut rows = src_stmt
        .query(rusqlite::params![cid, source_uid])
        .map_err(|e| e.to_string())?;
    let Some(row) = rows.next().map_err(|e| e.to_string())? else {
        return Err(format!("未找到源会话: {cid}"));
    };
    let mut source_values = std::collections::HashMap::with_capacity(source_cols.len());
    for (i, col) in source_cols.iter().enumerate() {
        let value = row
            .get::<_, rusqlite::types::Value>(i)
            .unwrap_or(rusqlite::types::Value::Null);
        if col == "cwd" {
            if let rusqlite::types::Value::Text(ref path) = value {
                if is_claw_workspace(path) {
                    return Err("Claw 工作区绑定当前账号渠道，不支持复制".into());
                }
            }
        }
        source_values.insert(col.clone(), value);
    }
    drop(rows);
    drop(src_stmt);

    let target_stmt = target_conn
        .prepare("SELECT * FROM sessions LIMIT 0")
        .map_err(|e| e.to_string())?;
    let target_cols: Vec<String> = target_stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    drop(target_stmt);

    // 国内/国际版数据库的 sessions 列会随版本演进而不同；只写入目标库
    // 已存在且源库有值的列，遗漏的目标列交给 SQLite 默认值处理，避免跨库复制失败。
    let mut insert_cols = Vec::new();
    let mut vals = Vec::new();
    for col in target_cols {
        let value = match col.as_str() {
            "id" => Some(rusqlite::types::Value::Text(new_cid.to_string())),
            "user_id" => Some(rusqlite::types::Value::Text(target_uid.to_string())),
            "created_at" | "updated_at" => Some(rusqlite::types::Value::Integer(now_ms())),
            "deleted_at" => Some(rusqlite::types::Value::Null),
            _ => source_values.get(&col).cloned(),
        };
        let Some(value) = value else { continue };
        insert_cols.push(col);
        vals.push(value);
    }

    let placeholders = insert_cols
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let colnames = insert_cols.join(", ");
    let sql = format!("INSERT OR REPLACE INTO sessions ({colnames}) VALUES ({placeholders})");
    let params: Vec<&rusqlite::types::Value> = vals.iter().collect();
    target_conn
        .execute(&sql, rusqlite::params_from_iter(params))
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// 把新会话注册进 edge_sync_mapping（云端归属关键）。失败不致命，返回 False。
fn register_edge_sync_mapping_for_edition(
    new_cid: &str,
    target_uid: &str,
    edition: SessionEdition,
) -> bool {
    let Some(path) = edge_sync_db_path_for_edition(edition, Some(target_uid)) else {
        return false;
    };
    insert_edge_sync_mapping(&path, new_cid, target_uid)
}

fn insert_edge_sync_mapping(db_path: &Path, new_cid: &str, target_uid: &str) -> bool {
    if !db_path.is_file() {
        return false;
    }
    let Some(conn) = open_db(db_path, false) else {
        return false;
    };
    if !table_exists(&conn, "edge_sync_mapping") {
        return false;
    }
    let created_at = now_secs();
    let r = conn.execute(
        "INSERT OR REPLACE INTO edge_sync_mapping \
         (session_id, conversation_id, msg_channel, created_at) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            new_cid,
            new_cid,
            format!("convmsg:{target_uid}"),
            created_at
        ],
    );
    match r {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// 切换前把勾选的会话复制到目标账号（路径 B）。返回复制报告。
pub fn copy_sessions_for_switch(target_acc: &Value, session_ids: &[String]) -> Option<Value> {
    let target_uid = target_acc
        .get("uid")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if target_uid.is_empty() {
        return None;
    }
    let source_context = current_session_context()?;
    let source_uid = source_context.uid.clone();
    if source_uid == target_uid {
        return None;
    }
    let target_edition = edition_for_account(target_acc);
    let target_db = workbuddy_db_path_for_edition(target_edition);
    if !target_db.is_file() {
        return Some(json!({
            "sourceUid": source_uid,
            "targetUid": target_uid,
            "copied": [],
            "errors": [{"error": format!("目标账号数据目录不存在: {}", target_db.display())}],
        }));
    }

    let mut report = json!({
        "sourceUid": source_uid,
        "targetUid": target_uid,
        "copied": [],
    });
    let mut errors: Vec<Value> = Vec::new();
    for cid in session_ids {
        match copy_session_to_user_for_editions(
            cid,
            &source_uid,
            &target_uid,
            source_context.edition,
            target_edition,
        ) {
            Ok(r) => report["copied"].as_array_mut().unwrap().push(r),
            Err(e) => errors.push(json!({"id": cid, "error": e})),
        }
    }
    if !errors.is_empty() {
        report["errors"] = json!(errors);
    }
    Some(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn db_paths_point_to_home() {
        assert!(workbuddy_db_path()
            .to_string_lossy()
            .ends_with(".workbuddy/workbuddy.db"));
        assert!(edge_sync_candidates(SessionEdition::Cn)
            .iter()
            .all(|path| path.to_string_lossy().contains(".workbuddy")));
    }

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "wb_switch_test_{}_{name}.db",
            uuid::Uuid::new_v4().simple()
        ))
    }

    #[test]
    fn insert_session_copy_duplicates_row_with_target_uid() {
        let db = temp_db("sessions");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                title TEXT,
                cwd TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                deleted_at INTEGER,
                payload BLOB
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, user_id, title, cwd, created_at, updated_at, deleted_at, payload)
             VALUES ('src-1', 'uid-a', '旧标题', '/ws', 1000, 2000, NULL, x'DEADBEEF')",
            [],
        )
        .unwrap();

        insert_session_copy_between(&db, &db, "new-uuid-1", "src-1", "uid-a", "uid-b").unwrap();

        let (id, user_id, title, deleted_at): (String, String, String, Option<i64>) = conn
            .query_row(
                "SELECT id, user_id, title, deleted_at FROM sessions WHERE id = 'new-uuid-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(id, "new-uuid-1");
        assert_eq!(user_id, "uid-b");
        assert_eq!(title, "旧标题"); // 普通列原样保留
        assert_eq!(deleted_at, None); // deleted_at 置空

        // 源行保持不变
        let src_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id = 'src-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(src_count, 1);
    }

    #[test]
    fn destination_project_jsonl_moves_between_edition_roots() {
        let source = home_dir()
            .join(".workbuddy")
            .join("projects")
            .join("workspace")
            .join("source.jsonl");
        let destination =
            destination_project_jsonl(&source, SessionEdition::Cn, SessionEdition::Ai, "target")
                .expect("destination path");
        assert!(destination
            .to_string_lossy()
            .ends_with(".workbuddy-ai/projects/workspace/target.jsonl"));
    }

    #[test]
    fn insert_session_copy_missing_source_is_noop() {
        let db = temp_db("noop");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, user_id TEXT, title TEXT, created_at INTEGER, updated_at INTEGER, deleted_at INTEGER);",
        )
        .unwrap();
        let err = insert_session_copy_between(&db, &db, "new-1", "missing", "uid-a", "uid-b")
            .expect_err("缺少源会话时应明确失败");
        assert!(err.contains("未找到源会话"));
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_session_copy_missing_db_is_ok() {
        let db = temp_db("missing");
        // 不创建文件
        assert!(insert_session_copy_between(&db, &db, "new-1", "src-1", "a", "b").is_ok());
    }

    #[test]
    fn insert_edge_sync_mapping_registers_channel() {
        let db = temp_db("edge");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE edge_sync_mapping (
                session_id TEXT,
                conversation_id TEXT,
                msg_channel TEXT,
                created_at INTEGER
            );",
        )
        .unwrap();
        assert!(insert_edge_sync_mapping(&db, "new-1", "uid-b"));
        let (sid, cid, channel): (String, String, String) = conn
            .query_row(
                "SELECT session_id, conversation_id, msg_channel FROM edge_sync_mapping",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(sid, "new-1");
        assert_eq!(cid, "new-1");
        assert_eq!(channel, "convmsg:uid-b");
    }

    #[test]
    fn insert_edge_sync_mapping_missing_table_false() {
        let db = temp_db("edge-no-table");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE other (x INTEGER);")
            .unwrap();
        assert!(!insert_edge_sync_mapping(&db, "new-1", "uid-b"));
    }

    #[test]
    fn session_display_title_prefers_custom_title() {
        assert_eq!(
            session_display_title(Some("自动标题".into()), Some("美团每日自动领券".into())),
            "美团每日自动领券"
        );
        assert_eq!(
            session_display_title(None, Some("美团每日自动领券".into())),
            "美团每日自动领券"
        );
        assert_eq!(
            session_display_title(Some("汉字详情页".into()), None),
            "汉字详情页"
        );
        assert_eq!(session_display_title(None, None), "(无标题)");
        assert_eq!(
            session_display_title(Some("  ".into()), Some("".into())),
            "(无标题)"
        );
    }

    #[test]
    fn claw_workspace_detected_by_folder_name() {
        assert!(is_claw_workspace("/Users/apple/WorkBuddy/Claw"));
        assert!(is_claw_workspace("/Users/apple/WorkBuddy/claw/"));
        assert!(is_claw_workspace(r"C:\Users\me\WorkBuddy\Claw"));
        assert!(!is_claw_workspace("/Users/apple/WorkBuddy/ClawBot"));
        assert!(!is_claw_workspace(
            "/Users/apple/Documents/AI-PROJECT/LetterTotTown"
        ));
    }
}
