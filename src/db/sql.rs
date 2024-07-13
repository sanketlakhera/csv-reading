use rusqlite::{params, Connection, Result};

pub fn connect_to_db() -> Connection {
    let db_path = "data.db";
    let conn = Connection::open(db_path).unwrap();
    conn
}
pub fn create_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task TEXT NOT NULL,
            completed INTEGER DEFAULT 0
        )",
        [],
    )?;
    Ok(())
}
