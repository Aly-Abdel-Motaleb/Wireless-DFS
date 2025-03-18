package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB

func InitDb() {
	var err error
	DB, err = sql.Open("sqlite3", "db.db")
	if err != nil {
		panic(err)
	}

	DB.SetMaxOpenConns(10)
	DB.SetMaxIdleConns(5)

	createTables()
	createIndices()
}

func createTables() {
	filesTable := `
	CREATE TABLE IF NOT EXISTS files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filename TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`

	dataKeeperTable := `
	CREATE TABLE IF NOT EXISTS datakeepers (
		id TEXT PRIMARY KEY,
		ip TEXT NOT NULL,
		port TEXT NOT NULL,
		last_heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
		is_alive BOOLEAN DEFAULT 1
	);
	`

	matchesTable := `
	CREATE TABLE IF NOT EXISTS file_locations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_id INTEGER NOT NULL,
		data_keeper_id TEXT NOT NULL,
		filepath TEXT NOT NULL,
		FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
		FOREIGN KEY (data_keeper_id) REFERENCES datakeepers(id) ON DELETE CASCADE
	);`

	_, err := DB.Exec(filesTable)
	if err != nil {
		panic(err)
	}
	_, err = DB.Exec(dataKeeperTable)
	if err != nil {
		panic(err)
	}

	_, err = DB.Exec(matchesTable)
	if err != nil {
		panic(err)
	}
}

func createIndices() {
	statement := `
	CREATE INDEX IF NOT EXISTS idx_files_filename ON files(filename);
	CREATE INDEX IF NOT EXISTS idx_data_keepers_ip_port ON datakeepers(ip, port);
	CREATE INDEX IF NOT EXISTS idx_file_locations_file ON file_locations(file_id);
	`

	_, err := DB.Exec(statement)
	if err != nil {
		panic(err)
	}
}

func DoesFileExistByHash(hash string, dkId string) bool {
	statement := `
	SELECT fl.id FROM file_locations fl
	JOIN files f ON fl.file_id = f.id
	WHERE f.hash = ? AND fl.data_keeper_id = ?;`
	row := DB.QueryRow(statement, hash, dkId)
	var id int
	err := row.Scan(&id)
	if err != nil {
		return false
	}
	return true
}
