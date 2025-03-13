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
	CREATE TABLE files IF NOT EXISTS (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filename TEXT NOT NULL UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`

	dataKeeperTable := `
	CREATE TABLE datakeepers IF NOT EXISTS (
		id TEXT PRIMARY KEY AUTOINCREMENT,
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		last_heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
		is_alive BOOLEAN DEFAULT 1
	);
	`

	matchesTable := `
	CREATE TABLE filelocations IF NOT EXISTS (
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
	CREATE INDEX IF NOT EXISTS idx_data_keepers_ip_port ON data_keepers(ip, port);
	CREATE INDEX IF NOT EXISTS idx_file_locations_file ON file_locations(file_id);
	`

	_, err := DB.Exec(statement)
	if err != nil {
		panic(err)
	}
}
