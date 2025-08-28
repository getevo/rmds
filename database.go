package rmds

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db *sql.DB
	mu sync.RWMutex
}

type StoredMessage struct {
	ID         string
	Channel    string
	Sender     string
	Receiver   string
	Data       []byte
	CreatedAt  time.Time
	ExpiresAt  time.Time
	Status     string
	RetryCount int
	LastRetry  time.Time
}

func NewDatabase(path string) (*Database, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	database := &Database{db: db}
	if err := database.createTables(); err != nil {
		db.Close()
		return nil, err
	}

	return database, nil
}

func (d *Database) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			channel TEXT NOT NULL,
			sender TEXT NOT NULL,
			receiver TEXT NOT NULL,
			data BLOB NOT NULL,
			created_at DATETIME NOT NULL,
			expires_at DATETIME NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0,
			last_retry DATETIME
		)`,
		`CREATE INDEX IF NOT EXISTS idx_status ON messages(status)`,
		`CREATE INDEX IF NOT EXISTS idx_receiver ON messages(receiver)`,
		`CREATE INDEX IF NOT EXISTS idx_expires ON messages(expires_at)`,
		`CREATE TABLE IF NOT EXISTS acks (
			message_id TEXT PRIMARY KEY,
			receiver TEXT NOT NULL,
			received_at DATETIME NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_received ON acks(received_at)`,
		`CREATE TABLE IF NOT EXISTS topology (
			node_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			mode TEXT NOT NULL,
			last_seen DATETIME NOT NULL,
			is_alive INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (node_id, channel)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_node_alive ON topology(node_id, is_alive)`,
		`CREATE INDEX IF NOT EXISTS idx_channel ON topology(channel)`,
		`CREATE INDEX IF NOT EXISTS idx_last_seen ON topology(last_seen)`,
	}

	for _, query := range queries {
		if _, err := d.db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (d *Database) StoreMessage(msg *StoredMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `
	INSERT OR REPLACE INTO messages 
	(id, channel, sender, receiver, data, created_at, expires_at, status, retry_count, last_retry)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.db.Exec(query,
		msg.ID, msg.Channel, msg.Sender, msg.Receiver, msg.Data,
		msg.CreatedAt, msg.ExpiresAt, msg.Status, msg.RetryCount, msg.LastRetry)

	return err
}

func (d *Database) GetPendingMessages(receiver string) ([]*StoredMessage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT id, channel, sender, receiver, data, created_at, expires_at, status, retry_count, last_retry
	FROM messages
	WHERE receiver = ? AND status = 'pending' AND expires_at > ?
	ORDER BY created_at ASC
	LIMIT 100
	`

	rows, err := d.db.Query(query, receiver, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*StoredMessage
	for rows.Next() {
		msg := &StoredMessage{}
		var lastRetry sql.NullTime

		err := rows.Scan(
			&msg.ID, &msg.Channel, &msg.Sender, &msg.Receiver,
			&msg.Data, &msg.CreatedAt, &msg.ExpiresAt,
			&msg.Status, &msg.RetryCount, &lastRetry)

		if err != nil {
			continue
		}

		if lastRetry.Valid {
			msg.LastRetry = lastRetry.Time
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

func (d *Database) GetPendingReceivers() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT DISTINCT receiver
	FROM messages
	WHERE status = 'pending' AND expires_at > ?
	ORDER BY receiver
	`

	rows, err := d.db.Query(query, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var receivers []string
	for rows.Next() {
		var receiver string
		if err := rows.Scan(&receiver); err != nil {
			return nil, err
		}
		receivers = append(receivers, receiver)
	}

	return receivers, rows.Err()
}

func (d *Database) MarkMessageSent(messageID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `UPDATE messages SET status = 'sent' WHERE id = ?`
	_, err := d.db.Exec(query, messageID)
	return err
}

func (d *Database) MarkMessageAcknowledged(messageID string, receiver string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `UPDATE messages SET status = 'acknowledged' WHERE id = ? AND receiver = ?`
	if _, err := tx.Exec(query, messageID, receiver); err != nil {
		return err
	}

	ackQuery := `INSERT INTO acks (message_id, receiver, received_at) VALUES (?, ?, ?)`
	if _, err := tx.Exec(ackQuery, messageID, receiver, time.Now()); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *Database) IncrementRetryCount(messageID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `
	UPDATE messages 
	SET retry_count = retry_count + 1, last_retry = ?
	WHERE id = ?
	`
	_, err := d.db.Exec(query, time.Now(), messageID)
	return err
}

func (d *Database) CleanupOldMessages(olderThan time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteExpired := `DELETE FROM messages WHERE expires_at < ?`
	if _, err := tx.Exec(deleteExpired, time.Now()); err != nil {
		return err
	}

	deleteAcked := `DELETE FROM messages WHERE status = 'acknowledged' AND created_at < ?`
	if _, err := tx.Exec(deleteAcked, cutoff); err != nil {
		return err
	}

	deleteOldAcks := `DELETE FROM acks WHERE received_at < ?`
	if _, err := tx.Exec(deleteOldAcks, cutoff); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *Database) GetStatistics() (map[string]interface{}, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := make(map[string]interface{})

	var pending, sent, acknowledged int
	row := d.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE status = 'pending'`)
	row.Scan(&pending)

	row = d.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE status = 'sent'`)
	row.Scan(&sent)

	row = d.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE status = 'acknowledged'`)
	row.Scan(&acknowledged)

	stats["pending_messages"] = pending
	stats["sent_messages"] = sent
	stats["acknowledged_messages"] = acknowledged

	return stats, nil
}

func (d *Database) PurgeAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM messages`); err != nil {
		return err
	}

	if _, err := tx.Exec(`DELETE FROM acks`); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *Database) PurgeSent() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `DELETE FROM messages WHERE status IN ('sent', 'acknowledged')`
	_, err := d.db.Exec(query)
	return err
}

func (d *Database) PurgeQueue() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `DELETE FROM messages WHERE status = 'pending'`
	_, err := d.db.Exec(query)
	return err
}

// Topology management methods

type TopologyNode struct {
	NodeID   string
	Channel  string
	Mode     string
	LastSeen time.Time
	IsAlive  bool
}

func (d *Database) SaveTopologyNode(nodeID, channel, mode string, isAlive bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `
	INSERT OR REPLACE INTO topology (node_id, channel, mode, last_seen, is_alive)
	VALUES (?, ?, ?, ?, ?)
	`
	_, err := d.db.Exec(query, nodeID, channel, mode, time.Now(), isAlive)
	return err
}

func (d *Database) UpdateNodeStatus(nodeID string, isAlive bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := `
	UPDATE topology 
	SET is_alive = ?, last_seen = ?
	WHERE node_id = ?
	`
	_, err := d.db.Exec(query, isAlive, time.Now(), nodeID)
	return err
}

func (d *Database) GetTopologyNodes() ([]TopologyNode, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT node_id, channel, mode, last_seen, is_alive
	FROM topology
	ORDER BY last_seen DESC
	`
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []TopologyNode
	for rows.Next() {
		var node TopologyNode
		var aliveInt int
		err := rows.Scan(&node.NodeID, &node.Channel, &node.Mode, &node.LastSeen, &aliveInt)
		if err != nil {
			return nil, err
		}
		node.IsAlive = aliveInt == 1
		nodes = append(nodes, node)
	}

	return nodes, rows.Err()
}

func (d *Database) GetAliveNodesForChannel(channel string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT DISTINCT node_id
	FROM topology
	WHERE channel = ? AND is_alive = 1 AND mode IN ('reader', 'rw')
	ORDER BY last_seen DESC
	`
	rows, err := d.db.Query(query, channel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		nodes = append(nodes, nodeID)
	}

	return nodes, rows.Err()
}

// GetAllNodesForChannel returns all nodes (alive and offline) that have ever been on a channel
func (d *Database) GetAllNodesForChannel(channel string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT DISTINCT node_id
	FROM topology
	WHERE channel = ? AND mode IN ('reader', 'rw')
	ORDER BY last_seen DESC
	`
	rows, err := d.db.Query(query, channel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		nodes = append(nodes, nodeID)
	}

	return nodes, rows.Err()
}

func (d *Database) GetAllAliveNodes() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `
	SELECT DISTINCT node_id
	FROM topology
	WHERE is_alive = 1
	ORDER BY last_seen DESC
	`
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		nodes = append(nodes, nodeID)
	}

	return nodes, rows.Err()
}

func (d *Database) IsNodeAlive(nodeID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	query := `SELECT COUNT(*) FROM topology WHERE node_id = ? AND is_alive = 1`
	var count int
	err := d.db.QueryRow(query, nodeID).Scan(&count)
	return err == nil && count > 0
}

func (d *Database) CleanupOldTopologyNodes(olderThan time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	query := `DELETE FROM topology WHERE last_seen < ? AND is_alive = 0`
	_, err := d.db.Exec(query, cutoff)
	return err
}

func (d *Database) Close() error {
	return d.db.Close()
}
