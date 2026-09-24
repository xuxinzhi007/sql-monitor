package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func runDemo(output io.Writer) error {
	sqlLogger := logger.New(log.New(output, "\r\n", log.LstdFlags), logger.Config{
		SlowThreshold: time.Nanosecond,
		LogLevel:      logger.Info,
		Colorful:      false,
	})
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: sqlLogger})
	if err != nil {
		return err
	}
	connection, err := db.DB()
	if err != nil {
		return err
	}
	defer connection.Close()
	connection.SetMaxOpenConns(1)
	if err := db.Exec("CREATE TABLE demo_items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)").Error; err != nil {
		return err
	}
	for id := 1; id <= 3; id++ {
		result := db.Exec("INSERT INTO demo_items (id, label) VALUES (?, ?)", id, fmt.Sprintf("demo-value-%d", id))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("insert affected %d rows", result.RowsAffected)
		}
	}
	for repeat := 0; repeat < 2; repeat++ {
		for id := 1; id <= 3; id++ {
			var item struct {
				ID    int
				Label string
			}
			result := db.Raw("SELECT id, label FROM demo_items WHERE id = ?", id).Scan(&item)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 || item.ID != id || item.Label != fmt.Sprintf("demo-value-%d", id) {
				return fmt.Errorf("query verification failed for id %d", id)
			}
		}
	}
	var result struct{ Label string }
	err = db.Raw("SELECT label FROM missing_demo_table WHERE id = ?", 1).Scan(&result).Error
	if err == nil || !strings.Contains(err.Error(), "no such table: missing_demo_table") {
		return fmt.Errorf("expected missing-table error, got %v", err)
	}
	return nil
}

func main() {
	if err := runDemo(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Demo failed:", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "Demo verified: CREATE, 3 INSERTs, 6 SELECTs, and 1 intentional SQL error; database was in memory only.")
}
