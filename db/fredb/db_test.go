package fredb

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/magiconair/properties"
)

func newTestDB(t *testing.T) (*freDB, func()) {
	tmpDir := filepath.Join(os.TempDir(), "fredb-test")
	os.RemoveAll(tmpDir)

	p := properties.NewProperties()
	p.Set(fredbPath, tmpDir)
	p.Set("dropdata", "true")

	dbi, err := fredbcreator{}.Create(p)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	db := dbi.(*freDB)

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
	return db, cleanup
}

func TestInsertReadDelete(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{"field0": []byte("v")}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := db.Read(ctx, table, key, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(got, vals) {
		t.Fatalf("read mismatch, got %v want %v", got, vals)
	}

	if err := db.Delete(ctx, table, key); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify deletion
	_, err = db.Read(ctx, table, key, nil)
	if err == nil {
		t.Fatalf("expected error reading deleted key, got nil")
	}
}

func TestScan(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	for i := 0; i < 3; i++ {
		key := "key" + string(rune('A'+i))
		val := map[string][]byte{"field0": {byte(i)}}

		if err := db.Insert(ctx, table, key, val); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	res, err := db.Scan(ctx, table, "keyA", 3, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res))
	}
}

func TestUpdate(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{"field0": []byte("v"), "field1": []byte("v2")}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// update existing field
	if err := db.Update(ctx, table, key, map[string][]byte{"field0": []byte("v_updated")}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// read the updated value
	got, err := db.Read(ctx, table, key, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	expected := map[string][]byte{"field0": []byte("v_updated"), "field1": []byte("v2")}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("read mismatch, got %v want %v", got, expected)
	}
}

func TestBatchInsert(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	keys := []string{"k1", "k2", "k3"}
	values := []map[string][]byte{
		{"field0": []byte("v1")},
		{"field0": []byte("v2")},
		{"field0": []byte("v3")},
	}

	if err := db.BatchInsert(ctx, table, keys, values); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	// Verify all keys were inserted
	for i, key := range keys {
		got, err := db.Read(ctx, table, key, nil)
		if err != nil {
			t.Fatalf("read key %s: %v", key, err)
		}
		if !reflect.DeepEqual(got, values[i]) {
			t.Fatalf("read mismatch for key %s, got %v want %v", key, got, values[i])
		}
	}
}

func TestBatchRead(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	keys := []string{"k1", "k2", "k3"}
	values := []map[string][]byte{
		{"field0": []byte("v1")},
		{"field0": []byte("v2")},
		{"field0": []byte("v3")},
	}

	// Insert test data
	if err := db.BatchInsert(ctx, table, keys, values); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	// Batch read
	got, err := db.BatchRead(ctx, table, keys, nil)
	if err != nil {
		t.Fatalf("batch read: %v", err)
	}

	if len(got) != len(values) {
		t.Fatalf("expected %d results, got %d", len(values), len(got))
	}

	for i := range values {
		if !reflect.DeepEqual(got[i], values[i]) {
			t.Fatalf("batch read mismatch at index %d, got %v want %v", i, got[i], values[i])
		}
	}
}

func TestBatchUpdate(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	keys := []string{"k1", "k2", "k3"}
	values := []map[string][]byte{
		{"field0": []byte("v1")},
		{"field0": []byte("v2")},
		{"field0": []byte("v3")},
	}

	// Insert initial data
	if err := db.BatchInsert(ctx, table, keys, values); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	// Batch update
	updatedValues := []map[string][]byte{
		{"field0": []byte("v1_updated")},
		{"field0": []byte("v2_updated")},
		{"field0": []byte("v3_updated")},
	}
	if err := db.BatchUpdate(ctx, table, keys, updatedValues); err != nil {
		t.Fatalf("batch update: %v", err)
	}

	// Verify updates
	got, err := db.BatchRead(ctx, table, keys, nil)
	if err != nil {
		t.Fatalf("batch read: %v", err)
	}

	for i := range updatedValues {
		if !reflect.DeepEqual(got[i], updatedValues[i]) {
			t.Fatalf("batch update mismatch at index %d, got %v want %v", i, got[i], updatedValues[i])
		}
	}
}

func TestBatchDelete(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	keys := []string{"k1", "k2", "k3"}
	values := []map[string][]byte{
		{"field0": []byte("v1")},
		{"field0": []byte("v2")},
		{"field0": []byte("v3")},
	}

	// Insert test data
	if err := db.BatchInsert(ctx, table, keys, values); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	// Batch delete
	if err := db.BatchDelete(ctx, table, keys); err != nil {
		t.Fatalf("batch delete: %v", err)
	}

	// Verify all keys were deleted
	for _, key := range keys {
		_, err := db.Read(ctx, table, key, nil)
		if err == nil {
			t.Fatalf("expected error reading deleted key %s, got nil", key)
		}
	}
}

func TestReadWithFields(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "tbl"
	key := "k1"
	vals := map[string][]byte{
		"field1": []byte("value1"),
		"field2": []byte("value2"),
		"field3": []byte("value3"),
	}

	if err := db.Insert(ctx, table, key, vals); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Read specific fields
	fields := []string{"field1", "field3"}
	got, err := db.Read(ctx, table, key, fields)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	expected := map[string][]byte{
		"field1": []byte("value1"),
		"field3": []byte("value3"),
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("read mismatch, got %v want %v", got, expected)
	}
}

func TestDeleteNonExistentTable(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "nonexistent"
	key := "k1"

	// Should not error on deleting from non-existent table
	if err := db.Delete(ctx, table, key); err != nil {
		t.Fatalf("delete from non-existent table: %v", err)
	}
}

func TestBatchDeleteNonExistentTable(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	ctx := context.Background()
	table := "nonexistent"
	keys := []string{"k1", "k2"}

	// Should not error on batch deleting from non-existent table
	if err := db.BatchDelete(ctx, table, keys); err != nil {
		t.Fatalf("batch delete from non-existent table: %v", err)
	}
}
