// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package fredb

import (
	"context"
	"fmt"
	"os"

	"github.com/alexhholmes/fredb"
	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	fredbPath = "fredb.path"
)

type fredbcreator struct {
}

type fredbOptions struct {
	Path      string
	DBOptions fredb.Option
}

type freDB struct {
	p *properties.Properties

	db *fredb.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c fredbcreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Path)
	}

	db, err := fredb.Open(opts.Path, opts.DBOptions)
	if err != nil {
		return nil, err
	}

	return &freDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) fredbOptions {
	path := p.GetString(fredbPath, "/tmp/fredb")

	opts := fredb.DefaultOptions()

	return fredbOptions{
		Path:      path,
		DBOptions: opts,
	}
}

func (db *freDB) Close() error {
	return db.db.Close()
}

func (db *freDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *freDB) CleanupThread(_ context.Context) {
}

func (db *freDB) Read(_ context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(tx *fredb.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		row := bucket.Get([]byte(key))
		if row == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		var err error
		m, err = db.r.Decode(row, fields)
		return err
	})
	return m, err
}

func (db *freDB) Scan(_ context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(tx *fredb.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		cursor := bucket.Cursor()
		key, value := cursor.Seek([]byte(startKey))
		for i := 0; key != nil && i < count; i++ {
			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res[i] = m
			key, value = cursor.Next()
		}

		return nil
	})
	return res, err
}

func (db *freDB) Update(_ context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *fredb.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		data, err := db.r.Decode(value, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *freDB) Insert(_ context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *fredb.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, values)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *freDB) Delete(_ context.Context, table string, key string) error {
	err := db.db.Update(func(tx *fredb.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return nil
		}

		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

func init() {
	ycsb.RegisterDBCreator("fredb", fredbcreator{})
}
