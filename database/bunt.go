package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type BuntDB struct {
	db          *buntdb.DB
	collections map[string]bool
}

func NewBuntDB(url string) (*BuntDB, error) {
	db, err := buntdb.Open(url)
	if err != nil {
		return nil, err
	}

	db.CreateIndex("created_at", "*", buntdb.IndexJSON("created_at"))
	db.CreateIndex("updated_at", "*", buntdb.IndexJSON("updated_at"))
	db.CreateIndex("deleted_at", "*", buntdb.IndexJSON("deleted_at"))

	return &BuntDB{db: db, collections: map[string]bool{}}, err
}

// Read will read data from the database
func (b *BuntDB) Read(col string, ID string) (val string, err error) {
	err = b.db.View(func(tx *buntdb.Tx) error {
		val, err = tx.Get(col + ":" + ID)
		return err
	})
	return val, err
}

// Write will write data to the database
func (b *BuntDB) Write(col string, data string) (string, error) {
	// create index if non exists
	if _, ok := b.collections[col]; !ok {
		b.collections[col] = true
		b.db.CreateIndex("_id", col+":_id", buntdb.IndexString)
	}

	ID := ksuid.New().String()

	err := b.db.Update(func(tx *buntdb.Tx) error {

		data, err := sjson.Set(data, "_id", ID)
		if err != nil {
			return err
		}

		data, err = setAtTimes(data, time.Now().Unix(), 0, 0)
		if err != nil {
			return err
		}

		_, _, err = tx.Set(col+":"+ID, data, nil)
		return err
	})
	return ID, err
}

// Update will update data in the database
func (b *BuntDB) Update(col string, ID string, data string) error {
	err := b.db.Update(func(tx *buntdb.Tx) error {
		data, err := tx.Get(col + ":" + ID)
		if err != nil {
			return err
		}

		results := gjson.GetMany(data, "created_at", "deleted_at")
		data, err = setAtTimes(data, results[0].Int(), time.Now().Unix(), results[1].Int())
		if err != nil {
			return err
		}

		_, _, err = tx.Set(col+":"+ID, data, nil)
		return err
	})
	return err
}

func (b *BuntDB) List(count32 int32, token string) (string, string, error) {
	res := []string{}

	index, count, size := 0, int(count32), 0

	offset, e := strconv.Atoi(token)
	if e != nil {
		return "", "", e
	}

	err := b.db.View(func(tx *buntdb.Tx) error {
		var e error

		tx.Ascend("created_at", func(key, value string) bool {
			if size == count {
				return false
			}
			if index < offset {
				index++
				return true
			}

			res = append(res, value)

			index++
			size++
			return true
		})
		return e
	})

	nextToken := strconv.Itoa(index + count)

	out := "[" + strings.Join(res, ",") + "]"

	return out, nextToken, err
}

// Delete will delete data in the database
func (b *BuntDB) Delete(col string, ID string, perm bool) error {
	if perm {
		return b.db.Update(func(tx *buntdb.Tx) error {
			_, err := tx.Delete(col + ":" + ID)
			return err
		})
	}

	return b.db.Update(func(tx *buntdb.Tx) error {
		data, err := tx.Get(col + ":" + ID)
		if err != nil {
			return err
		}

		results := gjson.GetMany(data, "created_at", "updated_at")
		data, err = setAtTimes(data, results[0].Int(), results[1].Int(), time.Now().Unix())
		if err != nil {
			return err
		}

		_, _, err = tx.Set(col+":"+ID, data, nil)
		return err
	})
}

// Close will close the connection to the db
func (b *BuntDB) Close() error {
	return b.db.Close()
}

func setAtTimes(jsonData string, created int64, updated int64, deleted int64) (string, error) {
	var err error
	data := jsonData

	data, err = sjson.Set(data, "created_at", created)
	if err != nil {
		return data, err
	}

	data, err = sjson.Set(data, "updated_at", updated)
	if err != nil {
		return data, err
	}

	data, err = sjson.Set(data, "deleted_at", deleted)
	if err != nil {
		return data, err
	}
	return data, err
}
