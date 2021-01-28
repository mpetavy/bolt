package main

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"strconv"

	"github.com/mpetavy/common"

	"github.com/boltdb/bolt"
)

type Person struct {
	ID      int
	Name    string
	Vorname string
	Strasse string
	PLZ     int
	Ort     string
}

const (
	dbname = "my.db"
)

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func run() error {
	b := common.FileExists(dbname)
	if b {
		err := os.Remove(dbname)
		if err != nil {
			return err
		}
	}

	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(dbname, os.ModePerm, nil)
	if err != nil {
		return err
	}

	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("person"))
		if err != nil {
			return err
		}

		for i := 0; i < 1000000; i++ {
			p := Person{}

			// Generate ID for the user.
			// This returns an error only if the Tx is closed or not writeable.
			// That can't happen in an Update() call so I ignore the error check.
			id, err := b.NextSequence()
			if err != nil {
				return err
			}

			p.ID = int(id)
			p.Name = "Name #" + strconv.Itoa(p.ID)
			p.Vorname = "Vorname #" + strconv.Itoa(p.ID)

			// Marshal user data into bytes.
			buf, err := json.Marshal(p)
			if err != nil {
				return err
			}

			// Persist bytes to users bucket.
			err = b.Put(itob(p.ID), buf)
			if err != nil {
				return err
			}
		}

		//c := b.Cursor()
		//
		//for k, v := c.First(); k != nil; k, v = c.Next() {
		//	fmt.Printf("key=%s, value=%s\n", k, v)
		//}

		return nil
	})

	return err
}

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}
