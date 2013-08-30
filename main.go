package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type PersonEventKey struct {
	Partition uint8
	PersonId  uint16
	EventId   uint16
	Timestamp int64
}

// Globals yeah...
var db *levigo.DB

func main() {
	// Setup command line flags.
	var dbpath = flag.String("dbpath", "", "the path to use to store the LevelDB files")
	var jsonpath = flag.String("jsonpath", "", "the path to scan for JSON files to parse and load into LevelDB")
	var keys = flag.Bool("keys", false, "dumps the keys in LevelDB")

	flag.Parse()
	
	if *dbpath == "" {
		fmt.Println("no path specified in -dbpath")
		return
	}

	// Setup the structs for LevelDB.
	env := levigo.NewDefaultEnv()
	defer env.Close()

	cache := levigo.NewLRUCache(1 << 20)
	defer cache.Close()

	// TODO: Figure out the best options to use.
	options := levigo.NewOptions()
	options.SetErrorIfExists(false)
	options.SetCache(cache)
	options.SetEnv(env)
	options.SetInfoLog(nil)
	options.SetWriteBufferSize(1 << 20)
	options.SetParanoidChecks(true)
	options.SetMaxOpenFiles(10)
	options.SetBlockSize(1024)
	options.SetBlockRestartInterval(8)
	options.SetCompression(levigo.SnappyCompression)
	options.SetCreateIfMissing(true)
	defer options.Close()

	var err error
	db, err = levigo.Open(*dbpath, options)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// If the path flag was set then we'll load the given path into the DB.
	if *jsonpath != "" {
		err = filepath.Walk(*jsonpath, processFile)
		if err != nil {
			log.Fatal(err)
		}
	}

	// If the keys flag was set then we'll dump all the keys and their values to stdout.
	if *keys {
		roptions := levigo.NewReadOptions()
		roptions.SetVerifyChecksums(true)
		roptions.SetFillCache(false)
		defer roptions.Close()

		iterator := db.NewIterator(roptions)
		iterator.SeekToFirst()
		for {
			if !iterator.Valid() {
				break
			}

			var byteKey PersonEventKey
			buf := bytes.NewBuffer(iterator.Key())
			err := binary.Read(buf, binary.LittleEndian, &byteKey)
			if err != nil {
				log.Fatal(err)
			}

			// The value is stored as a serialized map[string]string.
			getValue := iterator.Value()
			propmap := make(map[string]string)
			if getValue != nil && len(getValue) > 0 {
				buf := bytes.NewBuffer(getValue)
				dec := gob.NewDecoder(buf)
				err = dec.Decode(&propmap)
				if err != nil {
					log.Fatal(err)
				}
			}

			fmt.Printf("%v %v\n", byteKey, propmap)
			iterator.Next()
		}
	}
}

// Used by the call to filepath.Walk, this will iterate over the files
func processFile(path string, f os.FileInfo, err error) error {
	if f.IsDir() || !strings.Contains(f.Name(), ".") {
		// Guard dirs and useless files.
		return nil
	}

	name := f.Name()
	split := strings.Split(name, ".")

	if split[1] != "json" {
		// We're only going to look at JSON files.
		return nil
	}

	// With the data dump we received the person ID is the filename.
	personId64, _ := strconv.ParseUint(split[0], 0, 16)
	personId := uint16(personId64)

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	filebytes := make([]byte, stat.Size())
	_, err = file.Read(filebytes)
	if err != nil {
		log.Fatal(err)
	}

	// Yeah shit gets weird here...
	var parsedJson interface{}
	err = json.Unmarshal(filebytes, &parsedJson)

	// Iterate over the JSON and try and parse it into keys and values we can store in LevelDB.
	m0 := parsedJson.(map[string]interface{})
	for k0, _ := range m0 {
		switch k0 {
		case "e":
			// Events with Unix timestamps.
			m1 := m0[k0].(map[string]interface{})
			for k1, _ := range m1 {
				m2 := m1[k1].([]interface{})
				for i := 0; i < len(m2); i++ {
					eventId64, _ := strconv.ParseUint(k1, 0, 16)
					eventId := uint16(eventId64)
					timestamp := int64(m2[i].(float64))

					key := PersonEventKey{1, personId, eventId, timestamp}
					put(key.Bytes(), nil)
				}
			}
		case "p":
			// Property sets.
			m1 := m0[k0].(map[string]interface{})
			for propertyId, _ := range m1 {
				m2 := m1[propertyId].(map[string]interface{})
				m3 := m2["p"].([]interface{})
				for i := 0; i < len(m3); i++ {
					m4 := m3[i].([]interface{})

					timestamp := int64(m4[0].(float64))
					valueIndex := int(m4[1].(float64))
					eventId := uint16(m4[2].(float64))

					m5 := m2["v"].([]interface{})
					propertyValue := m5[valueIndex].(string)

					key := PersonEventKey{1, personId, eventId, timestamp}
					getValue := get(key.Bytes())

					propmap := make(map[string]string)
					if getValue != nil && len(getValue) > 0 {
						buf := bytes.NewBuffer(getValue)
						dec := gob.NewDecoder(buf)
						err = dec.Decode(&propmap)
						if err != nil {
							log.Fatal(err)
						}
					}

					propmap[propertyId] = propertyValue

					buf := new(bytes.Buffer)
					enc := gob.NewEncoder(buf)
					enc.Encode(propmap)

					put(key.Bytes(), buf.Bytes())
				}
			}
		}
	}

	return nil
}

// Wrapper around the LevelDB put.
func put(key, value []byte) {
	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)
	defer woptions.Close()

	err := db.Put(woptions, key, value)
	if err != nil {
		log.Fatal(err)
	}
}

// Wrapper around the LevelDB get.
func get(key []byte) []byte {
	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(true)
	roptions.SetFillCache(false)
	defer roptions.Close()

	value, err := db.Get(roptions, key)
	if err != nil {
		log.Fatal(err)
	}
	return value
}

// Function to convert a PersonEventKey to a byte slice.
func (key PersonEventKey) Bytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, key)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}
