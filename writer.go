package main

import (
	"fmt"
	"io"
	"bufio"
	"os"
	"log"
	"github.com/jmhodges/levigo"
)

func main() {

	dbpath := "/Users/bahern/leveldb_instances/sample"	
	fmt.Printf("LevelDB path: %s\n", dbpath)
	
	env := levigo.NewDefaultEnv()
	defer env.Close()
	
	cache := levigo.NewLRUCache(1 << 20)
	defer cache.Close()

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
	options.SetCompression(levigo.NoCompression)
	options.SetCreateIfMissing(true)
	defer options.Close()
	
	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(true)
	roptions.SetFillCache(false)
	defer roptions.Close()
	
	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)
	defer woptions.Close()

	db, err := levigo.Open(dbpath, options)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	file, err := os.Open("/usr/share/dict/words")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	reader := bufio.NewReader(file)

	for {
	        line, isPrefix, err := reader.ReadLine()
		 	
		// loop termination condition 1:  EOF.
	        // this is the normal loop termination condition.
	        if err == io.EOF {
	            break
	        }
	 
	        // loop termination condition 2: some other error.
	        // Errors happen, so check for them and do something with them.
		if err != nil {
			log.Fatal(err)
		}
		 
	        // loop termination condition 3: line too long to fit in buffer
	        // without multiple reads.  Bufio's default buffer size is 4K.
	        // Chances are if you haven't seen a line terminator after 4k
	        // you're either reading the wrong file or the file is corrupt.
	        if isPrefix {
	            fmt.Printf("Error: Unexpected long line reading", file.Name())
	        }
	 
		key := line
		value := line
		err = db.Put(woptions, key, value)
		if err != nil {
			log.Fatal(err)
		}
	}
}