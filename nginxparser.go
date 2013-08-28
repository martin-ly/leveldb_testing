package main

import (
	"bufio"
	"io"
	"log"
	"net/url"
	"regexp"
	"time"
)

type RecordEventValues struct {
	timestamp int64
	apiKey    string
	person    string
	eventName string
	// TODO: Need property parsing...
}

type SetPropertyValues struct {
	timestamp int64
	apiKey    string
	person    string
	// TODO: Need property parsing...
}

type AliasUserValues struct {
	timestamp int64
	apiKey    string
	identity1 string
	identity2 string
}

type RecordEventHandler func(RecordEventValues)

type SetPropertyHandler func(SetPropertyValues)

type AliasUserHandler func(AliasUserValues)

func Parse(reader *bufio.Reader, recordEvent RecordEventHandler, setProperty SetPropertyHandler, aliasUser AliasUserHandler) {
	for {
		// Read lines...
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			// Break on EOF.
			break
		}

		if err != nil {
			// Fail on other errors.
			log.Fatal(err)
		}

		// Regex for the Nginx log.
		re := regexp.MustCompile("([\\d]+/[a-zA-Z]+/[\\d]+:[\\d]+:[\\d]+:[\\d]+ ['+|-][\\d]+).*GET /(e|s|a)\\?(.*) HTTP/1.1")
		matched := re.MatchString(line)
		if matched {
			// This matches the timestamp, the method and the query string.
			slice := re.FindStringSubmatch(line)

			timestamp := slice[1]

			methodType := slice[2]

			query := slice[3]

			// Setup to parse the Common Log Format. (http://en.wikipedia.org/wiki/Common_Log_Format)
			const nginxFormat = "02/Jan/2006:15:04:05 -0700"
			parsedTimestamp, _ := time.Parse(nginxFormat, timestamp)
			unixTimestamp := parsedTimestamp.Unix()

			values, err := url.ParseQuery(query)
			if err != nil {
				log.Fatal(err)
			}

			switch methodType {
			case "e":
				apiKey := values.Get("_k")
				person := values.Get("_p")
				eventName := values.Get("_n")
				// TODO: Need property parsing...

				recordEvent(RecordEventValues{unixTimestamp, apiKey, person, eventName})
			case "s":
				apiKey := values.Get("_k")
				person := values.Get("_p")
				// TODO: Need property parsing...

				setProperty(SetPropertyValues{unixTimestamp, apiKey, person})
			case "a":
				apiKey := values.Get("_k")
				identity1 := values.Get("_p")
				identity2 := values.Get("_n")

				aliasUser(AliasUserValues{unixTimestamp, apiKey, identity1, identity2})
			}
		}
	}
}
