package main

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseRecordEvent(t *testing.T) {

	const input = "BUCKET_OWNER BUCKET [22/Feb/2010:18:20:02 -0500] 10.251.27.20 REQUESTER REQUEST_ID OPERATION KEY \"GET /e?_k=foobarapikey&_n=Viewed+Course+Description+Page&Course+ID=2&referrer=%2F&_p=foobarperson&Course+Name=Lean+Startup+%28OLD+VERSION%2C+LINK+TO+NEW+VERSION+ON+HOMEPAGE%29&_t=1266880560 HTTP/1.1\" 200 ERROR_CODE 43 OBJECT_SIZE TOTAL_TIME TURN_AROUND_TIME \"-\" \"-\"\n"
	reader := bufio.NewReader(strings.NewReader(input))

	verify := func(values RecordEventValues) {
		const timestamp = 1266880802
		if values.timestamp != timestamp {
			t.Errorf("Failed parsing timestamp.  Expected %d, found %d\n", timestamp, values.timestamp)
		}

		const apiKey = "foobarapikey"
		if values.apiKey != apiKey {
			t.Errorf("Failed parsing apiKey.  Expected %s, found %s\n", apiKey, values.apiKey)
		}

		const person = "foobarperson"
		if values.person != person {
			t.Errorf("Failed parsing person.  Expected %s, found %s\n", person, values.person)
		}

		const eventName = "Viewed Course Description Page"
		if values.eventName != eventName {
			t.Errorf("Failed parsing eventName.  Expected %s, found %s\n", eventName, values.eventName)
		}
	}

	Parse(reader, verify, nil, nil)
}

func TestParseAliasUser(t *testing.T) {

	const input = "BUCKET_OWNER BUCKET [22/Feb/2010:18:20:02 -0500] 10.251.27.20 REQUESTER REQUEST_ID OPERATION KEY \"GET /a?_k=foobarapikey&_n=foobaralias&_p=foobarperson&_t=1266880560 HTTP/1.1\" 200 ERROR_CODE 43 OBJECT_SIZE TOTAL_TIME TURN_AROUND_TIME \"-\" \"-\"\n"
	reader := bufio.NewReader(strings.NewReader(input))

	verify := func(values AliasUserValues) {
		const timestamp = 1266880802
		if values.timestamp != timestamp {
			t.Errorf("Failed parsing timestamp.  Expected %d, found %d\n", timestamp, values.timestamp)
		}

		const apiKey = "foobarapikey"
		if values.apiKey != apiKey {
			t.Errorf("Failed parsing apiKey.  Expected %s, found %s\n", apiKey, values.apiKey)
		}

		const identity1 = "foobarperson"
		if values.identity1 != identity1 {
			t.Errorf("Failed parsing identity1.  Expected %s, found %s\n", identity1, values.identity1)
		}

		const identity2 = "foobaralias"
		if values.identity2 != identity2 {
			t.Errorf("Failed parsing identity2.  Expected %s, found %s\n", identity2, values.identity2)
		}
	}

	Parse(reader, nil, nil, verify)
}