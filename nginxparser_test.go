package main

import (
	"bufio"
	"strings"
	"testing"
)

var eventTests = []struct {
	rawevent  string
	timestamp int64
	apiKey    string
	person    string
	eventName string
}{
	{"BUCKET_OWNER BUCKET [22/Feb/2010:18:20:02 -0500] 10.251.27.20 REQUESTER REQUEST_ID OPERATION KEY \"GET /e?_k=foobarapikey&_n=Viewed+Course+Description+Page&Course+ID=2&referrer=%2F&_p=foobarperson&Course+Name=Lean+Format+Parsing+%28OLD+VERSION%2C+LINK+TO+NEW+VERSION+ON+HOMEPAGE%29&_t=1266880560 HTTP/1.1\" 200 ERROR_CODE 43 OBJECT_SIZE TOTAL_TIME TURN_AROUND_TIME \"-\" \"-\"\n",
		1266880802, "foobarapikey", "foobarperson", "Viewed Course Description Page"},
}

var aliasTests = []struct {
	rawevent  string
	timestamp int64
	apiKey    string
	identity1 string
	identity2 string
}{
	{"BUCKET_OWNER BUCKET [22/Feb/2010:18:20:02 -0500] 10.251.27.20 REQUESTER REQUEST_ID OPERATION KEY \"GET /a?_k=foobarapikey&_n=foobaralias&_p=foobarperson&_t=1266880560 HTTP/1.1\" 200 ERROR_CODE 43 OBJECT_SIZE TOTAL_TIME TURN_AROUND_TIME \"-\" \"-\"\n",
		1266880802, "foobarapikey", "foobarperson", "foobaralias"},
}

func expectedInt(t *testing.T, expected, actual int64, field string) {
	if expected != actual {
		t.Errorf("Failed parsing %s.\n\tExpected: %d, Actual: %d",
			field, expected, actual)
	}
}

func expectedStr(t *testing.T, expected, actual, field string) {
	if expected != actual {
		t.Errorf("Failed parsing %s.\n\tExpected: %s, Actual: %s",
			field, expected, actual)
	}
}

func TestParseRecordEvents(t *testing.T) {
	for _, test := range eventTests {
		reader := bufio.NewReader(strings.NewReader(test.rawevent))
		verify := func(values RecordEventValues) {
			expectedInt(t, values.timestamp, test.timestamp, "timestamp")
			expectedStr(t, values.apiKey, test.apiKey, "apiKey")
			expectedStr(t, values.person, test.person, "person")
			expectedStr(t, values.eventName, test.eventName, "eventName")
		}
		Parse(reader, verify, nil, nil)
	}
}

func TestParseAliasUser(t *testing.T) {
	for _, test := range aliasTests {
		reader := bufio.NewReader(strings.NewReader(test.rawevent))
		verify := func(values RecordEventValues) {
			expectedInt(t, values.timestamp, test.timestamp, "timestamp")
			expectedStr(t, values.apiKey, test.apiKey, "apiKey")
			expectedStr(t, values.identity1, test.identity1, "identity1")
			expectedStr(t, values.identity2, test.identity2, "identity2")
		}
		Parse(reader, nil, nil, verify)
	}
}
