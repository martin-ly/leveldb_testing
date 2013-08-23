package main

import (
	"fmt"
	"regexp"
	"bufio"
	"os"
	"time"
)

func main() {

	// Setup for reading from stdin.
	reader := bufio.NewReader(os.Stdin)

	for {
		// Read lines...
		line, err := reader.ReadString('\n')

		if err != nil {
			// Lame breaking based on EOF.
			break
		}

		// Look for _k=foo and _p=bar and the timestamp from the Nginx log entry.
		re := regexp.MustCompile("(?P<timestamp>[\\d]+/[a-zA-Z]+/[\\d]+:[\\d]+:[\\d]+:[\\d]+ ['+|-][\\d]+).*_k=(?P<eventid>[0-9A-Za-z]*).*&_p=(?P<productid>[0-9A-Za-z]*)")

		matched := re.MatchString(line)
		if matched {
			// This matches the three parts.
			slice := re.FindStringSubmatch(line)
		
			timestamp := slice[1]
			eventid := slice[2]
			personid := slice[3]
			
			// Setup to parse the Common Log Format. (http://en.wikipedia.org/wiki/Common_Log_Format) 
			const nginxFormat = "02/Jan/2006:15:04:05 -0700"
			parsedtimestamp, _ := time.Parse(nginxFormat, timestamp)
			unixtimestamp := parsedtimestamp.Unix()
			
			fmt.Printf("{timestamp:%d, eventid:%s, personid:%s}\n", unixtimestamp, eventid, personid)
		}
	}
	
}