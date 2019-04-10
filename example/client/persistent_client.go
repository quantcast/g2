package main

import (
	"log"
	"os"
	"time"

	"github.com/quantcast/g2/client"
	rt "github.com/quantcast/g2/pkg/runtime"
)

func main() {
	// Set the autoinc id generator
	// You can write your own id generator
	// by implementing IdGenerator interface.
	// client.IdGen = client.NewAutoIncId()

	logs.InitLogs()
	logs.FlushLogs()
	c, err := client.NewNetClient(rt.Network, "127.0.0.1:4730")
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Close()
	c.ErrorHandler = func(e error) {
		log.Println("Error:", e)
		os.Exit(1)
	}
	echo := []byte("Hello\x00 world")
	echomsg, err := c.Echo(echo)
	if err != nil {
		log.Fatalln("Fatal Error", err)
	}
	log.Println("EchoMsg:", string(echomsg))

	print_result := true
	print_update := false
	print_status := false

	jobHandler := func(resp *client.Response) {
		switch resp.DataType {
		case rt.PT_WorkException:
			fallthrough
		case rt.PT_WorkFail:
			fallthrough
		case rt.PT_WorkComplete:
			if print_result {
				if data, err := resp.Result(); err == nil {
					log.Printf("RESULT: %v, string:%v\n", data, string(data))
				} else {
					log.Printf("RESULT: %s\n", err)
				}
			}
		case rt.PT_WorkWarning:
			fallthrough
		case rt.PT_WorkData:
			if print_update {
				if data, err := resp.Update(); err == nil {
					log.Printf("UPDATE: %v\n", data)
				} else {
					log.Printf("UPDATE: %v, %s\n", data, err)
				}
			}
		case rt.PT_WorkStatus:
			if print_status {
				if data, err := resp.Status(); err == nil {
					log.Printf("STATUS: %v\n", data)
				} else {
					log.Printf("STATUS: %s\n", err)
				}
			}
		default:
			log.Printf("UNKNOWN: %v", resp.Data)
		}
	}

	log.Println("Press Ctrl-C to exit ...")

	for i := 0; ; i++ {

		if !c.IsConnectionSet() {
			log.Printf("No active connection to server.. waiting...")
			time.Sleep(5 * time.Second)
			continue
		}

		funcName := "ToUpper"
		log.Println("Calling function", funcName, "with data:", echo)
		handle, err := c.Do(funcName, echo, rt.JobNormal, jobHandler)
		if err != nil {
			log.Printf("Do %v ERROR:", funcName, err)
		}

		log.Printf("Calling Status for handle %v", handle)
		status, err := c.Status(handle)
		if err != nil {
			log.Printf("Status: %v, ERROR: %v", status, err)
		}

		funcName = "Foobar"
		log.Println("Calling function", funcName, "with data:", echo)
		_, err = c.Do(funcName, echo, rt.JobNormal, jobHandler)
		if err != nil {
			log.Printf("Do %v ERROR:", funcName, err)
		}
		var sleep_seconds int = 0
		log.Printf("Finished Cycle %v, sleeping %v seconds", i, sleep_seconds)
		time.Sleep(time.Duration(sleep_seconds) * time.Second)

	}

}
