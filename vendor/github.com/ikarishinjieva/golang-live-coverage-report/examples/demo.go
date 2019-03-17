package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	report "github.com/ikarishinjieva/golang-live-coverage-report/pkg"
)

func main() {
	flagPort := flag.Int("port", 8080, "port to serve")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `Available commands:
1. /run_once: run the target function once
2. /report: view the coverage report`)
	})

	http.HandleFunc("/run_once", func(w http.ResponseWriter, r *http.Request) {
		theTargetFunction()
		fmt.Fprintf(w, "OK: the target function ran once")
	})

	http.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		err := report.GenerateHtmlReport(w)
		if nil != err {
			log.Fatalf("generate html report error: %v", err)
		}
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", *flagPort), nil))
}
