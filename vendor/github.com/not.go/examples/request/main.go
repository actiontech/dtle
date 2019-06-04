// Copyright 2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"log"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/not.go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func usage() {
	log.Fatalf("Usage: request [-s server] [-creds file] <subject> <msg>")
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Requestor")}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	tracer, closer := not.InitTracing("NATS Requestor")
	opentracing.SetGlobalTracer(tracer)
	defer closer.Close()

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	subj, payload := args[0], []byte(args[1])

	// Setup our request span
	reqSpan := tracer.StartSpan("Service Request", ext.SpanKindRPCClient)
	ext.MessageBusDestination.Set(reqSpan, subj)
	defer reqSpan.Finish()

	// Log our request
	reqSpan.LogEvent("Starting request.")

	// A NATS OpenTracing Message.
	var t not.TraceMsg

	// Inject the span context into the TraceMsg.
	if err := tracer.Inject(reqSpan.Context(), opentracing.Binary, &t); err != nil {
		log.Printf("%v for Inject.", err)
	}

	// Add the payload.
	t.Write(payload)

	// Make the request.
	msg, err := nc.Request(subj, t.Bytes(), time.Second)
	if err != nil {
		if nc.LastError() != nil {
			log.Fatalf("%v for request", nc.LastError())
		}
		log.Fatalf("%v for request", err)
	} else {
		log.Printf("Published [%s] : '%s'", subj, payload)
		log.Printf("Received  [%v] : '%s'", msg.Subject, string(msg.Data))
	}

	// Log that we've completed the request.
	reqSpan.LogEvent("Request Complete")
}
