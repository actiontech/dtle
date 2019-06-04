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
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/not.go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func usage() {
	log.Fatalf("Usage: reply [-s server] [-creds file] [-t] <subject> <response>")
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var numMsgs = flag.Int("n", 1, "Exit after N msgs processed.")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	tracer, closer := not.InitTracing("NATS Responder")
	opentracing.SetGlobalTracer(tracer)
	defer closer.Close()

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Responder")}
	opts = not.SetupConnOptions(tracer, opts)

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}
	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(*numMsgs)

	subj, reply := args[0], []byte(args[1])

	nc.Subscribe(subj, func(msg *nats.Msg) {
		defer wg.Done()

		// Create new TraceMsg from the NATS message.
		t := not.NewTraceMsg(msg)

		// Extract the span context from the request message.
		sc, err := tracer.Extract(opentracing.Binary, t)
		if err != nil {
			log.Printf("Extract error: %v", err)
		}

		// Setup a span referring to the span context of the incoming NATS message.
		replySpan := tracer.StartSpan("Service Responder", ext.SpanKindRPCServer, ext.RPCServerOption(sc))
		ext.MessageBusDestination.Set(replySpan, msg.Subject)
		defer replySpan.Finish()

		log.Printf("Received request: %s", t)

		if err := nc.Publish(msg.Reply, reply); err != nil {
			replySpan.LogEvent(fmt.Sprintf("error: %v", err))
		}

		replySpan.LogEvent(fmt.Sprintf("Response msg: %s", reply))
	})

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	wg.Wait()
}
