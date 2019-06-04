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

	"github.com/nats-io/go-nats"
	"github.com/nats-io/not.go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func usage() {
	log.Fatalf("Usage: publish [-s server] [-creds file] <subject> <msg>")
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		usage()
	}

	tracer, closer := not.InitTracing("NATS Publisher")
	opentracing.SetGlobalTracer(tracer)
	defer closer.Close()

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Tracing Publisher")}

	// Use UserCredentials.
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Connect to NATS.
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	subj, msg := args[0], []byte(args[1])

	// A NATS OpenTracing Message.
	var t not.TraceMsg

	// Setup a span for the operation to publish a message.
	pubSpan := tracer.StartSpan("Published Message", ext.SpanKindProducer)
	ext.MessageBusDestination.Set(pubSpan, subj)
	defer pubSpan.Finish()

	// Inject span context into our traceMsg.
	if err := tracer.Inject(pubSpan.Context(), opentracing.Binary, &t); err != nil {
		log.Fatalf("%v for Inject.", err)
	}

	// Add the payload.
	t.Write(msg)

	// Send the message over NATS.
	nc.Publish(subj, t.Bytes())

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Published [%s] : '%s'\n", subj, msg)
	}
}
