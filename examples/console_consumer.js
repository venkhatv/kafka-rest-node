/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"use strict";

var KafkaRest = require("..");

var topicName = process.argv[2];
var consumerGroup = process.argv[3];
var messageLimit = process.argv[4];

if (topicName === undefined) {
    console.log("Usage: node console_consumer.js topic [group]");
    process.exit(1);
}

if (consumerGroup === undefined)
    consumerGroup = "console-consumer-" + Math.round(Math.random() * 100000);

var kafka = new KafkaRest({"url": "http://localhost:8080"});
var consumed = 0;
kafka.consumer(consumerGroup).join(function(err, consumer_instance) {
    if (err) return console.log("Failed to create instance in consumer group: " + err);

    console.log("Consumer instance initialized: " + consumer_instance.toString());
    var stream = consumer_instance.subscribe(topicName);
    stream.on('read', function(msgs) {
        for(var i = 0; i < msgs.length; i++) {
            // Messages keys (if available) and values are decoded from base64 into Buffers. You'll need to decode based
            // on the .
            console.log(msgs[i].value.toString('utf8'));
            // Also available: msgs[i].key, msgs[i].partition
        }

        consumed += msgs.length;
        if (messageLimit !== undefined && consumed >= messageLimit)
            consumer_instance.shutdown(logShutdown);
    });
    stream.on('error', function(err) {
        console.log("Consumer instance reported an error: " + err);
        console.log("Attempting to shut down consumer instance...");
        consumer_instance.shutdown(logShutdown);
    });
    stream.on('end', function() {
        console.log("Consumer stream closed.");
    });

    // Events are also emitted by the parent consumer_instance, so you can either consume individual streams separately
    // or multiple streams with one callback. Here we'll just demonstrate the 'end' event.
    consumer_instance.on('end', function() {
        console.log("Consumer instance closed.");
    });
});

function logShutdown(err) {
    if (err)
        console.log("Error while shutting down: " + err);
    else
        console.log("Shutdown cleanly.");
}