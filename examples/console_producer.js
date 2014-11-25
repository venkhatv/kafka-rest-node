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

var KafkaRest = require(".."),
    async = require('async');

var topicName = process.argv[2];
var partitionId = process.argv[3];

if (topicName === undefined) {
    console.log("Usage: node console_producer.js topic [partition]");
    process.exit(1);
}

var kafka = new KafkaRest({"url": "http://localhost:8080"});

var target = kafka.topic(topicName);
if (partitionId)
    target = target.partition(partitionId);

// Runs initial check to make sure the target (topic or partition) actually exists. This happens automatically on the
// REST proxy as well, but you may want to check this proactively to ensure no messages will get lost.
function checkTarget(cb) {
    console.log("Checking that requested topic/partition exists (" + target.getPath() + ")");
    target.get(function(err, res) {
        if (err) {
            console.log("Something looks wrong with the specified topic/partition: " + err);
        }
        cb(err);
    })
}

// Handles reading raw stdin
var finishedStdin = false;
function produceFromInput(cb) {
    console.log("Ready to write messages. Enter one per line. EOF ends production and exits.")

    var stdin = process.stdin;
    stdin.setEncoding('utf8');

    var outstanding = "";
    stdin.on('readable', function() {
        var chunk = stdin.read();
        if (chunk == null) return;
        outstanding += chunk;
        outstanding = processInput(outstanding, cb);
    });
    stdin.on('end', function() {
        finishedStdin = true;
        // Make sure the last line gets processed even if it was only terminated by EOF
        if (outstanding.length > 0 && outstanding[outstanding.length-1] != '\n')
            outstanding += '\n';
        outstanding = processInput(outstanding, cb);
        // Since there may not have been outstanding work, we need to check immediately if it's safe to exit.
        checkSendingComplete(cb);
    });
}

var num_messages = 0;
var num_bytes = 0;
// Splits input by lines into individual messages and passes them to the producer. Tracks stats to print at exit.
function processInput(buffer, cb) {
    if (buffer.length == 0) return;
    var split_char = '\n';
    var lines = buffer.split(split_char);
    // If there are any line splits, the below logic always works, but if there are none we need to detect this and skip
    // any processing.
    if (lines[0].length == buffer.length) return buffer;
    // Note last item is ignored since it is the remainder (or empty)
    for(var i = 0; i < lines.length-1; i++) {
        var line = lines[i];
        target.produce(line, handleProduceResponse.bind(undefined, cb));
        // OR with key or partition:
        //target.produce({'partition': 0, 'value': line}, handleProduceResponse.bind(undefined, cb));
        //target.produce({'key': 'console', 'value': line}, handleProduceResponse.bind(undefined, cb));
        num_messages += 1;
        num_bytes += line.length;
    }
    return lines[lines.length-1];
}

var num_responses = 0;
var num_messages_acked = 0;
// Handles produce responses
function handleProduceResponse(cb, err, res) {
    num_responses += 1;
    if (err) {
        console.log("Error producing message: " + err);
    } else {
        num_messages_acked += 1;
    }

    // We can only indicate were done if stdin was closed and we have no outstanding messages.
    checkSendingComplete(cb);
}

function checkSendingComplete(cb) {
    if (finishedStdin && num_responses == num_messages)
        cb();
}

function reportStats(done) {
    console.log("Finished sending " + num_messages + " messages with a total of " + num_bytes + " bytes.");
    done();
}

async.series([checkTarget, produceFromInput, reportStats]);