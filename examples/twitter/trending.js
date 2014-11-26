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

/**
 * A simple consumer of tweets that periodically reports trending topics.
 */

var KafkaRest = require("../.."),
    argv = require('minimist')(process.argv.slice(2));

var api_url = argv.url || "http://localhost:8080";
var topicName = argv.topic;
var consumerGroup = argv.group;
var help = (argv.help || argv.h);

if (help || topicName === undefined) {
    console.log("Compute and report trending topics in tweets.");
    console.log();
    console.log("Usage: node trending.js [--url <api-base-url>] --topic <topic> [--group <consumer-group-name>]");
    process.exit(help ? 0 : 1);
}

if (consumerGroup === undefined)
    consumerGroup = "tweet-trending-consumer-" + Math.round(Math.random() * 100000);

var kafka = new KafkaRest({"url": api_url});
var consumer_instance;
// How often to report the top 10
var report_period = 10000;
// How much to discount the current weights for each report_period
var period_discount_rate = .99;

kafka.consumer(consumerGroup).join(function(err, ci) {
    if (err) return console.log("Failed to create instance in consumer group: " + err);
    consumer_instance = ci;
    var stream = consumer_instance.subscribe(topicName);
    stream.on('read', function(msgs) {
        for(var i = 0; i < msgs.length; i++) {
            var tweet = JSON.parse(msgs[i].value.toString('utf8'));
            processTweet(tweet);
        }
    });
    stream.on('error', function(err) {
        console.log("Consumer instance reported an error: " + err);
        shutdown();
    });

    process.on('SIGINT', shutdown);
});

// Implements a simple EWA scheme on lower-cased ngrams.
var topics = {};
function processTweet(tweet) {
    var words = tweet.text.toLowerCase().split(/\s/);

    // Filter blanks, common words
    var filtered_words = [];
    for(var i = 0; i < words.length; i++) {
        if (words[i].length > 0 && !stopwords[words[i]])
            filtered_words.push(words[i]);
    }

    // Generate ngrams and increment weights
    for(var i = 0; i < filtered_words.length; i++) {
        var ngram = filtered_words[i];
        for(var n = 1; n < 3; n++) {
            if (i + n < filtered_words.length) {
                ngram += " " + filtered_words[i + n];
                if (topics[ngram] === undefined)
                    topics[ngram] = {'topic': ngram, 'weight': 0};
                topics[ngram].weight += 1;
            }
        }
    }
}

// Setup period reporting, discounting of topic weights, and cleanup of small
var reportInterval = setInterval(function() {
    var sorted_terms = [];
    for(var topicKey in topics) {
        var topic = topics[topicKey];
        // Discounting won't affect sorting, so we can do this in the same pass
        topic.weight *= period_discount_rate;
        sorted_terms.push(topic);
    }
    sorted_terms.sort(function(a,b) { return (a.weight > b.weight ? -1 : (a.weight == b.weight ? 0 : 1)); });

    for(var i = 0; i < Math.min(10, sorted_terms.length); i++) {
        console.log("" + i + ". " + sorted_terms[i].topic + " \t(" + sorted_terms[i].weight + ")");
    }
    console.log();
}, report_period);

function shutdown() {
    consumer_instance.shutdown(function(err) {
        if (err) console.log("Error shutting down consumer instance: " + err);
    });
    clearInterval(reportInterval);
}



var stopwords = {};
(function() {
    var stopwords_str = "a about above after again against all am an and any are aren't as at be because been before being " +
        "below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few " +
        "for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him " +
        "himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't my " +
        "myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll " +
        "she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they " +
        "they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've " +
        "were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't " +
        "you you'd you'll you're you've your yours yourself yourselves";
    var stopwords_list = stopwords_str.split(' ');
    for(var i = 0; i < stopwords_list.length; i++)
        stopwords[stopwords_list[i]] = true;
})();
