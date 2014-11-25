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

var utils = require('./utils'),
    Partitions = require('./partitions');

var Topics = module.exports = function(client) {
    this.client = client;
}

/**
 * Request a list of topics and their metadata.
 * @param function(err, topics) callback returning the list of Topic objects with metadata
 */
Topics.prototype.list = function(res) {
    this.client.request("/topics", function(err, topics_response) {
        if (err) return res(err);
        var topics = [];
        for(var i = 0; i < topics_response.length; i++) {
            topics.push(new Topic(this.client, topics_response[i].name, topics_response[i]));
        }
        res(null, topics);
    }.bind(this));
}

/**
 * Request a single topic's metadata. Equivalent to topic = Topics.topic(name); topic.get(res); return topic;
 * @param name string the name of the topic
 * @param res function(err, topic)
 */
Topics.prototype.get = function(name, res) {
    var topic = this.topic(name);
    topic.get(res);
    return topic;
}

Topics.prototype.topic = function(name) {
    return new Topic(this.client, name);
}

/**
 * Create a new Topic object with the given name, optionally providing the raw
 */
var Topic = function(client, name, raw) {
    this.client = client;
    this.name = name;
    utils.mixin(this, raw);
    this.raw = raw;
}

/**
 * Request this topic's metadata.
 * @param res function(err, topic) callback where topic will be this (updated) Topic
 */
Topic.prototype.get = function(res) {
    this.client.request(this.getPath(), function(err, topic_raw) {
        if (err) return res(err);
        this.raw = topic_raw;
        res(null, this);
    }.bind(this));
}

/**
 * Get a Partitions resource representing the partitions for this topic. This does not request any metadata since you may
 * use the Partitions resource to get a specific partition; call list() on the resulting Partitions resource to get a
 * full listing with metadata.
 *
 * @param res function(err,
 */
Topic.prototype.__defineGetter__("partitions", function() {
    if (!this._partitions)
        this._partitions = new Partitions(this);
    return this._partitions;
});

/**
 * Helper to get a specific partition on a topic. Shorthand for topic.partitions.partition(id). Note that this does not
 * automatically retrieve the metadata for the partition.
 * @param id partition ID
 */
Topic.prototype.partition = function(id) {
    return this.partitions.partition(id);
}

Topic.prototype.toString = function() {
    var result = "Topic{name=\"" + this.name + "\"";
    if (this.raw) {
        result += ", partitions=" + this.raw.num_partitions;
    }
    result += "}";
    return result;
}

Topic.prototype.getPath = function() {
    return utils.urlPathJoin("topics", this.name);
}