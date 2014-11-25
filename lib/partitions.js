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

var utils = require('./utils');

/**
 * Partitions resource, scoped to a single Topic.
 * @type {Function}
 */
var Partitions = module.exports = function(topic) {
    this.client = topic.client;
    this.topic = topic;
}

/**
 * Request a list of partitions and their metadata.
 * @param res function(err, partitions)
 */
Partitions.prototype.list = function(res) {
    this.client.request(utils.urlPathJoin(this.topic.getPath(), "partitions"), function(err, partitions_response) {
        if (err) return res(err);
        var partitions = [];
        for(var i = 0; i < partitions_response.length; i++) {
            partitions.push(new Partition(this.client, this.topic, /* id == index */i, partitions_response[i]));
        }
        res(null, partitions);
    }.bind(this));
}

/**
 * Request a single partition's metadata. Equivalent to partition = topic.partition(id); partition.get(res); return partition;
 * @param id partition ID
 * @param res function(err, partition)
 */
Partitions.prototype.get = function(id, res) {
    var partition = this.partition(id);
    partition.get(res);
    return partition;
}

Partitions.prototype.partition = function(id) {
    return new Partition(this.client, this.topic, id);
}


var Partition = function(client, topic, id, raw) {
    this.client = client;
    this.topic = topic;
    this.id = id;
    this.raw = raw;
}

Partition.prototype.get = function(res) {
    this.client.request(this.getPath(), function(err, partition_raw) {
        if (err) return res(err);
        this.raw = partition_raw;
        res(null, this);
    }.bind(this));
}

Partition.prototype.toString = function() {
    var result = "Partition{topic=\"" + this.topic.name + "\"" +
                ", id=" + this.id;
    if (this.raw) {
        result += ", leader=" + this.raw.leader;
        result += ", replicas=" + this.raw.replicas.length;
    }
    result += "}";
    return result;
}

Partition.prototype.getPath = function() {
    return utils.urlPathJoin(this.topic.getPath(), "partitions", this.id.toString());
}