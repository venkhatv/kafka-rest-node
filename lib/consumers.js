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
    messages = require('./messages'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter;

var Consumers = module.exports = function(client) {
    this.client = client;
}

Consumers.prototype.group = function(groupName) {
    return new Consumer(this.client, groupName);
}


/**
 * Represents a Consumer resource. This provides access to metadata about consumer groups. To join a consumer group, call
 * the join() method.
 * @param client
 * @param groupName
 * @constructor
 */
var Consumer = function(client, groupName) {
    this.client = client;
    this.groupName = groupName;
}

/**
 * Creates a new consumer instance in this consumer group.
 * @param res function(err, res)
 */
Consumer.prototype.join = function(res) {
    this.client.post(this.getPath(), null, function(err, join_response) {
        if (err) return res(err);
        var ci = new ConsumerInstance(this, join_response);
        res(null, ci);
    }.bind(this))
}

Consumer.prototype.getPath = function() {
    return utils.urlJoin("/consumers", this.groupName);
}

Consumer.prototype.toString = function() {
    return "Consumer{group=" + this.groupName + "}";
}


/**
 * A consumer instance that is part of a consumer group.
 * @param consumer
 * @param raw
 * @constructor
 */
var ConsumerInstance = function(consumer, raw) {
    EventEmitter.call(this);

    this.client = consumer.client;
    this.consumer = consumer;
    this.raw = raw;
    if (!this.id || !this.uri)
        throw new Error("ConsumerInstance response did not contain a required field.");
}
util.inherits(ConsumerInstance, EventEmitter);

ConsumerInstance.prototype.__defineGetter__("id", function() {
    return this.raw.instance_id;
});

ConsumerInstance.prototype.__defineGetter__("uri", function() {
    return this.raw.base_uri;
});

ConsumerInstance.prototype.subscribe = function(topic) {
    return new ConsumerStream(this, topic);
}

ConsumerInstance.prototype.toString = function() {
    var result = "ConsumerInstance{group=" + this.consumer.groupName;
    result += ", id=" + this.id;
    result += ", uri=" + this.uri;
    result += "}";
    return result;
}

ConsumerInstance.prototype.getUri = function() {
    return this.uri;
}




var ConsumerStream = function(instance, topic) {
    EventEmitter.call(this);

    this.client = instance.client;
    this.instance = instance;
    this.topic = topic;

    this.read();
}
util.inherits(ConsumerStream, EventEmitter);

ConsumerStream.prototype.read = function() {
    this.client.request(this.getUri(), function(err, msgs) {
        if (err) {
            this.emit('error', err);
            this.instance.emit('error', err)
        } else {
            if (msgs.length > 0) {
                var decoded_msgs = [];
                for(var i = 0; i < msgs.length; i++) {
                    decoded_msgs.push(messages.decode(msgs[i]));
                }
                this.emit('read', decoded_msgs);
                this.instance.emit('read', decoded_msgs);
            }
            this.read();
        }
    }.bind(this));
}

ConsumerStream.prototype.toString = function() {
    var result = "ConsumerStream{group=" + this.instance.consumer.groupName;
    result += ", id=" + this.instance.id;
    result += ", topic=" + this.topic;
    result += "}";
    return result;
}

ConsumerStream.prototype.getUri = function() {
    return utils.urlJoin(this.instance.getUri(), "topics", this.topic);
}
