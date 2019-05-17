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
    Schema = require('./schema'),
    BinarySchema = require('./binarySchema'),
    AvroSchema = require('./avroSchema'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable,
    async = require('async');

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
var Consumer = Consumers.Consumer = function(client, groupName) {
    this.client = client;
    this.groupName = groupName;
    this.schemaClass = BinarySchema; // Default binary mode
}

/**
 * Creates a new consumer instance in this consumer group.
 * @param opts object dictionary containing flags to include in the
               request. Current options are: id (string), format (string),
               auto.offset.reset (string) and auto.commit.enable (string)
 * @param res function(err, res)
 */
Consumer.prototype.join = function(opts, res) {
    return new Promise((resolve, reject) => {
        if (opts && opts.format === 'avro')
            this.schemaClass = AvroSchema;
        // Note that this *doesn't* use the schema class to request a content type
        // since this interaction contains no embedded data.
        this.client.post(this.getPath(), opts, null, function(err, join_response) {
            if (err) return utils.getPromiseCallback(resolve, reject, res)(err);
            var ci = new ConsumerInstance(this, join_response);
            return utils.getPromiseCallback(resolve, reject, res)(null, ci);
        }.bind(this));
    });
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
var ConsumerInstance = Consumers.ConsumerInstance = function(consumer, raw) {
    EventEmitter.call(this);

    this.client = consumer.client;
    this.consumer = consumer;
    this.raw = raw;
    if (!this.id || !this.uri)
        throw new Error("ConsumerInstance response did not contain a required field.");

    this.active = true;
    this.streams = [];
}
util.inherits(ConsumerInstance, EventEmitter);

ConsumerInstance.prototype.__defineGetter__("id", function() {
    return this.raw.instance_id;
});

ConsumerInstance.prototype.__defineGetter__("uri", function() {
    return this.raw.base_uri;
});

ConsumerInstance.prototype.subscribe = function(topic, options) {
    var supportedVersions = [1];
    if (!this.client._validateVersionSupport(supportedVersions)) return null;
    var stream = new ConsumerStream(this, topic, options);
    this.streams.push(stream);
    return stream;
}

/**
 * Subscribes to the provided topics. On successfull subscription, calls calback with a stream instance
 * @param {Array} topics Array of topic name strings
 * @param {Object} options Object which can options for the stream 
 * E.g. {requestDelay : 2000} in ms, represents the polling interval for the stream.
 * @param {Function} res callback function 
 */
ConsumerInstance.prototype.subscription = function(topicsObj, options, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;    
        var stream;
        this.client.post(utils.urlJoin(this.getUri(), 'subscription'), topicsObj , null, function() {
            //There will always be one stream at a time in v2 api.
            // if(this.streams.length){
            //     this._shutdownStreams(function(err, response){
            //         if(err){
            //             console.log('Failed to shutdown Stream', err);
            //             utils.getPromiseCallback(resolve, reject, res)(err);
            //         }
            //         stream = new ConsumerStream(this, topicsObj, options);
            //         this.streams = [stream];
            //         utils.getPromiseCallback(resolve, reject, res)(null, stream);
            //     });
            // } else {
                stream = new ConsumerStream(this, topicsObj, options);
                this.streams.push(stream);
                utils.getPromiseCallback(resolve, reject, res)(null, stream);
            // }
        }.bind(this));
    });
}

ConsumerInstance.prototype.getSubscription = function(res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.request(utils.urlJoin(this.getUri(), 'subscription'), utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.cancelSubscription = function(res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.post(utils.urlJoin(this.getUri(), 'subscription'), function() {
            this._shutdownStreams(utils.getPromiseCallback(resolve, reject, res));
        }.bind(this));
    });
}

ConsumerInstance.prototype.commit = function(res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [1];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.post(this.getUri() + '/offsets', null, null, utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.offset = function(body, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        var offsetUrl = this.getUri() + '/offsets';
        this.client.post(offsetUrl, body, null, utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.getOffset = function(body, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.get(this.getUri() + '/offsets', body, null, utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.assign = function(body, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.post(this.getUri() + '/assignments', body, null, utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.getAssignment = function(body, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        this.client.request(this.getUri() + '/assignments', utils.getPromiseCallback(resolve, reject, res));
    });
}

/**
 * Method to override the fetch offsets that the consumer will use for the next set of records to fetch.
 * @param {Object} body body of the request
 * @param {String} position Position to seek for the given partitions in body. Possible values - '', null, 'beginning', 'end' 
 * @param {Function} res callback
 */
ConsumerInstance.prototype.position = function(body, position, res) {
    return new Promise((resolve, reject) => {
        var supportedVersions = [2];
        if (!this.client._validateVersionSupport(supportedVersions, utils.getPromiseCallback(resolve, reject, res))) return;
        if(position && ['beginning', 'end'].indexOf(position) === -1)
            utils.getPromiseCallback(resolve, reject, res)(new Error('Provide a valid position value or keep it blank'));
        var positionUrl = utils.urlJoin(this.getUri(), 'positions', position ? position : '');
        this.client.post(positionUrl, body, null, utils.getPromiseCallback(resolve, reject, res));
    });
}

ConsumerInstance.prototype.shutdown = function(res) {
    // Stop reading from all child streams first so we can be sure there will be no more events initiated by them, then
    // delete the consumer group
    async.series([
        this._shutdownStreams.bind(this),
        this._shutdownInstance.bind(this)
    ], res);
}

ConsumerInstance.prototype._shutdownStreams = function(res) {
    async.each(
        this.streams,
        function (stream, res) {
            stream.shutdown(res);
        },
        res
    );
}

ConsumerInstance.prototype._shutdownInstance = function(res) {
    this.client.delete(this.getUri(), function(err) {
        if (err) return res(err);
        this.active = false;
        this.emit('end');
        res(null);
    }.bind(this));
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




var ConsumerStream = Consumers.ConsumerStream = function(instance, topic, options) {
    Readable.call(this, {'objectMode' : true});

    this.client = instance.client;
    this.instance = instance;
    this.topic = topic;
    this.options = options || {};
    this.schemaClass = this.instance.consumer.schemaClass;

    this.active = true;
}
util.inherits(ConsumerStream, Readable);

ConsumerStream.prototype._read = function() {
    var acceptContentType = this.schemaClass.getContentType(this.client);
    this.client.request({'url' : this.getUri(), 'accept': acceptContentType}, function(err, msgs) {
        if (err) {
            // Unlike read, which is unconditionally duplicated to the parent instance, only emit error on the parent
            // if nothing catches it on the stream. Otherwise, an error caught on the child is also emitted on the parent
            // and throws an exception if it isn't also caught there.
            if (!this.emit('error', err))
                this.instance.emit('error', err);
        } else {
            if (msgs.length > 0) {
                var decoded_msgs = [];
                for(var i = 0; i < msgs.length; i++) {
                    decoded_msgs.push(this.schemaClass.decodeMessage(msgs[i]));
                }
                this.push(decoded_msgs);
                this.instance.emit('read', decoded_msgs);
            } else if (this.active) {
                if(this.options.requestDelay) {
                    setTimeout(this._read.bind(this), this.options.requestDelay);
                } else {
                    this._read();
                }
            }
            if (!this.active) {
                this.push(null);
            }
        }
    }.bind(this));
}

ConsumerStream.prototype.shutdown = function(res) {
    // Just mark as no longer active, read callback will clean up
    this.on('end', res);
    this.active = false;
}

ConsumerStream.prototype.toString = function() {
    var result = "ConsumerStream{group=" + this.instance.consumer.groupName;
    result += ", id=" + this.instance.id;
    result +=  this.client.config.version === 2 ?
        ", topics=" + this.topic.topic_pattern || this.topic.topics && this.topic.topics.join() :
        ", topic=" + this.topic;
    result += "}";
    return result;
}

ConsumerStream.prototype.getUri = function() {
    if (this.client.config.version === 2)
        return utils.urlJoin(this.instance.getUri(), "records");
    return utils.urlJoin(this.instance.getUri(), "topics", this.topic);
}
