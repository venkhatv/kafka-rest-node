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

var Brokers = require('./brokers'),
    Topics = require('./topics'),
    http = require('http'),
    url = require('url'),
    utils = require('./utils');

var Client = module.exports = function(config) {
    this.config = {};
    var base_url = config.url || this.DEFAULTS.url;
    this.config.url = url.parse(base_url);
    this.config.version = config.version || this.DEFAULTS.version;

    this.brokers = new Brokers(this);
    this.topics = new Topics(this);
}


Client.prototype.DEFAULTS = {
    "url": "http://localhost:8080",
    "version": 1
}

/**
 * Shorthand for Client.brokers().broker(id). Note that this does not request metadata so it can be chained to get
 * to nested resources.
 * @param id broker ID
 */
Client.prototype.broker = function(id) {
    return this.brokers.broker(id);
}

/**
 * Shorthand for Client.topics.topic(name). Note that this does not request metadata so it can be chained to get to
 * nested resources, e.g. Client.topic(name).partition(id).
 * @param name topic name
 */
Client.prototype.topic = function(name) {
    return this.topics.topic(name);
}

/**
 * Shorthand for Client.topics.topic(name).partitions.partition(id). Note that this does not request metadata so it
 * can be used to get to nested resources and for producing messages.
 * @param name topic name
 * @param id partition ID
 */
Client.prototype.topicPartition = function(name, id) {
    return this.topics.topic(name).partitions.partition(id);
}

/**
 * Performs a request against the API. This is a low-level convenience wrapper making it easy to override a few
 * key parameters like the path and HTTP method. It also catches some common HTTP errors (e.g. 404) and converts
 * them into user-friendly errors.
 *
 * opts can contain overrides for method, path, a request body. Alternatively, if you only need to override the path
 * it can be specified directly.
 *
 * @param opts
 * @param res callback of form function(error, result)
 */
Client.prototype.request = function(opts, res) {
    if (typeof opts == "string")
        opts = {"path": opts}

    var reqOpts = utils.shallowCopy(this.config.url);
    reqOpts.method = opts.method || 'GET';
    reqOpts.path = utils.urlPathJoin(reqOpts.path, opts.path);

    var req = http.request(reqOpts, this._handleRequestResponse.bind(this, res));
    if (opts.body && (method == 'POST' || method == 'PUT'))
        req.write(opts.body);
    req.end();

    req.on('error', this._handleRequestError.bind(this, res));
}

Client.prototype._handleRequestResponse = function(cb, res) {
    if (res.statusCode >= 400)
        return res(res.statusCode);

    var data = '';
    res.on('data', function(chunk) {
        data += chunk;
    });
    res.on('end', function() {
        var parsed = JSON.parse(data);
        cb(null, parsed);
    });
}

Client.prototype._handleRequestError = function(cb, e) {
    cb(e);
}
