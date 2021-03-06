/**
 * Copyright 2015 Confluent Inc.
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

var Schema = require('./schema'),
    utils = require('./utils'),
    util = require('util');

/**
 * JsonSchema is a dummy Schema implementation whose only purpose is to
 * provide content type information for requests.
 */
var JsonSchema = module.exports = function() {
};
util.inherits(JsonSchema, Schema);

JsonSchema.prototype.toSchemaString = function() {
    return null;
};

JsonSchema.prototype.getContentType = function(client) {
    return "application/vnd.kafka.json.v" + client.config.version + "+json";
};

JsonSchema.getContentType = JsonSchema.prototype.getContentType;

JsonSchema.decodeMessage = function(msg) {
    return msg;
};