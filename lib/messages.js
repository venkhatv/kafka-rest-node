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
 * Handles extraction and normalization of producer records from arguments to a produce() method.
 * @returns {Array}
 */
exports.normalizeFromArguments = function(args, allow_partition) {
    if (allow_partition === undefined) throw new Error("allow_partition must be specified");

    // Extract
    var arg_mode = null;
    var msgs = [];
    var res = null;
    for(var i = 0; i < args.length; i++) {
        var arg = args[i];
        if (typeof(arg) == "function") {
            if (res != null)
                throw new Error("Produce request contains multiple callbacks");
            res = arg;
        } else if (Array.isArray(arg)) {
            // Should only ever encounter a single array of msgs; also shouldn't encounter an array if we already saw individual messages
            if (arg_mode != null) {
                if (arg_mode == "array")
                    throw new Error("Produce request contains multiple arrays of messages");
                else
                    throw new Error("Produce request contains both arrays of messages and individual messages.");
            }
            msgs = arg;
        } else if (typeof(arg) == "object" || typeof(arg) == "string") {
            msgs.push(arg);
        }
    }

    // Validate/transform messages into standard format
    for(var i = 0; i < msgs.length; i++) {
        var key = undefined;
        var value = undefined;
        var partition = undefined;
        if (typeof(msgs[i]) == "string" || Buffer.isBuffer(msgs[i])) {
            value = msgs[i];
        }
        else {
            value = msgs[i].value;
            key = msgs[i].key;
            partition = msgs[i].partition;
        }
        if (value === undefined || value === null)
            throw new Error("Message " + i + " did not contain a value.");
        if (!allow_partition && partition !== undefined && partition != null)
            throw new Error("Message " + i + " contains a partition but this resource does not allow specifying the partition.")
        value = utils.toBase64(value);
        key = (key === undefined || key === null) ? null : utils.toBase64(key);
        if (allow_partition)
            msgs[i] = {
                'value': value,
                'key' : key,
                'partition': partition
            }
        else
            msgs[i] = {
                'value': value,
                'key' : key
            }
    }

    return [msgs, res];
}


exports.decode = function(raw) {
    return {
        'key': (raw.key === null ? null : utils.fromBase64(raw.key)),
        'value': utils.fromBase64(raw.value),
        'partition': raw.partition
    }
}