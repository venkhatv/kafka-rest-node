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

var messages = require('../lib/messages');

// Test data
var MSG = 'foo';
var MSG_ENC = 'Zm9v';
var KEY = 'key';
var KEY_ENC = 'a2V5';
var CB = function() {}

// Test helpers
var assertMessageEquals = function(test, msg, value, key, partition) {
    test.strictEqual(msg.value, value);
    test.strictEqual(msg.key, key);
    test.strictEqual(msg.partition, partition);
}

exports.testNormalizeFromArguments = {

    // Test formats of a bunch of individual messages

    testNullMessage: function(test) {
        var result = messages.normalizeFromArguments([null], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], null, null, null);
    },

    testStringMessage: function(test) {
        var result = messages.normalizeFromArguments([MSG], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.done();
    },

    testBufferMessage: function(test) {
        var result = messages.normalizeFromArguments([new Buffer(MSG)], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.done();
    },

    testObjectMessage: function(test) {
        var result = messages.normalizeFromArguments([{'value': MSG}], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.done();
    },

    testObjectMessageWithKey: function(test) {
        var result = messages.normalizeFromArguments([{'value': MSG, 'key': KEY}], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, KEY_ENC, undefined);
        test.done();
    },

    testObjectMessageWithPartition: function(test) {
        var result = messages.normalizeFromArguments([{'value': MSG, 'partition': 0}], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, 0);
        test.done();
    }
    ,

    testObjectMessageWithPartitionNotAllowed: function(test) {
        test.throws(function() {
            messages.normalizeFromArguments([{'value': MSG, 'partition': 0}], false);
        });
        test.done();
    },

    testObjectMessageWithKeyAndPartition: function(test) {
        var result = messages.normalizeFromArguments([{'value': MSG, 'key': KEY, 'partition': 0}], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 1);
        assertMessageEquals(test, msgs[0], MSG_ENC, KEY_ENC, 0);
        test.done();
    },



    // Make sure we can parse lists of messages/callbacks and generate correct errors. Not exhaustive on all types of
    // message formats

    testMultiple: function(test) {
        var result = messages.normalizeFromArguments([MSG, MSG, MSG], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 3);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.done();
    },

    testMultipleWithCallback: function(test) {
        var result = messages.normalizeFromArguments([MSG, MSG, MSG, CB], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 3);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.strictEqual(cb, CB);
        test.done();
    },

    testMultipleCallbackError: function(test) {
        test.throws(function() {
            messages.normalizeFromArguments([MSG, MSG, MSG, CB, CB], true);
        });
        test.done();
    },


    testMultipleAsList: function(test) {
        var result = messages.normalizeFromArguments([[MSG, MSG, MSG]], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 3);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.done();
    },

    testMultipleAsListWithCallback: function(test) {
        var result = messages.normalizeFromArguments([[MSG, MSG, MSG], CB], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 3);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        test.strictEqual(cb, CB);
        test.done();
    },


    // And test a few mixed types to sanity check combinations
    testMixed: function(test) {
        var result = messages.normalizeFromArguments([MSG, new Buffer(MSG), {'value':MSG,'key':KEY}, {'value':MSG,'partition':0}, CB], true),
            msgs = result[0], cb = result[1];
        test.strictEqual(msgs.length, 4);
        assertMessageEquals(test, msgs[0], MSG_ENC, null, undefined);
        assertMessageEquals(test, msgs[1], MSG_ENC, null, undefined);
        assertMessageEquals(test, msgs[2], MSG_ENC, KEY_ENC, undefined);
        assertMessageEquals(test, msgs[3], MSG_ENC, null, 0);
        test.strictEqual(cb, CB);
        test.done();
    }
}