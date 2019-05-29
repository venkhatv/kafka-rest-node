/**
 * Copyright 2019 Nutanix Inc.
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

var chai = require("chai"),
    sinon = require("sinon"),
    sinonChai = require("sinon-chai"),
    Topics = require("../lib/topics.js"),
    Client = require("../lib/client.js"),
    expect = chai.expect,
    should = chai.should();

chai.use(sinonChai);

describe("Topics apis", function() {
    var topics,
        topic,
        topicName = "case-topic";

    before(function() {
        topics = new Topics({
            _validateVersionSupport: Client.prototype._validateVersionSupport,
            config: {
                version: 2
            }
        });
        topic = topics.topic(topicName);
    });

    it("Topics list fetch", function() {
        var requestStub = sinon.stub().callsFake(function(urlString, callback) {
            callback(null, [topicName]);
        });
        topics.client.request = requestStub;
        return topics.list().then(function(result) {
            expect(result).to.have.length(1);
            expect(requestStub.calledOnce).to.be.true;
            expect(requestStub).to.be.calledWith("/topics");
        });
    });
    it("Topics list fetch fails and throws err", function() {
        var requestStub = sinon.stub().callsFake(function(urlString, callback) {
            callback(new Error("Fail"));
        });
        topics.client.request = requestStub;
        return topics.list().catch(function(err) {
            should.exist(err);
            err.should.be.an.instanceOf(Error);
            expect(requestStub.calledOnce).to.be.true;
            expect(requestStub).to.be.calledWith("/topics");
        });
    });

    it("Topic Metadata fetch", function() {
        var requestStub = sinon.stub().callsFake(function(urlString, callback) {
            callback(null, {});
        });
        topic.client.request = requestStub;
        return topic.get().then(function(result) {
            expect(result).to.be.instanceOf(Topics.Topic);
            expect(requestStub.calledOnce).to.be.true;
            expect(requestStub).to.be.calledWith("/topics/" + topicName);
        });
    });
    it("Topic Metadata fetch fails and throws err", function() {
        var requestStub = sinon.stub().callsFake(function(urlString, callback) {
            callback(new Error("Fail"));
        });
        topic.client.request = requestStub;
        return topic.get().catch(function(err) {
            should.exist(err);
            err.should.be.an.instanceOf(Error);
            expect(requestStub.calledOnce).to.be.true;
            expect(requestStub).to.be.calledWith("/topics/" + topicName);
        });
    });

    it("Topic Produce msgs", function() {
        var postStub = sinon
            .stub()
            .callsFake(function(urlString, body, schema, callback) {
                callback(null, true);
            });
        topic.client.post = postStub;
        return topic.produce().then(function(result) {
            expect(postStub.calledOnce).to.be.true;
            expect(postStub).to.be.calledWith("/topics/" + topicName);
        });
    });
    it("Topic produce msg fails and throws err", function() {
        var postStub = sinon
            .stub()
            .callsFake(function(urlString, body, schema, callback) {
                callback(new Error("Fail"));
            });
        topic.client.post = postStub;
        return topic.produce().catch(function(err) {
            should.exist(err);
            err.should.be.an.instanceOf(Error);
            expect(postStub.calledOnce).to.be.true;
            expect(postStub).to.be.calledWith("/topics/" + topicName);
        });
    });
});