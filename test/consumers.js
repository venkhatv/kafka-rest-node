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
    Consumers = require("../lib/consumers.js"),
    Client = require("../lib/client.js"),
    expect = chai.expect,
    should = chai.should();

chai.use(sinonChai);
describe("Consumers", function() {
    describe("Join api", function() {
        var consumers, consumer;

        beforeEach(function(done) {
            consumers = new Consumers({});
            consumer = consumers.group("consumer-group");
            done();
        });

        it("Consumer, joins a group succsesfully", function() {
            var consumerName = "consumer-name-1";
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, {
                        instance_id: consumerName,
                        base_uri: consumer.getPath() + "/" + consumerName
                    });
                });
            consumer.client.post = postStub;
            return consumer.join({}).then(function(consumerInstance) {
                expect(consumerInstance)
                    .to.have.property("id")
                    .equals(consumerName);
                expect(postStub.calledOnce).to.be.true;
                expect(postStub).to.be.calledWith(consumer.getPath(), {}, null);
            });
        });
        it("Consumer join fails and throws err", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumer.client.post = postStub;
            return consumer.join({}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
                expect(postStub.calledOnce).to.be.true;
            });
        });
    });

    describe("Subscribe method", function() {
        var consumers,
            consumer,
            consumerInstance,
            topic = "case-topic",
            consumerName = "consumer-name-1",
            streamString =
            "ConsumerStream{group=consumer-group, id=consumer-name-1, topic=case-topic}";

        before(function(done) {
            consumers = new Consumers({
                _validateVersionSupport: Client.prototype._validateVersionSupport,
                config: {
                    version: 1
                }
            });
            consumer = consumers.group("consumer-group");
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, {
                        instance_id: consumerName,
                        base_uri: consumer.getPath() + "/" + consumerName
                    });
                });
            consumer.client.post = postStub;

            consumer.join({}).then(function(consumerInstance1) {
                consumerInstance = consumerInstance1;
                done();
            });
        });

        it("Subscribe method version check success for v1", function() {
            var stream = consumerInstance.subscribe(topic, {});
            expect(stream.toString()).to.equal(streamString);
        });

        it("Subscribe method version fails for v2", function() {
            consumerInstance.client.config.version = 2;
            var stream = consumerInstance.subscribe(topic, {});
            expect(stream).to.be.null;
        });
    });

    describe("Subcription api", function() {
        var consumers,
            consumer,
            consumerInstance,
            topic = "case-topic",
            topics = {
                topics: ["case-topic"]
            },
            consumerName = "consumer-name-1",
            streamString =
            "ConsumerStream{group=consumer-group, id=consumer-name-1, topics=case-topic}";

        before(function(done) {
            consumers = new Consumers({
                _validateVersionSupport: Client.prototype._validateVersionSupport,
                config: {
                    version: 2
                }
            });
            consumer = consumers.group("consumer-group");
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, {
                        instance_id: consumerName,
                        base_uri: consumer.getPath() + "/" + consumerName
                    });
                });
            consumer.client.post = postStub;

            consumer.join({}).then(function(consumerInstance1) {
                consumerInstance = consumerInstance1;
                done();
            });
        });

        afterEach(function() {
            consumerInstance.client.config.version = 2;
        });

        it("Subscription method version fails for v1", function() {
            consumerInstance.client.config.version = 1;
            return consumerInstance.subscription(topics, {}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("Subscription is successfull", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback();
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.subscription(topics, {}).then(function(stream) {
                expect(stream.toString()).to.equal(streamString);
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/subscription"
                );
                expect(postStub.getCall(0).args[1]).to.deep.equal(topics);
            });
        });
        it("Subscription api fails and throws err", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.subscription(topics, {}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
                expect(postStub.calledOnce).to.be.true;
            });
        });
        it("Get Subscription is successfull", function() {
            var getStub = sinon.stub().callsFake(function(path, callback) {
                callback(null, topics);
            });
            consumerInstance.client.request = getStub;
            return consumerInstance.getSubscription().then(function(details) {
                expect(details).to.deep.equal(topics);
                expect(getStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/subscription"
                );
            });
        });

        it("Cancel Subscription is successfull", function() {
            var delStub = sinon.stub().callsFake(function(path, callback) {
                callback(null, true);
            });
            consumerInstance.client.delete = delStub;
            return consumerInstance.cancelSubscription().then(function(resp) {
                console.log('promise resolved');
                expect(resp).to.equal(true);
                expect(delStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/subscription"
                );
            });
        });
    });

    describe("Other Consumer apis", function() {
        var consumers,
            consumer,
            consumerInstance,
            topic = "case-topic",
            topics = {
                topics: ["case-topic"]
            },
            consumerName = "consumer-name-1",
            streamString =
            "ConsumerStream{group=consumer-group, id=consumer-name-1, topics=case-topic}";

        before(function(done) {
            consumers = new Consumers({
                _validateVersionSupport: Client.prototype._validateVersionSupport,
                config: {
                    version: 2
                }
            });
            consumer = consumers.group("consumer-group");
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, {
                        instance_id: consumerName,
                        base_uri: consumer.getPath() + "/" + consumerName
                    });
                });
            consumer.client.post = postStub;

            consumer.join({}).then(function(consumerInstance1) {
                consumerInstance = consumerInstance1;
                done();
            });
        });

        afterEach(function() {
            consumerInstance.client.config.version = 2;
        });

        it("Commit method success for v1", function() {
            consumerInstance.client.config.version = 1;
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.commit().then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/offsets"
                );
            });
        });
        it("Commit method failure", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.commit().catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("post offset method success for v1", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.offset({}).then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/offsets"
                );
            });
        });
        it("post offset method failure", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.offset({}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("post assignment method success for v1", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.assign({}).then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/assignments"
                );
            });
        });
        it("post assignment method failure", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.assign({}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("post position method success for v1", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.position({}).then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/positions"
                );
            });
        });
        it("post position method success for v1, beginning", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.position({}, "beginning").then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/positions/beginning"
                );
            });
        });
        it("post position method success for v1, end", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, undefined);
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.position({}, "end").then(function(resp) {
                expect(postStub.calledOnce).to.be.true;
                expect(postStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/positions/end"
                );
            });
        });
        it("post position method failure", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.position({}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });
        it("post position method failure, for wrong position value", function() {
            var postStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback();
                });
            consumerInstance.client.post = postStub;
            return consumerInstance.position({}, "xyPosition").catch(function(err) {
                expect(postStub.calledOnce).to.be.false;
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("Get Assignemnt is successfull", function() {
            var getStub = sinon.stub().callsFake(function(path, callback) {
                callback(null, {});
            });
            consumerInstance.client.request = getStub;
            return consumerInstance.getAssignment().then(function(details) {
                expect(details).to.deep.equal({});
                expect(getStub.calledOnce).to.be.true;
                expect(getStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/assignments"
                );
            });
        });

        it("Get Assignment method failure", function() {
            var requestStub = sinon.stub().callsFake(function(path, callback) {
                callback(new Error("Fail"));
            });
            consumerInstance.client.request = requestStub;
            return consumerInstance.getAssignment().catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });

        it("Get Offset is successfull", function() {
            var getStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(null, {});
                });
            consumerInstance.client.get = getStub;
            return consumerInstance.getOffset({}).then(function(details) {
                expect(details).to.deep.equal({});
                expect(getStub.calledOnce).to.be.true;
                expect(getStub.getCall(0).args[0]).to.equal(
                    consumerInstance.getUri() + "/offsets"
                );
            });
        });

        it("Get Offset method failure", function() {
            var getStub = sinon
                .stub()
                .callsFake(function(path, body, schema, callback) {
                    callback(new Error("Fail"));
                });
            consumerInstance.client.get = getStub;
            return consumerInstance.getOffset({}).catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
            });
        });
    });
});
