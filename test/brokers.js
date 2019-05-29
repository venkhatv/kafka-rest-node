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
    Brokers = require("../lib/brokers.js"),
    expect = chai.expect,
    should = chai.should();

chai.use(sinonChai);
describe("Brokers", function() {
    describe("List api", function() {
        var brokers;

        beforeEach(function(done) {
            brokers = new Brokers({});
            done();
        });

        it("Brokers fetch via promise", function() {
            var requestStub = sinon.stub().callsFake(function(urlString, callback) {
                callback(null, {
                    brokers: ["broker1"]
                });
            });
            brokers.client.request = requestStub;
            return brokers.list().then(function(result) {
                expect(result).to.have.length(1);
                expect(requestStub.calledOnce).to.be.true;
                expect(requestStub).to.be.calledWith("/brokers");
            });
        });

        it("Brokers fetch fail throws err", function() {
            var requestStub = sinon.stub().callsFake(function(urlString, callback) {
                callback(new Error("Fail"));
            });
            brokers.client.request = requestStub;
            return brokers.list().catch(function(err) {
                should.exist(err);
                err.should.be.an.instanceOf(Error);
                expect(requestStub.calledOnce).to.be.true;
                expect(requestStub).to.be.calledWith("/brokers");
            });
        });

        it("Brokers fetch via callvack", function(done) {
            var requestStub = sinon.stub().callsFake(function(urlString, callback) {
                callback(null, {
                    brokers: ["broker1"]
                });
            });
            brokers.client.request = requestStub;
            brokers.list(function(err, result) {
                expect(result).to.have.length(1);
                expect(requestStub.calledOnce).to.be.true;
                expect(requestStub).to.be.calledWith("/brokers");
                done();
            });
        });
    });
});
