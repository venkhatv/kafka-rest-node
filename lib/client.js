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

var Client = function(config) {
    this.config = {};
    this.config.url = config.url || Client.DEFAULTS.url;
    this.config.version = config.version || Client.DEFAULTS.version;
}

Client.prototype.DEFAULTS = {
    "url": "http://localhost:8080",
    "version": 1
}