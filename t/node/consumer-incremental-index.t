# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use t::APISIX 'no_plan';

repeat_each(1);
no_long_string();
no_root_location();

my $yaml_config = <<_EOC_;
apisix:
  enable_incremental_consumer_index: true
_EOC_

add_block_preprocessor(sub {
    my ($block) = @_;

    if (!$block->yaml_config) {
        $block->set_value("yaml_config", $yaml_config);
    }

    if (!$block->no_error_log && !$block->error_log) {
        $block->set_value("no_error_log", "[error]\n[alert]");
    }
});

run_tests;

__DATA__

=== TEST 1: enable key-auth on the route /echo
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code, body = t('/apisix/admin/routes/1',
                ngx.HTTP_PUT,
                [[{
                    "plugins": {
                        "key-auth": {}
                    },
                    "upstream": {
                        "nodes": {
                            "127.0.0.1:1980": 1
                        },
                        "type": "roundrobin"
                    },
                    "uri": "/echo"
                }]]
            )

            ngx.status = code
            ngx.say(body)
        }
    }
--- request
GET /t
--- response_body
passed



=== TEST 2: create consumer
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code, body = t('/apisix/admin/consumers',
                ngx.HTTP_PUT,
                [[{
                    "username": "jack"
                }]]
            )

            ngx.status = code
            ngx.say(body)
        }
    }
--- request
GET /t
--- response_body
passed



=== TEST 3: create the first credential for the consumer
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code, body = t('/apisix/admin/consumers/jack/credentials/cred-a',
                ngx.HTTP_PUT,
                [[{
                    "plugins": {
                        "key-auth": {"key": "first-secret"}
                    }
                }]]
            )

            ngx.status = code
            ngx.say(body)
        }
    }
--- request
GET /t
--- response_body
passed



=== TEST 4: create the second credential for the consumer
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code, body = t('/apisix/admin/consumers/jack/credentials/cred-b',
                ngx.HTTP_PUT,
                [[{
                    "plugins": {
                        "key-auth": {"key": "second-secret"}
                    }
                }]]
            )

            ngx.status = code
            ngx.say(body)
        }
    }
--- request
GET /t
--- response_body
passed



=== TEST 5: both credentials can authenticate
--- request
GET /echo
--- more_headers
apikey: second-secret
--- response_headers
x-consumer-username: jack
x-credential-identifier: cred-b



=== TEST 6: deleting one credential does not affect the other
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code = t('/apisix/admin/consumers/jack/credentials/cred-a', ngx.HTTP_DELETE)

            ngx.sleep(0.2)

            local keep_code = t('/echo', ngx.HTTP_GET, "", nil, {apikey = "second-secret"})
            local delete_code = t('/echo', ngx.HTTP_GET, "", nil, {apikey = "first-secret"})

            ngx.say(code)
            ngx.say(keep_code)
            ngx.say(delete_code)
        }
    }
--- request
GET /t
--- response_body
200
200
401



=== TEST 7: updating the top-level consumer updates derived credential metadata
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code, body = t('/apisix/admin/consumers',
                ngx.HTTP_PUT,
                [[{
                    "username": "jack",
                    "labels": {
                        "custom_id": "updated-id"
                    }
                }]]
            )

            ngx.status = code
            ngx.say(body)
        }
    }
--- request
GET /t
--- response_body
passed



=== TEST 8: the surviving credential sees the updated custom_id
--- request
GET /echo
--- more_headers
apikey: second-secret
--- response_headers
x-consumer-username: jack
x-credential-identifier: cred-b
x-consumer-custom-id: updated-id



=== TEST 9: consumers_kv returns the incrementally updated consumer view
--- config
    location /t {
        content_by_lua_block {
            local consumer_mod = require("apisix.consumer")

            ngx.sleep(0.2)

            local consumer_conf = consumer_mod.plugin("key-auth")
            local consumers = consumer_mod.consumers_kv("key-auth", consumer_conf, "key")
            local consumer = consumers["second-secret"]

            ngx.say(consumer.consumer_name)
            ngx.say(consumer.custom_id)
            ngx.say(consumer.credential_id)
        }
    }
--- request
GET /t
--- response_body
jack
updated-id
cred-b



=== TEST 10: deleting the top-level consumer invalidates the derived credential
--- config
    location /t {
        content_by_lua_block {
            local t = require("lib.test_admin").test
            local code = t('/apisix/admin/consumers/jack', ngx.HTTP_DELETE)

            ngx.sleep(0.2)

            local request_code = t('/echo', ngx.HTTP_GET, "", nil, {apikey = "second-secret"})

            ngx.say(code)
            ngx.say(request_code)
        }
    }
--- request
GET /t
--- response_body
200
401
