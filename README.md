# slock-lua-nginx

High-performance distributed sync service and atomic DB. Provides good multi-core support through lock queues, high-performance asynchronous binary network protocols. Can be used for spikes, synchronization, event notification, concurrency control. https://github.com/snower/slock

# Conf Example

```conf
lua_package_path "lib/resty/?.lua;";

init_worker_by_lua_block {
        local slock = require("slock")
        slock:connect("lock1", "127.0.0.1", 5658)
}

server {
        listen 8080;
        default_type text/html;

        location /echo {
                echo "hello world";
        }

        location /lock {
                content_by_lua_block {
                        local slock = require("slock")
                        local client = slock:get("lock1")
                        local lock_key = "test"
                        local args = ngx.req.get_uri_args()
                        for key, val in pairs(args) do
                                if key == "lock_key" then
                                        lock_key = val
                                end
                        end
                        local lock = client:newLock(lock_key, 5, 3)
                        local ok, err = lock:acquire()
                        if not ok then
                                ngx.say("acquire error:" .. err)
                        else
                                ngx.ctx.lock1 = lock
                                ngx.say("<p>hello, world</p>")
                        end
                }

                log_by_lua_block {
                        local lock = ngx.ctx.lock1
                        if lock ~= nil then
                                local ok, err = lock:releasetry()
                                if not ok then
                                        ngx.log(ngx.ERR, "slock release error:" .. err)
                                end
                        end
                }
        }

        location /event {
                content_by_lua_block {
                        local slock = require("slock")
                        local client = slock:get("lock1")
                        local event_type = "clear"
                        local wait_type = ""
                        local event_key = "event"
                        local args = ngx.req.get_uri_args()
                        for key, val in pairs(args) do
                                if key == "event_type" then
                                        event_type = val;
                                end
                                if key == "wait_type" then
                                        wait_type = val;
                                end
                                if key == "event_key" then
                                        event_key = val;
                                end
                        end

                        local event = nil;
                        if event_type == "clear" then
                                event = client:newDefaultClearEvent(event_key, 60, 60)
                        else
                                event = client:newDefaultSetEvent(event_key, 60, 60)
                        end

                        if wait_type == "cycle" then
                                local ok, err = event:waitAndTimeoutRetryClear(60)
                                if not ok then
                                        ngx.say("cycle wait fail " .. err)
                                else
                                        ngx.say("cycle wait succed")
                                end
                        else
                                local ok, err = event:wait(60)
                                if not ok then
                                        ngx.say("wait fail " .. err)
                                else
                                        ngx.say("wait succed")
                                end
                        end

                }
        }

        location /flow/maxconcurrent {
                access_by_lua_block {
                        local slock = require("slock")
                        local client = slock:get("lock1")
                        local flow_key = "flow:maxconcurrent"
                        local args = ngx.req.get_uri_args()
                        for key, val in pairs(args) do
                                if key == "flow_key" then
                                        flow_key = val
                                end
                        end
                        local lock = client:newMaxConcurrentFlow(flow_key, 10, 5, 60)
                        local ok, err = lock:acquire()
                        if not ok then
                                ngx.say("acquire error:" .. err)
                                ngx.exit(ngx.HTTP_OK)
                        else
                                ngx.ctx.lock1 = lock
                        end
                }

                echo "hello world";

                log_by_lua_block {
                        local lock = ngx.ctx.lock1
                        if lock ~= nil then
                                local ok, err = lock:release()
                                if not ok then
                                        ngx.log(ngx.ERR, "slock release error:" .. err)
                                end
                        end
                }
        }

        location /flow/tokenbucket {
                access_by_lua_block {
                        local slock = require("slock")
                        local client = slock:get("lock1")
                        local flow_key = "flow:tokenbucket"
                        local args = ngx.req.get_uri_args()
                        for key, val in pairs(args) do
                                if key == "flow_key" then
                                        flow_key = val
                                end
                        end
                        local lock = client:newTokenBucketFlow(flow_key, 10, 5, 60)
                        local ok, err = lock:acquire()
                        if not ok then
                                ngx.say("acquire error:" .. err)
                                ngx.exit(ngx.HTTP_OK)
                        end
                }

                echo "hello world";
        }
}
```