local sub = string.sub
local byte = string.byte
local tab_insert = table.insert
local tab_remove = table.remove
local tcp = ngx.socket.tcp
local null = ngx.null
local type = type
local pairs = pairs
local setmetatable = setmetatable
local tonumber = tonumber
local tostring = tostring
local rawget = rawget
local select = select
local semaphore = require "ngx.semaphore"

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 55)
local _MetaM = { __index = _M }
local _clients = {}
local _inited = false

_M._VERSION = '0.0.1'

local MAGIC = 0x56
local VERSION = 0x01

local COMMAND_TYPE_INIT = 0
local COMMAND_TYPE_LOCK = 1
local COMMAND_TYPE_UNLOCK = 2
local COMMAND_TYPE_PING = 5

local RESULT_SUCCED = 0
local RESULT_UNKNOWN_MAGIC = 1
local RESULT_UNKNOWN_VERSION = 2
local RESULT_UNKNOWN_DB = 3
local RESULT_UNKNOWN_COMMAND = 4
local RESULT_LOCKED_ERROR = 5
local RESULT_UNLOCK_ERROR = 6
local RESULT_UNOWN_ERROR = 7
local RESULT_TIMEOUT = 8
local RESULT_EXPRIED = 9
local RESULT_STATE_ERROR = 10
local RESULT_ERROR = 11

math.randomseed(os.time())
function random_bytes(size)
    local b = ''
    for i = 1, size do
        b = b .. string.char(math.random(0, 255))
    end
    return b
end

local ENCODE_HEXS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}
local DECODE_HEXS = {['0']=0, ['1']=1, ['2']=2, ['3']=3, ['4']=4, ['5']=5, ['6']=6, ['7']=7, ['8']=8, ['9']=9, ['A']=10, ['B']=11, ['C']=12, ['D']=13, ['E']=14, ['F']=15}

function hex_encode(s)
    local es = ''
    for i = 1, #s do
        local b = string.byte(string.sub(s, i, i))
        local ub = math.floor(b / 16)
        es = es .. ENCODE_HEXS[ub + 1] .. ENCODE_HEXS[(b % 16) + 1]
    end
    return es
end

function hex_decode(es)
    if (#es % 2) ~= 0 then
        return nil, 'hex string len error'
    end

    local s = ''
    for i = 1, #es, 2 do
        local ubit = DECODE_HEXS[string.upper(string.sub(es, i, i))]
        if ubit == nil then
            return nil, 'unknown char'
        end

        local lbit = DECODE_HEXS[string.upper(string.sub(es, i + 1, i + 1))]
        if lbit == nil then
            return nil, 'unknown char'
        end

        s = s .. string.char(ubit * 16 + lbit)
    end
    return s
end

function uint16_to_bin(i)
    if i > 0xffff or i < 0 then
        return nil, 'out max size'
    end

    local ui = math.floor(i / 256)
    return string.char(i % 256) .. string.char(ui)
end

function bin_to_uint16(b)
    if #b ~= 2 then
        return nil, 'size error'
    end

    return string.byte(string.sub(b, 1, 1)) + string.byte(string.sub(b, 2, 2)) * 256
end

function uint32_to_bin(i)
    if i > 0xffffffff or i < 0 then
        return nil, 'out max size'
    end

    local lbit, msg = uint16_to_bin(i % 65536)
    if lbit == nil then
        return nil, msg
    end

    local ui = math.floor(i / 65536)
    local ubit, msg = uint16_to_bin(ui)
    if ubit == nil then
        return nil, msg
    end
    
    return lbit .. ubit
end

function bin_to_uint32(b)
    if #b ~= 4 then
        return nil, 'size error'
    end

    local li, msg =  bin_to_uint16(string.sub(b, 1, 2))
    if li == nil then
        return nil, msg
    end

    local ui = bin_to_uint16(string.sub(b, 3, 4))
    if ui == nil then
        return nil, msg
    end
    return li + ui * 65536
end

local lock_id_index = 0
function gen_lock_id()
    lock_id_index = lock_id_index + 1
    if lock_id_index >= 0xffffffff then
        lock_id_index = 0
    end
    return uint32_to_bin(os.time()) .. uint32_to_bin(lock_id_index) .. random_bytes(8)
end

local request_id_index = 0
function gen_request_id()
    request_id_index = request_id_index + 1
    if request_id_index >= 0xffffffff then
        request_id_index = 0
    end
    return uint32_to_bin(os.time()) .. uint32_to_bin(request_id_index) .. random_bytes(8)
end

function format_key(key)
    if #key == 16 then
        return key
    end

    if #key < 16 then
        local padding = ''
        for i = 1, 16 - #key do
            padding = padding .. string.char(0)
        end
        return padding .. key
    end

    if #key == 32 then
        local d, err = hex_decode(key)
        if d ~= nil then
            return d
        end
    end

    return ngx.md5_bin(key)
end

local Lock = new_tab(0, 55)
local _MetaLock = { __index = Lock }

function Lock.new(self, db, lock_key, timeout, expried, lock_id, max_count, reentrant_count) 
    lock_key = format_key(lock_key)
    if lock_id == nil or #lock_id == 0 then
        lock_id = gen_lock_id()
    else
        lock_id = format_key(lock_id)
    end

    if max_count ~= nil and max_count > 0 then
        max_count = max_count -1
    end
    if reentrant_count ~= nil and reentrant_count > 0 then
        reentrant_count = reentrant_count -1
    end

    return setmetatable({
        _db = db,
        _lock_key = lock_key,
        _timeout = timeout or 0,
        _expried = expried or 0,
        _lock_id = lock_id or gen_lock_id(),
        _max_count = max_count or 0,
        _reentrant_count = reentrant_count or 0
    }, _MetaLock)
end

function Lock.acquire(self)
    local result, err = self._db._client:command({
        command = COMMAND_TYPE_LOCK,
        request_id = gen_request_id(),
        flag = 0,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = self._timeout,
        expried = self._expried,
        count = self._max_count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return false, err
    end

    if result.result ~= 0 then
        return false, "errcode:" .. result.result, result
    end
    return true, "", result
end

function Lock.release(self)
    local result, err = self._db._client:command({
        command = COMMAND_TYPE_UNLOCK,
        request_id = gen_request_id(),
        flag = 0,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = self._timeout,
        expried = self._expried,
        count = self._max_count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return false, err
    end

    if result.result ~= 0 then
        return false, "errcode:" .. result.result, result
    end
    return true, "", result
end

function Lock.releasetry(self)
    local ok, err = self._db._client:push({
        command = COMMAND_TYPE_UNLOCK,
        request_id = gen_request_id(),
        flag = 0,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = self._timeout,
        expried = self._expried,
        count = self._max_count,
        rcount = self._reentrant_count,
    })
    return ok, err
end

function Lock.show(self)
    local result, err = self._db._client:command({
        command = COMMAND_TYPE_LOCK,
        request_id = gen_request_id(),
        flag = 0x01,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = 0,
        expried = 0,
        count = 0,
        rcount = 0,
    })
    if result == nil then
        return false, err
    end

    if result.result ~= RESULT_UNOWN_ERROR then
        return false, "errcode:" .. result.result, result
    end
    return true, "", result
end

function Lock.update(self)
    local result, err = self._db._client:command({
        command = COMMAND_TYPE_LOCK,
        request_id = gen_request_id(),
        flag = 0x02,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = self._timeout,
        expried = self._expried,
        count = self._max_count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return false, err
    end

    if result.result ~= 0 and result.result ~= RESULT_LOCKED_ERROR then
        return false, "errcode:" .. result.result, result
    end
    return true, "", result
end

function Lock.releaseHead(self)
    local result, err = self._db._client:command({
        command = COMMAND_TYPE_UNLOCK,
        request_id = gen_request_id(),
        flag = 0x01,
        db_id = self._db._db_id,
        lock_id = self._lock_id,
        lock_key = self._lock_key,
        timeout = self._timeout,
        expried = self._expried,
        count = self._max_count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return false, err
    end

    if result.result ~= 0 then
        return false, "errcode:" .. result.result, result
    end
    return true, "", result
end

local Event = new_tab(0, 55)
local _MetaEvent = { __index = Event }

function Event.new(self, db, event_key, timeout, expried, default_seted) 
    event_key = format_key(event_key)

    return setmetatable({
        _db = db,
        _event_key = event_key,
        _timeout = timeout or 0,
        _expried = expried or 0,
        _default_seted = default_seted or false,
        _event_lock = nil,
        _check_lock = nil,
        _wait_lock = nil
    }, _MetaEvent)
end

function Event.clear(self)
    if self._default_seted then
        if self._event_lock == nil then
            self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 0, 0)
        end

        local ok, err, result = self._event_lock:update()
        if ok then
            return true, "", result
        end
        return false, err, result
    end


    if self._event_lock == nil then
        self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 2, 0)
    end

    local ok, err, result = self._event_lock:release()
    if ok then
        return true, "", result
    end
    if result ~= nil and result.result == RESULT_UNLOCK_ERROR then
        return true, "", result
    end
    return false, err, result
end

function Event.set(self)
    if self._default_seted then
        if self._event_lock == nil then
            self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 0, 0)
        end

        local ok, err, result = self._event_lock:release()
        if ok then
            return true, "", result
        end
        if result ~= nil and result.result == RESULT_UNLOCK_ERROR then
            return true, "", result
        end
        return false, err, result
    end


    if self._event_lock == nil then
        self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 2, 0)
    end

    local ok, err, result = self._event_lock:update()
    if ok then
        return true, "", result
    end
    return false, err, result
end

function Event.isSet(self)
    if self._default_seted then
        self._event_lock = Lock:new(self._db, self._event_key, 0, 0, nil, 0, 0)
        local ok, err, result = self._event_lock:acquire()
        if ok then
            return true, "", result
        end
        return false, err, result
    end

    self._event_lock = Lock:new(self._db, self._event_key, 0x02000000, 0, nil, 2, 0)
    local ok, err, result = self._event_lock:acquire()
    if ok then
        return true, "", result
    end
    return false, err, result
end

function Event.wait(self, timeout)
    if self._default_seted then
        self._wait_lock = Lock:new(self._db, self._event_key, timeout, 0, nil, 0, 0)
        local ok, err, result = self._wait_lock:acquire()
        if ok then
            return true, "", result
        end
        return false, err, result
    end

    timeout = bit.bor(timeout, 0x02000000)
    self._wait_lock = Lock:new(self._db, self._event_key, timeout, 0, nil, 2, 0)
    local ok, err, result = self._wait_lock:acquire()
    if ok then
        return true, "", result
    end
    return false, err, result
end

function Event.waitAndTimeoutRetryClear(self, timeout)
    if self._default_seted then
        self._wait_lock = Lock:new(self._db, self._event_key, timeout, 0, nil, 0, 0)
        local ok, err, result = self._wait_lock:acquire()
        if ok then
            return true, "", result
        end

        if result ~= nil and result.result == RESULT_TIMEOUT then
            if self._event_lock == nil then
                self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 0, 0)
            end

            local eok, eerr, eresult = self._event_lock:update()
            if eok and eresult ~= nil and eresult.result == RESULT_SUCCED then
                self._event_lock:releasetry()
                return true, "", result
            end
        end
        return false, err, result
    end
    
    timeout = bit.bor(timeout, 0x02000000)
    self._wait_lock = Lock:new(self._db, self._event_key, timeout, 0, nil, 2, 0)
    local ok, err, result = self._wait_lock:acquire()
    if ok then
        if self._event_lock == nil then
            self._event_lock = Lock:new(self._db, self._event_key, self._timeout, self._expried, self._event_key, 2, 0)
        end
        self._event_lock:releasetry()
        return true, "", result
    end
    return false, err, result
end

local MaxConcurrentFlow = new_tab(0, 55)
local _MetaMaxConcurrentFlow = { __index = MaxConcurrentFlow }

function MaxConcurrentFlow.new(self, db, flow_key, count, timeout, expried, require_aof) 
    flow_key = format_key(flow_key)
    if count == nil or count >= 0xffff then
        count = 0xffff
    end
    if expried < 1 then
        expried = bit.bor(math.ceil(expried * 1000), 0x04000000)
    else
        expried = math.ceil(expried)
    end
    local expried_flag = 0x02000000
    if require_aof then
        expried_flag = 0x01000000
    end

    return setmetatable({
        _db = db,
        _flow_key = flow_key,
        _count = count or 0xffff,
        _timeout = timeout or 5,
        _expried = expried or 60,
        _expried_flag = expried_flag,
        _lock = nil
    }, _MetaMaxConcurrentFlow)
end

function MaxConcurrentFlow.acquire(self)
    if self._lock == nil then
        local expried = bit.bor(self._expried, self._expried_flag)
        self._lock = Lock:new(self._db, self._flow_key, self._timeout, expried, nil, self._count, 0)
    end
    return self._lock:acquire()
end

function MaxConcurrentFlow.release(self)
    if self._lock == nil then
        return false, "unacquire"
    end
    return self._lock:releasetry()
end

local TokenBucketFlow = new_tab(0, 55)
local _MetaTokenBucketFlow = { __index = TokenBucketFlow }

function TokenBucketFlow.new(self, db, flow_key, count, timeout, period, require_aof) 
    flow_key = format_key(flow_key)
    if count == nil or count >= 0xffff then
        count = 0xffff
    end
    local expried_flag = 0x02000000
    if require_aof then
        expried_flag = 0x01000000
    end

    return setmetatable({
        _db = db,
        _flow_key = flow_key,
        _count = count or 0xffff,
        _timeout = timeout or 5,
        _period = period or 60,
        _expried_flag = expried_flag,
        _lock = nil
    }, _MetaTokenBucketFlow)
end

function TokenBucketFlow.acquire(self)
    ngx.update_time()
    local expried = 0
    if self._period < 3 then
        expried = bit.bor(math.ceil(self._period * 1000), self._expried_flag)
        expried = bit.bor(expried, 0x04000000)
        self._lock = Lock:new(self._db, self._flow_key, self._timeout, expried, nil, self._count, 0)
        return self._lock:acquire()
    end

    local now = ngx.time()
    expried = math.ceil(self._period) - (now % math.ceil(self._period))
    expried = bit.bor(expried, self._expried_flag)
    self._lock = Lock:new(self._db, self._flow_key, 0, expried, nil, self._count, 0)
    local ok, err, result = self._lock:acquire()
    if not ok and result ~= nil and result.result == RESULT_TIMEOUT then
        expried = bit.bor(math.ceil(self._period), self._expried_flag)
        self._lock = Lock:new(self._db, self._flow_key, self._timeout, expried, nil, self._count, 0)
        return self._lock:acquire()
    end
    return ok, err, result
end

local DataBase = new_tab(0, 55)
local _MetaDataBase = { __index = DataBase }

function DataBase.new(self, client, db_id) 
    return setmetatable({
        _client = client,
        _db_id = db_id
    }, _MetaDataBase)
end

function DataBase.newLock(self, lock_key, timeout, expried, lock_id, max_count, reentrant_count)
    return Lock:new(self, lock_key, timeout, expried, lock_id, max_count, reentrant_count)
end

function DataBase.newDefaultSetEvent(self, event_key, timeout, expried)
    return Event:new(self, event_key, timeout, expried, true)
end

function DataBase.newDefaultClearEvent(self, event_key, timeout, expried)
    return Event:new(self, event_key, timeout, expried, false)
end

function DataBase.newMaxConcurrentFlow(self, flow_key, count, timeout, expried, require_aof)
    return MaxConcurrentFlow:new(self, flow_key, count, timeout, expried, require_aof)
end

function DataBase.newTokenBucketFlow(self, flow_key, count, timeout, period, require_aof)
    return TokenBucketFlow:new(self, flow_key, count, timeout, period, require_aof)
end

local Client = new_tab(0, 55)
local _MetaClient = { __index = Client }

function Client.new(self, name, host, port) 
    return setmetatable({ 
        _name = name,
        _host = host, 
        _port = port, 
        _sock = nil,
        _client_id = gen_lock_id(),
        _dbs = {},
        _commands_waiter = semaphore.new(),
        _commands_head = nil,
        _commands_tail = nil,
        _results_waiter = {},
        _results = {},
        _closed = false
    }, _MetaClient)
end

function Client.connect(self)
    local sock, err = tcp()
    if not sock then
        return false, err
    end
    sock:settimeouts(5000, 60000, 0x7fffffff)

    local ok, err = sock:connect(self._host, self._port)
    if not ok then
        ngx.log(ngx.ERR, "slock connect error: " .. err)
        return false, err
    end
    
    self._sock = sock
    local err = self:init()
    if err ~= nil then
        sock:close()
        self._sock = nil
        return false, err
    end
    return true
end

function Client.reconnect(self)
    self._sock = nil

    while true
    do
        ngx.sleep(3)
        if self._closed then
            return
        end
        local ok, err = self:connect()
        if ok then
            return
        end
    end
end

function Client.close(self) 
    self._closed = true

    if self._sock ~= nil then
        local command = {
            command = COMMAND_TYPE_PING,
            request_id = gen_request_id()
        }

        if self._commands_head == nil then
            self._commands_head = command
            self._commands_tail = command
        else
            self._commands_tail._next = command
            self._commands_tail = command
        end
        self._commands_waiter:post(1)
    end
end

function Client.init(self) 
    data = string.char(MAGIC) .. string.char(VERSION) .. string.char(COMMAND_TYPE_INIT) .. gen_request_id() .. self._client_id
    for i=1,29,1 do
        data = data .. string.char(0)
    end

    local n, err = self._sock:send(data)
    if n == nil then
        if err == 'closed' then
            self:close()
        end
        return err
    end

    local data, err = self._sock:receive(64)
    if data == nil then
        return err
    end

    if #data ~= 64 then
        return 'data len error'
    end

    local result = {}
    if string.byte(string.sub(data, 1, 1)) ~= MAGIC then
        return 'magic error'
    end

    if string.byte(string.sub(data, 2, 2)) ~= VERSION then
        return 'unknown version'
    end
    
    local result = string.byte(string.sub(data, 20, 20))
    if result ~= 0 then
        return "result error code: " .. hex_encode(string.sub(data, 20, 20))
    end
    return nil
end

function Client.readCommand(self)
    if self._sock == nil then
        return nil, 'closed'
    end

    local data, err = self._sock:receive(64)
    if data == nil then
        return nil, err
    end

    if #data ~= 64 then
        return nil, 'data len error'
    end

    local result = {}
    if string.byte(string.sub(data, 1, 1)) ~= MAGIC then
        return nil, 'magic error'
    end

    if string.byte(string.sub(data, 2, 2)) ~= VERSION then
        return nil, 'unknown version'
    end
    
    result.command = string.byte(string.sub(data, 3, 3))
    result.request_id = string.sub(data, 4, 19)
    result.result = string.byte(string.sub(data, 20, 20))
    if result.command == COMMAND_TYPE_LOCK or result.command == COMMAND_TYPE_UNLOCK then
        result.flag = string.byte(string.sub(data, 21, 21))
        result.db_id = string.byte(string.sub(data, 22, 22))
        result.lock_id = string.sub(data, 23, 38)
        result.lock_key = string.sub(data, 39, 54)
        result.lcount = bin_to_uint16(string.sub(data, 55, 56))
        result.count = bin_to_uint16(string.sub(data, 57, 58))
        result.lrcount = string.byte(string.sub(data, 59, 59))
        result.rcount = string.byte(string.sub(data, 60, 60))
        return result
    end

    if result.command == COMMAND_TYPE_PING then
        return result
    end

    return result
end

function Client.writeCommand(self, command)
    if self._sock == nil then
        return 0, 'closed'
    end

    data = string.char(MAGIC) .. string.char(VERSION) .. string.char(command.command) .. command.request_id
    if command.command == COMMAND_TYPE_LOCK or command.command == COMMAND_TYPE_UNLOCK then
        data = data .. string.char(command.flag or 0) .. string.char(command.db_id) .. command.lock_id .. command.lock_key
        data = data .. uint32_to_bin(command.timeout or 0) .. uint32_to_bin(command.expried or 0)
        data = data .. uint16_to_bin(command.count or 0) .. string.char(command.rcount or 0)
    else
        if command.command == COMMAND_TYPE_PING then
            for i=1,45,1 do
                data = data .. string.char(0)
            end
        end
    end

    local n, err = self._sock:send(data)
    if n == nil then
        return 0, err
    end
    return n
end

function Client.command(self, command)
    if self._sock == nil then
        return nil, 'unconnected'
    end

    local waiter = semaphore.new()
    self._results_waiter[command.request_id] = waiter
    if self._commands_head == nil then
        self._commands_head = command
        self._commands_tail = command
    else
        self._commands_tail._next = command
        self._commands_tail = command
    end
    self._commands_waiter:post(1)
    local wait_timeout = 1
    if bit.band(command.timeout, 0x04000000) == 0 then
        wait_timeout = bit.band(command.timeout, 0xffff) + 1
    else
        wait_timeout = bit.band(math.floor(command.timeout / 1000)) + 1
    end
    ok, err = waiter:wait(wait_timeout)
    if self._results_waiter[command.request_id] ~= nil then
        self._results_waiter[command.request_id] = waiter
    end
    if ok then
        local result = self._results[command.request_id]
        if result ~= nil then
            self._results[command.request_id] = nil
            return result
        end
        return nil, "unknown result"
    end
        
    command._closed = true
    return nil, err or "timeout"
end

function Client.push(self, command)
    if self._sock == nil then
        return false, 'unconnected'
    end

    if self._commands_head == nil then
        self._commands_head = command
        self._commands_tail = command
    else
        self._commands_tail._next = command
        self._commands_tail = command
    end
    self._commands_waiter:post(1)
    return true
end

function Client.processRead(self)
    while not self._closed 
    do
        local result, err = self:readCommand()
        if result == nil then
            if err == "closed" then
                if self._closed then
                    self._commands_waiter:post(1)
                    return
                end
                self:reconnect()
            end

            if err ~= "timeout" then
                ngx.log(ngx.ERR, "slock read command error: " .. err)
            end
        else
            local waiter = self._results_waiter[result.request_id]
            if waiter ~= nil then
                self._results[result.request_id] = result
                self._results_waiter[result.request_id] = nil
                waiter:post(1)
            end
        end
    end

    if self._sock ~= nil then
        local ok, err = self._sock:close()
        if not ok then
            ngx.log(ngx.ERR, "slock close sockket error: " .. err)
        end
        self._sock = nil
    end
    self._commands_waiter:post(1)

    for request_id, waiter in pairs(self._results_waiter) do
        if waiter ~= nil then
            local result = {
                command = 0xff,
                request_id = request_id,
                result = 0x81
            }
            self._results[request_id] = result
            waiter:post(1)
        end
    end
    self._results_waiter = {}
end

function Client.processWrite(self)
    while not self._closed
    do
        local ok, err =  self._commands_waiter:wait(86400)
        if not ok then
            if err ~= "timeout" then
                ngx.log(ngx.ERR, "slock wait command error: " .. err)
            end
        else
            local command = self._commands_head
            if command ~= nil then
                self._commands_head = self._commands_head._next
                if self._commands_head == nil then
                    self._commands_tail = nil
                end

                if not command._closed then
                    local n, err = self:writeCommand(command)
                    if err ~= nil then
                        local waiter = self._results_waiter[command.request_id]
                        if waiter ~= nil then
                            local result = {
                                command = command.command,
                                request_id = command.request_id,
                                result = 0x81
                            }
                            self._results[command.request_id] = result
                            self._results_waiter[result.request_id] = nil
                            waiter:post(1)
                        end
                        ngx.log(ngx.ERR, "slock write command error: " .. err)
                    end
                end
            end
        end
    end
end

function Client.select(self, db_id)
    if self._dbs[db_id] == nil then
        local db = DataBase:new(self, db_id)
        self._dbs[db_id] = db
    end
    return self._dbs[db_id]
end

function Client.newLock(self, lock_key, timeout, expried)
    local db = self:select(0)
    return db:newLock(lock_key, timeout, expried, '', 0, 0)
end

function Client.newDefaultSetEvent(self, event_key, timeout, expried)
    local db = self:select(0)
    return db:newDefaultSetEvent(event_key, timeout, expried)
end

function Client.newDefaultClearEvent(self, event_key, timeout, expried)
    local db = self:select(0)
    return db:newDefaultClearEvent(event_key, timeout, expried)
end

function Client.newMaxConcurrentFlow(self, flow_key, count, timeout, expried, require_aof)
    local db = self:select(0)
    return db:newMaxConcurrentFlow(flow_key, count, timeout, expried, require_aof)
end

function Client.newTokenBucketFlow(self, flow_key, count, timeout, period, require_aof)
    local db = self:select(0)
    return db:newTokenBucketFlow(flow_key, count, timeout, period, require_aof)
end

local ReplsetClient = new_tab(0, 55)
local _MetaReplsetClient = { __index = ReplsetClient }

function ReplsetClient.new(self, name, hosts) 
    return setmetatable({ 
        _name = name,
        _hosts = hosts, 
        _clients = {},
        _closed = false
    }, _MetaReplsetClient)
end

function ReplsetClient.connect(self) 
    local doConnect = function(index, host, port)
        local c = Client:new(self._name .. index, host, port)
        c:connect()
        self._clients[index] = c

        local doProcessWrite = function()
            c:processWrite()
        end
        ngx.thread.spawn(doProcessWrite)
        c:processRead()
    end

    for i, host in ipairs(self._hosts) do
        ngx.timer.at(0, doConnect, i, host[1], host[2])
    end
end

function ReplsetClient.close(self)
    self._closed = true 
    for i, client in ipairs(self._clients) do
        client:close()
    end
end

function ReplsetClient.getClient(self)
    for i, client in ipairs(self._clients) do
        if client._sock ~= nil then
            return client
        end
    end
    return nil
end

function ReplsetClient.select(self, db_id)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:select(db_id)
end

function ReplsetClient.newLock(self, lock_key, timeout, expried)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:newLock(lock_key, timeout, expried)
end

function ReplsetClient.newDefaultSetEvent(self, event_key, timeout, expried)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:newDefaultSetEvent(event_key, timeout, expried)
end

function ReplsetClient.newDefaultClearEvent(self, event_key, timeout, expried)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:newDefaultClearEvent(event_key, timeout, expried)
end

function ReplsetClient.newMaxConcurrentFlow(self, flow_key, count, timeout, expried, require_aof)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:newMaxConcurrentFlow(flow_key, count, timeout, expried, require_aof)
end

function ReplsetClient.newTokenBucketFlow(self, flow_key, count, timeout, period, require_aof)
    local client = self:getClient()
    if client == nil then
        return nil, "all client closed"
    end
    return client:newTokenBucketFlow(flow_key, count, timeout, period, require_aof)
end

function _M.connect(self, name, host, port)
    if not _inited then
        self:init()
    end

    local doConnect = function()
        if _clients[name] ~= nil then
            return nil, "name used"
        end

        local c = Client:new(name, host, port)
        c:connect()
        _clients[name] = c

        local doProcessWrite = function()
            c:processWrite()
        end
        ngx.thread.spawn(doProcessWrite)
        c:processRead()
    end
    ngx.timer.at(0, doConnect)
end

function _M.connectReplset(self, name, hosts)
    if not _inited then
        self:init()
    end

    local doConnect = function()
        if _clients[name] ~= nil then
            return nil, "name used"
        end

        local c = ReplsetClient:new(name, hosts)
        c:connect()
        _clients[name] = c
    end
    ngx.timer.at(0, doConnect)
end

function _M.get(self, name)
    if _clients[name] == nil then
        return nil
    end
    return _clients[name]
end

function _M.init(self)
    local check_exiting = function()
        while true
        do
            local exiting = ngx.worker.exiting()
            if exiting then
                for name, client in pairs(_clients) do
                    client:close()
                end
                return
            end
            ngx.sleep(0.5)
        end
    end
    ngx.timer.at(0, check_exiting)
end

return _M