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

_M._VERSION = '0.0.1'

local MAGIC = 0x56
local VERSION = 0x01

local COMMAND_TYPE_INIT = 0
local COMMAND_TYPE_LOCK = 1
local COMMAND_TYPE_UNLOCK = 2

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
        local ub = math.modf(b / 16)
        es = es .. ENCODE_HEXS[ub + 1] .. ENCODE_HEXS[math.fmod(b, 16) + 1]
    end
    return es
end

function hex_decode(es)
    if math.fmod(#es, 2) == 1 then
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
    if i >= 0xffff or i < 0 then
        return nil, 'out max size'
    end

    local ui = math.modf(i / 256)
    return string.char(math.fmod(i, 256)) .. string.char(ui)
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

    local lbit, msg = uint16_to_bin(math.fmod(i, 65536))
    if lbit == nil then
        return nil, msg
    end

    local ui = math.modf(i / 65536)
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

local Lock = new_tab(0, 55)
local _MetaLock = { __index = Lock }

function Lock.new(self, db, lock_key, timeout, expried, lock_id, max_count, reentrant_count) 
    if #lock_key < 16 then
        local padding = ''
        for i = 1, 16 - #lock_key do
            padding = padding .. string.char(0)
        end
        lock_key = padding .. lock_key
    else
        if #lock_key == 32 then
            local d, err = hex_decode(lock_key)
            if d ~= nil then
                lock_key = d
            end
        end
    end

    if lock_id == nil or #lock_id == 0 then
        lock_id = gen_lock_id()
    else
        if #lock_id < 16 then
            local padding = ''
            for i = 1, 16 - #lock_id do
                padding = padding .. string.char(0)
            end
            lock_id = padding .. lock_id
        else 
            if #lock_id == 32 then
                local d, err = hex_decode(lock_id)
                if d ~= nil then
                    lock_id = d
                end
            end
        end
    end

    return setmetatable({
        _db = db,
        _lock_key = lock_key,
        _timeout = timeout or 0,
        _expried = expried or 0,
        _lock_id = lock_id or gen_lock_id(),
        _max_count = max_count or 0,
        _reentrant_count or 0
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
        count = self._count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return nil, err
    end
    return result
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
        count = self._count,
        rcount = self._reentrant_count,
    })
    if result == nil then
        return nil, err
    end
    return result
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
    sock:settimeouts(5000, 60000, 15000)

    local ok, err = sock:connect(self._host, self._port)
    if not ok then
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
    _clients[self._name] = nil
    self._closed = true
end

function Client.init(self) 
    data = string.char(MAGIC) .. string.char(VERSION) .. string.char(COMMAND_TYPE_INIT) .. gen_request_id() .. self._client_id
    for i=0,28,1 do
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

function Client.readCommand(self)
    if self._sock == nil then
        return nil, 'sock unconnected'
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

function Client.writeCommand(self, command)
    if self._sock == nil then
        return 0, 'sock unconnected'
    end

    data = string.char(MAGIC) .. string.char(VERSION) .. string.char(command.command) .. command.request_id .. string.char(command.flag or 0)
    data = data .. string.char(command.db_id) .. command.lock_id .. command.lock_key
    data = data .. uint32_to_bin(command.timeout or 0) .. uint32_to_bin(command.expried or 0)
    data = data .. uint16_to_bin(command.count or 0) .. string.char(command.rcount or 0)

    local n, err = self._sock:send(data)
    if n == nil then
        return 0, err
    end
    return n
end

function Client.command(self, command)
    if self._sock == nil then
        return nil, 'sock unconnected'
    end

    command.request_id = gen_request_id()
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
    local result = nil
    local ok, err = waiter:wait(command.timeout + 1)
    if ok then
        result = self._results[command.request_id]
        if result ~= nil then
            self._results[command.request_id] = nil
        end
    else
        command._closed = true
    end
    if self._results_waiter[command.request_id] ~= nil then
        self._results_waiter[command.request_id] = waiter
    end
    return result
end

function Client.processRead(self)
    while not self._closed 
    do
        local result, err = self:readCommand()
        if result == nil then
            if err ~= "timeout" then
                ngx.log(ngx.ERR, "slock read command error: " .. err)
            end

            if err == "closed" then
                if self._closed then
                    self._commands_waiter:post(1)
                    return
                end
                self:reconnect()
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
    end
    self._commands_waiter:post(1)
end

function Client.processWrite(self)
    while not self._closed
    do
        local ok, err =  self._commands_waiter:wait(60)
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
                        ngx.log(ngx.ERR, "slock write command error: " .. err)
                    end
                end
            end
        end
    end
end

function Client.processExiting(self)
    while not self._closed
    do
        local exiting = ngx.worker.exiting()
        if exiting then
            self:close()
            return
        end
        ngx.sleep(5)
    end
end

function _M.connect(self, name, host, port)
    local doConnect = function()
        if _clients[name] ~= nil then
            return nil, "name used"
        end

        local c = Client:new(name, host, port)
        local ok, err = c:connect()
        if not ok then
            ngx.log(ngx.ERR, "connect error:" .. err)
            return nil, err
        end
        _clients[name] = c

        local doProcessWrite = function()
            c:processWrite()
        end
        local doProcessExiting = function()
            c:processExiting()
        end
        ngx.thread.spawn(doProcessWrite)
        ngx.thread.spawn(doProcessExiting)
        c:processRead()
    end
    ngx.timer.at(0, doConnect)
end

function _M.get(self, name)
    if _clients[name] == nil then
        return nil
    end
    return _clients[name]
end

return _M