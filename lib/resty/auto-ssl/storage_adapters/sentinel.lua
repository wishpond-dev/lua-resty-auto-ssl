-- Based on the redis store but modified to support sentinel

local _M = {}

-- lua doesn't have a built-in string split function so all this is to
-- support the sentinels configuration.
local function strsplit(delimiter, text)
   local list = {}
   local pos = 1
   if string.find("", delimiter, 1) then -- this would result in endless loops
      error("delimiter matches empty string!")
   end
   while 1 do
      local first, last = string.find(text, delimiter, pos)
      if first then -- found?
         table.insert(list, string.sub(text, pos, first-1))
         pos = last+1
      else
         table.insert(list, string.sub(text, pos))
         break
      end
   end
   return list
end

local function prefixed_key(self, key)
  if self.options["prefix"] then
    return self.options["prefix"] .. ":" .. key
  else
    return key
  end
end

function _M.new(auto_ssl_instance)
  local options = auto_ssl_instance:get("redis") or {}

  if not options["sentinels"] and options["sentinel_hosts"] then
    local sentinels = {}
    local hosts = strsplit(",", options["sentinel_hosts"])
    for _, host in ipairs(hosts) do
      table.insert(sentinels, { host = host, port = "26379" })
    end
    options["sentinels"] = sentinels
  end

  return setmetatable({ options = options }, { __index = _M })
end

function _M.get_connection(self)
  local connection = ngx.ctx.auto_ssl_redis_connection
  if connection then
    return connection
  end

  local options = {
    connect_timeout   = 50,
    read_timeout      = 1000,
    keepalive_timeout = 30000
  }

  local host = {
    sentinels   = self.options["sentinels"],
    master_name = self.options["master_name"],
    db          = self.options["db"]
  }

  local rc = require("resty.redis.connector").new(options)

  local err
  connection, err = rc:connect(host)

  if not connection then
    return false, err
  end

  ngx.ctx.auto_ssl_redis_connection = connection
  return connection
end

function _M.setup()
end

function _M.get(self, key)
  local connection, connection_err = self:get_connection()
  if connection_err then
    return nil, connection_err
  end

  local res, err = connection:get(prefixed_key(self, key))
  if res == ngx.null then
    res = nil
  end

  return res, err
end

function _M.set(self, key, value, options)
  local connection, connection_err = self:get_connection()
  if connection_err then
    return false, connection_err
  end

  key = prefixed_key(self, key)
  local ok, err = connection:set(key, value)
  if ok then
    if options and options["exptime"] then
      local _, expire_err = connection:expire(key, options["exptime"])
      if expire_err then
        ngx.log(ngx.ERR, "auto-ssl: failed to set expire: ", expire_err)
      end
    end
  end

  return ok, err
end

function _M.delete(self, key)
  local connection, connection_err = self:get_connection()
  if connection_err then
    return false, connection_err
  end

  return connection:del(prefixed_key(self, key))
end

function _M.keys_with_suffix(self, suffix)
  local connection, connection_err = self:get_connection()
  if connection_err then
    return false, connection_err
  end

  -- Use scan for non-blocking cursor-based key scanning
  local keys = {}
  local cursor = "0"
  local err
  local res
  repeat
    res, err = connection:scan(cursor, "match", "*" .. suffix)
    if not res then
      ngx.say(err)
      break
    end

    local data
    cursor, data = unpack(res)
    for _, key in ipairs(data) do
      table.insert(keys, key)
    end
  until cursor == "0"

  if keys and self.options["prefix"] then
    local unprefixed_keys = {}
    -- First character past the prefix and a colon
    local offset = string.len(self.options["prefix"]) + 2

    for _, key in ipairs(keys) do
      local unprefixed = string.sub(key, offset)
      table.insert(unprefixed_keys, unprefixed)
    end

    keys = unprefixed_keys
  end

  return keys, err
end

return _M
