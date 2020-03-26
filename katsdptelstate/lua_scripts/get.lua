local result = redis.pcall('GET', KEYS[1])
if type(result) == 'table' then
    local last = redis.call('ZREVRANGEBYLEX', KEYS[1], '+', '-', 'LIMIT', 0, 1)
    result = string.sub(last[1], 9)   -- strip off timestamp
end
return result
