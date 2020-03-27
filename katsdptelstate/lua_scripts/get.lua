local result = redis.pcall('GET', KEYS[1])
if type(result) == 'table' then
    local last = redis.call('ZREVRANGEBYLEX', KEYS[1], '+', '-', 'LIMIT', 0, 1)
    return {last[1], true}
else
    return {result, false}
end
