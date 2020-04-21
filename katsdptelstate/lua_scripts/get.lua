local type_ = redis.call('TYPE', KEYS[1])['ok']
if type_ == 'none' then
    return {false, type_}
elseif type_ == 'string' then
    return {redis.call('GET', KEYS[1]), type_}
elseif type_ == 'zset' then
    local last = redis.call('ZREVRANGEBYLEX', KEYS[1], '+', '-', 'LIMIT', 0, 1)
    return {last[1], type_}
else
    return {redis.call('HGETALL', KEYS[1]), type_}
end
