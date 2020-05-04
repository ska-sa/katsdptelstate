local value = redis.call('HGET', KEYS[1], ARGV[1])
if value ~= false then
    return value
else
    return redis.call('EXISTS', KEYS[1]) == 0
end
