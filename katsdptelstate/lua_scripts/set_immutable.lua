if redis.call('SETNX', KEYS[1], ARGV[1]) == 1 then
    redis.call('PUBLISH', 'update/' .. KEYS[1], ARGV[1])
    return false
else
    return redis.call('GET', KEYS[1])
end
