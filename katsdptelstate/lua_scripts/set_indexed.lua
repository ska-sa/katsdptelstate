if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
    redis.call('PUBLISH', 'update/' .. KEYS[1], '')
    return false
else
    return redis.call('HGET', KEYS[1], ARGV[1])
end
