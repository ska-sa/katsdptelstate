if redis.call('ZADD', KEYS[1], 0, ARGV[1]) == 1 then
    redis.call('PUBLISH', 'update/' .. KEYS[1], ARGV[1])
end
