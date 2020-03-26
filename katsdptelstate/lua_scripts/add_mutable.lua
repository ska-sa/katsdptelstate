local result = redis.pcall('ZADD', KEYS[1], 0, ARGV[1])
if result == 1 then
    redis.call('PUBLISH', 'update/' .. KEYS[1], ARGV[1])
end
return result
