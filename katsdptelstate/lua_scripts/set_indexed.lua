if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
    -- Bundle subkey and value into a single message in a way that allows unpacking
    local message = "\x03" .. string.len(ARGV[1]) .. "\n" .. ARGV[1] .. ARGV[2]
    redis.call('PUBLISH', 'update/' .. KEYS[1], message)
    return false
else
    return redis.call('HGET', KEYS[1], ARGV[1])
end
