if redis.call('EXISTS', KEYS[1]) == 0 then
    return false
else
    local packed_st = ARGV[1]
    local packed_et = ARGV[2]
    local include_previous = ARGV[3]
    local previous = {}
    if include_previous == '1' and packed_st ~= '-' then
        previous = redis.call('ZREVRANGEBYLEX', KEYS[1], packed_st, '-', 'LIMIT', 0, 1)
    end
    local ans = redis.call('ZRANGEBYLEX', KEYS[1], packed_st, packed_et)
    for i = 1, #previous do
        table.insert(ans, 1, previous[i])
    end
    return ans
end
