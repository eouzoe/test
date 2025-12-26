-- Atomic Redis token-bucket script
-- ARGV: capacity, refill_per_sec, now_millis, tokens_needed
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local need = tonumber(ARGV[4])

local function min(a,b) if a<b then return a else return b end end
local function max(a,b) if a>b then return a else return b end end

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then tokens = capacity end
if ts == nil then ts = now end

local elapsed_ms = now - ts
if elapsed_ms < 0 then elapsed_ms = 0 end
local added = math.floor((elapsed_ms / 1000) * refill_per_sec)
tokens = min(capacity, tokens + added)
ts = now

local allowed = 0
if tokens >= need then
  tokens = tokens - need
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
local ex = math.ceil(capacity / max(refill_per_sec,1)) * 2
redis.call('EXPIRE', key, ex)
return allowed
