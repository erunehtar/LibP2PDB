-- MIT License
--
-- Copyright (c) 2026 Erunehtar
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.
--
--
-- Bucket Hash Set implementation for WoW Lua 5.1 environment.
-- Based on: "Partitioned Bucket Hashing for Efficient Anti‑Entropy"
--
-- Bucket hash set is used for divergence detection as an alternative to Merkle trees.
-- It is essentially a partitioned anti‑entropy structure. Each bucket holds a combined
-- hash of all inserted items, allowing for efficient divergence detection.

local MAJOR, MINOR = "LibBucketedHashSet", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibBucketedHashSet = LibStub:NewLibrary(MAJOR, MINOR)
if not LibBucketedHashSet then return end -- no upgrade needed

-- Local lua references
local tostring = tostring
local bxor = bit.bxor
local strbyte = string.byte
local select = select
local setmetatable = setmetatable

-- Constants
local UINT32_MODULO = 2 ^ 32
local UINT32_MAX = 0xFFFFFFFF

--- FNV-1a hash function (32-bit)
--- @param value string Input string to hash.
--- @param seed integer? Seed value.
--- @return integer hash 32-bit hash value.
local function FNV1a32(value, seed)
    local str = tostring(value)
    local len = #str
    local hash = 2166136261 + (seed or 0) * 13
    for i = 1, len do
        hash = bxor(hash, strbyte(str, i))
        hash = (hash * 16777619) % UINT32_MODULO
    end
    return hash
end

--- Computes the bucket index and combined hash for the given key and values.
--- @param self LibBucketedHashSet The bucket hash set instance.
--- @param key string The key to include in the hash.
--- @param ... any Additional values to include in the hash.
local function Hash(self, key, ...)
    local keyHash = FNV1a32(key, self.seed)
    local bucketIndex = (keyHash % self.numBuckets) + 1
    local hash = keyHash
    local n = select("#", ...)
    for i = 1, n do
        local v = select(i, ...)
        local salt = (i * 0x9E3779B1) % UINT32_MODULO
        hash = bxor(hash, FNV1a32(v, self.seed + salt))
    end
    return bucketIndex, hash % UINT32_MODULO
end

--- @class LibBucketedHashSet
--- @field New fun(numBuckets: integer, seed: integer?): LibBucketedHashSet
--- @field Insert fun(self: LibBucketedHashSet, key: any, ...: any): integer
--- @field Matches fun(self: LibBucketedHashSet, key: any, ...: any): boolean
--- @field Clear fun(self: LibBucketedHashSet)
--- @field Export fun(self: LibBucketedHashSet): LibBucketedHashSet.State
--- @field Import fun(state: LibBucketedHashSet.State): LibBucketedHashSet
--- @field seed integer
--- @field numBuckets integer
--- @field buckets integer[]

--- @class LibBucketedHashSet.State
--- @field numBuckets integer
--- @field buckets integer[]

LibBucketedHashSet.__index = LibBucketedHashSet

--- Creates a new bucket hash set instance.
--- @param numBuckets integer Number of buckets to use.
--- @param seed integer? Seed for the hashing function (default: 0).
--- @return LibBucketedHashSet instance The new bucket hash set instance.
function LibBucketedHashSet.New(numBuckets, seed)
    assert(numBuckets > 0, "numBuckets must be greater than 0")
    seed = seed or 0
    assert(type(seed) == "number", "seed must be a number")

    local buckets = {}
    for i = 1, numBuckets do
        buckets[i] = 0
    end
    return setmetatable({
        seed = seed or 0,
        numBuckets = numBuckets,
        buckets = buckets,
    }, LibBucketedHashSet)
end

--- Inserts a combined hash of the key and values into the bucket hash set.
--- The key is included in the combined hash, and is used to determine the bucket index.
--- @param key any The key to include in the hash.
--- @param ... any Additional values to include in the hash.
--- @return integer bucketIndex The index of the bucket where the hash was inserted.
function LibBucketedHashSet:Insert(key, ...)
    local bucketIndex, hash = Hash(self, key, ...)
    self.buckets[bucketIndex] = bxor(self.buckets[bucketIndex], hash) % UINT32_MODULO
    return bucketIndex
end

--- Determines whether the combined hash of the key and values is consistent with the bucket hash set.
--- The key is included in the combined hash, and is used to determine the bucket index.
--- @param key any The key to include in the hash.
--- @param ... any Additional values to include in the hash.
--- @return boolean result True if the combined hash differs from the bucket's stored hash, false otherwise.
function LibBucketedHashSet:Matches(key, ...)
    local bucketIndex, hash = Hash(self, key, ...)
    return self.buckets[bucketIndex] == hash
end

--- Clears all entries in the bucket hash set.
function LibBucketedHashSet:Clear()
    for i = 1, self.numBuckets do
        self.buckets[i] = 0
    end
end

--- Exports the current state of the bucket hash set.
--- @return LibBucketedHashSet.State state The exported state.
function LibBucketedHashSet:Export()
    return {
        self.seed,
        self.numBuckets,
        self.buckets,
    }
end

--- Imports a new bucket hash set from an exported state.
--- @param state LibBucketedHashSet.State The bucket hash set state to import.
--- @return LibBucketedHashSet instance The imported bucket hash set instance.
function LibBucketedHashSet.Import(state)
    assert(type(state) == "table", "state must be a table")
    assert(type(state[1]) == "number", "invalid seed in state")
    assert(state[2] > 0, "invalid numBuckets in state")
    assert(type(state[3]) == "table" and #state[3] == state[2], "invalid buckets in state")
    return setmetatable({
        seed = state[1],
        numBuckets = state[2],
        buckets = state[3],
    }, LibBucketedHashSet)
end

--[[ -- Uncomment to run tests when loading this file

local function RunLibBucketedHashSetTests()
    print("=== LibBucketedHashSet Tests ===")

    do
        -- Test 1: Identical sets remain identical
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        for i = 1, 4 do
            assert(setA.buckets[i] == setB.buckets[i], "Test 1 Failed: Sets should be identical after same insert")
        end
        print("Test 1 PASSED: Identical sets after same insert")
    end

    do
        -- Test 2: Divergence after different insert
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        setB:Insert("bar", 456)
        local diverged = false
        for i = 1, 4 do
            if setA.buckets[i] ~= setB.buckets[i] then
                diverged = true
                break
            end
        end
        assert(diverged, "Test 2 Failed: Sets should diverge after different insert")
        print("Test 2 PASSED: Sets diverge after different insert")
    end

    do
        -- Test 3: Convergence after same sequence
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        setB:Insert("bar", 456)
        setA:Insert("bar", 456)
        for i = 1, 4 do
            assert(setA.buckets[i] == setB.buckets[i], "Test 3 Failed: Sets should converge after same sequence")
        end
        print("Test 3 PASSED: Sets converge after same sequence")
    end

    do
        -- Test 4: Clear resets to identical
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        setA:Clear()
        setB:Clear()
        for i = 1, 4 do
            assert(setA.buckets[i] == 0 and setB.buckets[i] == 0, "Test 4 Failed: Buckets should be zero after clear")
        end
        print("Test 4 PASSED: Clear resets buckets")
    end

    do
        -- Test 5: Export and Import preserves state
        local setA = LibBucketedHashSet.New(4)
        setA:Insert("baz", 789)
        local state = setA:Export()
        local setC = LibBucketedHashSet.Import(state)
        for i = 1, 4 do
            assert(setA.buckets[i] == setC.buckets[i], "Test 5 Failed: Import should preserve bucket state")
        end
        print("Test 5 PASSED: Export and Import preserves state")
    end

    do
        -- Test 6: Matches returns true for matching input
        local set = LibBucketedHashSet.New(4)
        set:Insert("foo", 123)
        assert(set:Matches("foo", 123), "Test 6 Failed: Matches should return true for consistent input")
        print("Test 6 PASSED: Matches returns true for consistent input")
    end

    do
        -- Test 7: Matches returns false for divergent input
        local set = LibBucketedHashSet.New(4)
        set:Insert("foo", 123)
        assert(not set:Matches("foo", 999), "Test 7 Failed: Matches should return false for divergent input")
        print("Test 7 PASSED: Matches returns false for divergent input")
    end

    do
        -- Test 8: Matches detects missing entries
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)
        setA:Insert("foo", 123)
        -- B is missing foo
        assert(not setB:Matches("foo", 123), "Test 8 Failed: Matches should detect missing entry")
        print("Test 8 PASSED: Matches detects missing entry")
    end

    do
        -- Test 9: Matches detects extra entries
        local setA = LibBucketedHashSet.New(4)
        local setB = LibBucketedHashSet.New(4)

        setB:Insert("foo", 123)

        -- A is missing foo
        assert(not setA:Matches("foo", 123), "Test 9 Failed: Matches should detect extra entry")
        print("Test 9 PASSED: Matches detects extra entries")
    end

    do
        -- Test 10: Matches works with multiple values
        local set = LibBucketedHashSet.New(4)
        set:Insert("foo", 1, 2, 3)
        assert(set:Matches("foo", 1, 2, 3), "Test 10 Failed: Matches should match multi-value insert")
        assert(not set:Matches("foo", 1, 2, 4), "Test 10 Failed: Matches should detect multi-value divergence")
        print("Test 10 PASSED: Matches works with multiple values")
    end

    do
        -- Test 11: Keys hashing to same bucket behave correctly
        local set = LibBucketedHashSet.New(1) -- force all keys into same bucket
        set:Insert("foo", 123)
        set:Insert("bar", 456)

        assert(not set:Matches("foo", 123), "Test 11 Failed: Matches should detect missing foo")
        assert(not set:Matches("bar", 456), "Test 11 Failed: Matches should detect missing bar")
        print("Test 11 PASSED: Keys hashing to same bucket behave correctly")
    end

    do
        -- Test 12: Different seeds produce different hashes
        local setA = LibBucketedHashSet.New(4, 123)
        local setB = LibBucketedHashSet.New(4, 456)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        local diverged = false
        for i = 1, 4 do
            if setA.buckets[i] ~= setB.buckets[i] then
                diverged = true
                break
            end
        end
        assert(diverged, "Test 12 Failed: Different seeds should produce different bucket states")
        print("Test 12 PASSED: Different seeds produce different hashes")
    end

    do
        -- Test 13: Invalid state raises error on import
        ---@diagnostic disable-next-line: param-type-mismatch
        local status, err = pcall(function() LibBucketedHashSet.Import("invalid") end)
        assert(not status, "Test 13 Failed: Importing invalid state should raise error")
        print("Test 13 PASSED: Invalid state raises error on import")
    end

    do
        -- Test 14: Zero buckets raises error
        local status, err = pcall(function() LibBucketedHashSet.New(0) end)
        assert(not status, "Test 14 Failed: Creating set with zero buckets should raise error")
        print("Test 14 PASSED: Zero buckets raises error")
    end

    do
        -- Test 15: Non-number seed raises error
        ---@diagnostic disable-next-line: param-type-mismatch
        local status, err = pcall(function() LibBucketedHashSet.New(4, "notanumber") end)
        assert(not status, "Test 15 Failed: Non-number seed should raise error")
        print("Test 15 PASSED: Non-number seed raises error")
    end

    do
        -- Test 16: Exported state structure
        local set = LibBucketedHashSet.New(4, 789)
        set:Insert("foo", 123)
        local state = set:Export()
        assert(type(state) == "table", "Test 16 Failed: Exported state should be a table")
        assert(type(state[1]) == "number" and state[1] == 789, "Test 16 Failed: Exported state should contain correct seed")
        assert(type(state[2]) == "number" and state[2] == 4, "Test 16 Failed: Exported state should contain correct numBuckets")
        assert(type(state[3]) == "table" and #state[3] == 4, "Test 16 Failed: Exported state should contain correct buckets table")
        print("Test 16 PASSED: Exported state structure is correct")
    end

    do
        -- Test 17: Bucket hash values are within 32-bit range
        local set = LibBucketedHashSet.New(1000)
        for i = 1, 1000 do
            set:Insert("key" .. i * 13, i * 37)
        end
        for i = 1, 1000 do
            local bucketValue = set.buckets[i]
            assert(type(bucketValue) == "number" and bucketValue >= 0 and bucketValue <= UINT32_MAX,
                "Test 17 Failed: Bucket value should be a valid 32-bit unsigned integer")
        end
        print("Test 17 PASSED: Bucket hash values are within 32-bit range")
    end

    do
        -- Test 18: Equal key and value doesn't result in zero hash
        local set = LibBucketedHashSet.New(4)
        local bucketIndex = set:Insert("same", "same")
        assert(set.buckets[bucketIndex] ~= 0, "Test 18 Failed: Bucket hash should not be zero for equal key and value")
        print("Test 18 PASSED: Equal key and value doesn't result in zero hash")
    end

    print("=== All LibBucketedHashSet Tests PASSED ===\n")
end

RunLibBucketedHashSetTests()

]]--
