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

local MAJOR, MINOR = "LibBucketHashSet", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibBucketHashSet = LibStub:NewLibrary(MAJOR, MINOR)
if not LibBucketHashSet then return end -- no upgrade needed

-- Local lua references
local tostring = tostring
local bxor = bit.bxor
local band = bit.band
local strbyte = string.byte
local select = select
local setmetatable = setmetatable

-- Constants
local UINT32_MODULO = 2 ^ 32
local UINT32_MAX = 0xFFFFFFFF

--- FNV-1a hash function (32-bit)
--- @param value any Input value to hash.
--- @return integer hash 32-bit hash value.
local function FNV1a32(value)
    local str = tostring(value)
    local len = #str
    local hash = 2166136261
    for i = 1, len do
        hash = bxor(hash, strbyte(str, i))
        hash = (hash * 16777619) % UINT32_MODULO
    end
    return hash
end

--- Computes the bucket index and combined hash for the given key and values.
--- @param self LibBucketHashSet The bucket hash set instance.
--- @param key string The key to include in the hash.
--- @param ... any Additional values to include in the hash.
local function Hash(self, key, ...)
    local keyHash = FNV1a32(key)
    local bucketIndex = (keyHash % self.numBuckets) + 1
    local hash = keyHash
    local n = select("#", ...)
    for i = 1, n do
        local v = select(i, ...)
        hash = bxor(hash, FNV1a32(v))
    end
    return bucketIndex, hash % UINT32_MODULO
end

--- @class LibBucketHashSet
--- @field New fun(numBuckets: integer):LibBucketHashSet
--- @field Insert fun(self: LibBucketHashSet, key: any, ...: any): integer
--- @field Matches fun(self: LibBucketHashSet, key: any, ...: any): boolean
--- @field Clear fun(self: LibBucketHashSet)
--- @field Export fun(self: LibBucketHashSet): LibBucketHashSet.State
--- @field Import fun(state: LibBucketHashSet.State): LibBucketHashSet
--- @field numBuckets integer
--- @field buckets integer[]

--- @class LibBucketHashSet.State
--- @field numBuckets integer
--- @field buckets integer[]

LibBucketHashSet.__index = LibBucketHashSet

--- Creates a new bucket hash set instance.
--- @param numBuckets integer Number of buckets to use.
--- @return LibBucketHashSet instance The new bucket hash set instance.
function LibBucketHashSet.New(numBuckets)
    local buckets = {}
    for i = 1, numBuckets do
        buckets[i] = 0
    end
    return setmetatable({
        numBuckets = numBuckets,
        buckets = buckets,
    }, LibBucketHashSet)
end

--- Inserts a combined hash of the key and values into the bucket hash set.
--- The key is included in the combined hash, and is used to determine the bucket index.
--- @param key any The key to include in the hash.
--- @param ... any Additional values to include in the hash.
--- @return integer bucketIndex The index of the bucket where the hash was inserted.
function LibBucketHashSet:Insert(key, ...)
    local bucketIndex, hash = Hash(self, key, ...)
    self.buckets[bucketIndex] = bxor(self.buckets[bucketIndex], hash) % UINT32_MODULO
    return bucketIndex
end

--- Determines whether the combined hash of the key and values is consistent with the bucket hash set.
--- The key is included in the combined hash, and is used to determine the bucket index.
--- @param key any The key to include in the hash.
--- @param ... any Additional values to include in the hash.
--- @return boolean result True if the combined hash differs from the bucket's stored hash, false otherwise.
function LibBucketHashSet:Matches(key, ...)
    local bucketIndex, hash = Hash(self, key, ...)
    return self.buckets[bucketIndex] == hash
end

--- Clears all entries in the bucket hash set.
function LibBucketHashSet:Clear()
    for i = 1, self.numBuckets do
        self.buckets[i] = 0
    end
end

--- Exports the current state of the bucket hash set.
--- @return LibBucketHashSet.State state The exported state.
function LibBucketHashSet:Export()
    return {
        self.numBuckets,
        self.buckets,
    }
end

--- Imports a new bucket hash set from an exported state.
--- @param state LibBucketHashSet.State The bucket hash set state to import.
--- @return LibBucketHashSet instance The imported bucket hash set instance.
function LibBucketHashSet.Import(state)
    assert(type(state) == "table" and #state == 2, "Invalid state format")
    assert(type(state[1]) == "number", "Invalid numBuckets in state")
    assert(type(state[2]) == "table", "Invalid buckets in state")
    local numBuckets = state[1]
    local buckets = state[2]
    return setmetatable({
        numBuckets = numBuckets,
        buckets = buckets,
    }, LibBucketHashSet)
end

--[[ -- Uncomment to run tests when loading this file

local function RunLibBucketHashSetTests()
    print("=== LibBucketHashSet Divergence Tests ===")

    do
        -- Test 1: Identical sets remain identical
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)
        setA:Insert("foo", 123)
        setB:Insert("foo", 123)
        for i = 1, 4 do
            assert(setA.buckets[i] == setB.buckets[i], "Test 1 Failed: Sets should be identical after same insert")
        end
        print("Test 1 PASSED: Identical sets after same insert")
    end

    do
        -- Test 2: Divergence after different insert
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)
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
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)
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
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)
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
        local setA = LibBucketHashSet.New(4)
        setA:Insert("baz", 789)
        local state = setA:Export()
        local setC = LibBucketHashSet.Import(state)
        for i = 1, 4 do
            assert(setA.buckets[i] == setC.buckets[i], "Test 5 Failed: Import should preserve bucket state")
        end
        print("Test 5 PASSED: Export and Import preserves state")
    end

    do
        print("---------------------------")
        -- Test 6: Matches returns true for matching input
        local set = LibBucketHashSet.New(4)
        set:Insert("foo", 123)
        assert(set:Matches("foo", 123), "Test 6 Failed: Matches should return true for consistent input")
        print("Test 6 PASSED: Matches returns true for consistent input")
    end

    do
        -- Test 7: Matches returns false for divergent input
        local set = LibBucketHashSet.New(4)
        set:Insert("foo", 123)
        assert(not set:Matches("foo", 999), "Test 7 Failed: Matches should return false for divergent input")
        print("Test 7 PASSED: Matches returns false for divergent input")
    end

    do
        -- Test 8: Matches detects missing entries
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)
        setA:Insert("foo", 123)
        -- B is missing foo
        assert(not setB:Matches("foo", 123), "Test 8 Failed: Matches should detect missing entry")
        print("Test 8 PASSED: Matches detects missing entry")
    end

    do
        -- Test 9: Matches detects extra entries
        local setA = LibBucketHashSet.New(4)
        local setB = LibBucketHashSet.New(4)

        setB:Insert("foo", 123)

        -- A is missing foo
        assert(not setA:Matches("foo", 123), "Test 9 Failed: Matches should detect extra entry")
        print("Test 9 PASSED: Matches detects extra entries")
    end

    do
        -- Test 10: Matches works with multiple values
        local set = LibBucketHashSet.New(4)
        set:Insert("foo", 1, 2, 3)
        assert(set:Matches("foo", 1, 2, 3), "Test 10 Failed: Matches should match multi-value insert")
        assert(not set:Matches("foo", 1, 2, 4), "Test 10 Failed: Matches should detect multi-value divergence")
        print("Test 10 PASSED: Matches works with multiple values")
    end

    do
        -- Test 11: Keys hashing to same bucket behave correctly
        local set = LibBucketHashSet.New(1) -- force all keys into same bucket
        set:Insert("foo", 123)
        set:Insert("bar", 456)

        assert(not set:Matches("foo", 123), "Test 11 Failed: Matches should detect missing foo")
        assert(not set:Matches("bar", 456), "Test 11 Failed: Matches should detect missing bar")
        print("Test 11 PASSED: Keys hashing to same bucket behave correctly")
    end

    print("=== All LibBucketHashSet Divergence Tests PASSED ===\n")
end

RunLibBucketHashSetTests()

]]--