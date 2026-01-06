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
-- Patterned Bloom Filter implementation for WoW Lua 5.1 environment.
-- Based on: https://save-buffer.github.io/bloom_filter.html Section 1.4
--
-- Credits:
--   This variant of the standard Bloom filter, proposed by Sasha Krassovsky, uses a
--   single hash function with a predetermined pattern of bits, improving efficiency
--   while maintaining similar false positive rates.
--
-- Optimized for 32-bit Lua environment with single hash and pattern lookup.
-- Uses FNV-1a hash function.
-- Compact bit array representation using 31-bit integers to avoid sign bit issues.
-- Supports insertion, membership testing, export/import, and false positive rate estimation.

local MAJOR, MINOR = "PatternedBloomFilter", 1
assert(LibStub, MAJOR .. " requires LibStub")

local PatternedBloomFilter = LibStub:NewLibrary(MAJOR, MINOR)
if not PatternedBloomFilter then return end -- no upgrade needed

-- Local lua references
local assert, type, setmetatable, pairs, ipairs = assert, type, setmetatable, pairs, ipairs
local band, bor, bxor, lshift, rshift = bit.band, bit.bor, bit.bxor, bit.lshift, bit.rshift
local floor, ceil, log, exp, random = math.floor, math.ceil, math.log, math.exp, fastrandom
local tostring, tonumber, strbyte = tostring, tonumber, strbyte
local tinsert = table.insert

-- Constants
local LOG2 = log(2)

--- Deterministic PRNG using Linear Congruential Generator.
--- This ensures all clients generate identical patterns for same parameters.
--- @param seed number Initial seed value.
--- @return function PRNG function returning pseudo-random numbers.
local function CreateLCG(seed)
    local state = seed
    return function(min, max)
        -- LCG parameters (same as glibc)
        state = band((state * 1103515245 + 12345), 0x7FFFFFFF)
        if max then
            return min + (state % (max - min + 1))
        end
        return state
    end
end

--- Deterministic pattern generation: Create bit patterns with k bits set.
--- Uses deterministic PRNG seeded from parameters to ensure all clients
--- with same parameters generate identical patterns.
--- @param numPatterns number Number of unique patterns to generate.
--- @param patternBits number Number of bits in each pattern.
--- @param bitsPerPattern number Number of bits set in each pattern.
local function GeneratePatterns(numPatterns, patternBits, bitsPerPattern)
    -- Create deterministic seed from parameters
    local seed = numPatterns * 31 + bitsPerPattern * 7 + patternBits * 13
    local prng = CreateLCG(seed)

    -- Generate unique patterns
    local patterns = {}
    local seen = {}
    for i = 1, numPatterns do
        local pattern
        repeat
            pattern = 0
            local positions = {}

            -- Set bitsPerPattern bits deterministically
            for j = 1, bitsPerPattern do
                local pos
                repeat
                    pos = prng(0, patternBits - 1)
                until not positions[pos]
                positions[pos] = true
                pattern = bor(pattern, lshift(1, pos))
            end
        until not seen[pattern]

        seen[pattern] = true
        patterns[i] = pattern
    end
    return patterns
end

--- Rotate left for 32-bit integers.
--- @param value number Input value.
--- @param shift number Number of bits to rotate.
--- @return number result Rotated value.
local function RotateLeft32(value, shift)
    shift = shift % 32 -- Ensure shift is 0-31
    if shift == 0 then return value end
    local left = (value * (2^shift)) % 4294967296
    local right = floor(value / (2^(32 - shift)))
    return left + right
end

--- FNV-1a hash function (32-bit)
--- @param value string Input string to hash.
--- @return number hash 32-bit hash value.
local function FNV1a32(value)
    local str = tostring(value)
    local len = #str
    local hash = 2166136261
    for i = 1, len do
        hash = bxor(hash, strbyte(str, i))
        hash = (hash * 16777619) % 4294967296
    end
    return hash
end

-- Construct mask from hash
local function ConstructMask(self, hash)
    -- Extract pattern index from low bits
    local patternIdx = band(hash, self.patternIndexMask) + 1

    -- Extract rotation amount from next bits
    local rotation = band(rshift(hash, self.patternIndexBits), self.rotateMask)

    -- Load pattern and rotate
    local pattern = self.patterns[patternIdx]
    return RotateLeft32(pattern, rotation)
end

--- @class PatternedBloomFilter Patterned Bloom Filter data structure.
--- @field New fun(numItems: number, falsePositiveRate: number?, numPatterns?: number, bitsPerPattern?: number): PatternedBloomFilter
--- @field Insert fun(self: PatternedBloomFilter, value: any)
--- @field Contains fun(self: PatternedBloomFilter, value: any): boolean
--- @field Export fun(self: PatternedBloomFilter): PatternedBloomFilterState
--- @field Import fun(data: PatternedBloomFilterState): PatternedBloomFilter
--- @field Clear fun(self: PatternedBloomFilter)
--- @field GetFalsePositiveRate fun(self: PatternedBloomFilter): number
--- @field numBits number Total number of bits in the filter.
--- @field numIntegers number Number of 31-bit integers in the bit array.
--- @field falsePositiveRate number Desired false positive rate.
--- @field numItems number Expected number of items to insert.
--- @field patterns number[] Array of bit patterns.
--- @field numPatterns number Number of unique bit patterns.
--- @field patternBits number Number of bits in each pattern.
--- @field bitsPerPattern number Number of bits set per pattern.
--- @field patternIndexBits number Number of bits used for pattern index.
--- @field patternIndexMask number Bitmask for pattern index extraction.
--- @field rotateBits number Number of bits used for rotation amount.
--- @field rotateMask number Bitmask for rotation amount extraction.
--- @field itemCount number Number of items inserted.
--- @field bits number[] Bit array represented as array of 31-bit integers.

--- @class PatternedBloomFilterState Compact representation of Patterned Bloom Filter.
--- @field [1] number numBits Total number of bits in the filter.
--- @field [2] number numIntegers Number of 31-bit integers in the bit array.
--- @field [3] number numPatterns Number of unique bit patterns.
--- @field [4] number patternBits Number of bits in each pattern.
--- @field [5] number bitsPerPattern Number of bits set per pattern.
--- @field [6] number itemCount Number of items inserted.
--- @field [7] number[] Bit array represented as array of 31-bit integers.

PatternedBloomFilter.__index = PatternedBloomFilter

--- Create a new Patterned Bloom Filter instance.
--- @param numItems number Expected number of items to insert.
--- @param falsePositiveRate number? Desired false positive rate (default: 0.01 which means 1%).
--- @param numPatterns number? Number of unique bit patterns to generate (default: 256).
--- @param bitsPerPattern number? Number of bits set per pattern (default: 4).
--- @return PatternedBloomFilter instance The new Patterned Bloom Filter instance.
function PatternedBloomFilter.New(numItems, falsePositiveRate, numPatterns, bitsPerPattern)
    assert(numItems and numItems > 0, "numItems must be a positive number")
    falsePositiveRate = falsePositiveRate or 0.01 -- Default: 1% FPR
    numPatterns = numPatterns or 256              -- Default: 256 patterns (8 bits for index)
    bitsPerPattern = bitsPerPattern or 4          -- Default: 4 bits set per pattern
    assert(falsePositiveRate > 0 and falsePositiveRate < 1, "falsePositiveRate must be between 0 and 1")
    assert(numPatterns > 0 and band(numPatterns, numPatterns - 1) == 0, "numPatterns must be a power of two")
    assert(bitsPerPattern > 0 and bitsPerPattern < 31, "bitsPerPattern must be between 1 and 30")

    -- Calculate optimal number of bits (using standard Bloom formula)
    -- m = -n * ln(p) / (ln(2)^2)
    local numBits = ceil(-numItems * log(falsePositiveRate) / (LOG2 * LOG2))

    -- Round up to multiple of 32 for efficient storage
    numBits = ceil(numBits / 32) * 32

    -- We use 31 bits per integer (avoid sign bit issues)
    local patternBits = 31
    local numIntegers = ceil(numBits / 31)

    -- Calculate derived fields
    local patternIndexBits = floor(log(numPatterns) / LOG2)
    local patternIndexMask = numPatterns - 1
    local rotateBits = 5
    local rotateMask = lshift(1, rotateBits) - 1

    -- Generate random bit patterns
    local patterns = GeneratePatterns(numPatterns, patternBits, bitsPerPattern)

    -- Initialize bit array
    local bits = {}
    for i = 1, numIntegers do
        bits[i] = 0
    end

    return setmetatable({
        numBits = numBits,
        numIntegers = numIntegers,
        falsePositiveRate = falsePositiveRate,
        numItems = numItems,
        patterns = patterns,
        numPatterns = numPatterns,
        patternBits = patternBits,
        bitsPerPattern = bitsPerPattern,
        patternIndexBits = patternIndexBits,
        patternIndexMask = patternIndexMask,
        rotateBits = rotateBits,
        rotateMask = rotateMask,
        itemCount = 0,
        bits = bits,
    }, PatternedBloomFilter)
end

--- Insert a value into the patterned bloom filter.
--- @param value any Value to insert.
function PatternedBloomFilter:Insert(value)
    assert(value ~= nil, "value cannot be nil")
    local hash = FNV1a32(value)
    local mask = ConstructMask(self, hash)

    -- Use high bits of hash to determine which integer to update
    local offset = rshift(hash, self.patternIndexBits + self.rotateBits)
    local idx = (offset % self.numIntegers) + 1

    -- Apply pattern to single integer
    self.bits[idx] = bor(self.bits[idx], mask)
    self.itemCount = self.itemCount + 1
end

--- Determine if a value is possibly in the patterned bloom filter.
--- @param value any Value to check.
--- @return boolean result True if value might be in the set, false if definitely not.
function PatternedBloomFilter:Contains(value)
    assert(value ~= nil, "value cannot be nil")
    local hash = FNV1a32(value)
    local mask = ConstructMask(self, hash)

    -- Use same offset calculation as Insert
    local offset = rshift(hash, self.patternIndexBits + self.rotateBits)
    local idx = (offset % self.numIntegers) + 1

    -- Check if all bits in pattern are set at this location
    return band(self.bits[idx], mask) == mask
end

--- Export the patterned bloom filter state to a compact representation.
--- @return PatternedBloomFilterState state Compact representation of the patterned bloom filter.
function PatternedBloomFilter:Export()
    return {
        self.numBits,
        self.numIntegers,
        self.numPatterns,
        self.patternBits,
        self.bitsPerPattern,
        self.itemCount,
        self.bits,
    }
end

--- Import a new patterned bloom filter from a compact representation.
--- @param state PatternedBloomFilterState Compact representation of the patterned bloom filter.
--- @return PatternedBloomFilter instance The reconstructed Patterned Bloom Filter instance.
function PatternedBloomFilter.Import(state)
    assert(state and type(state) == "table", "state must be a table")
    assert(state[1] and state[1] > 0, "invalid numBits in state")
    assert(state[2] and state[2] > 0, "invalid numIntegers in state")
    assert(state[3] and state[3] > 0, "invalid numPatterns in state")
    assert(state[4] and state[4] > 0, "invalid patternBits in state")
    assert(state[5] and state[5] > 0, "invalid bitsPerPattern in state")
    assert(state[6] and state[6] >= 0, "invalid itemCount in state")
    assert(state[7] and type(state[7]) == "table", "invalid bits array in state")
    local numBits = state[1]
    local numIntegers = state[2]
    local numPatterns = state[3]
    local patternBits = state[4]
    local bitsPerPattern = state[5]
    local itemCount = state[6]
    local bits = state[7]

    -- Calculate derived fields
    local patternIndexBits = floor(log(numPatterns) / LOG2)
    local patternIndexMask = numPatterns - 1
    local rotateBits = 5
    local rotateMask = lshift(1, rotateBits) - 1

    -- Regenerate patterns deterministically (same as on creation)
    local patterns = GeneratePatterns(numPatterns, patternBits, bitsPerPattern)

    return setmetatable({
        numBits = numBits,
        numIntegers = numIntegers,
        numPatterns = numPatterns,
        patternBits = patternBits,
        bitsPerPattern = bitsPerPattern,
        itemCount = itemCount,
        patternIndexBits = patternIndexBits,
        patternIndexMask = patternIndexMask,
        rotateBits = rotateBits,
        rotateMask = rotateMask,
        patterns = patterns,
        bits = bits,
    }, PatternedBloomFilter)
end

--- Clear all values from the patterned bloom filter.
function PatternedBloomFilter:Clear()
    for i = 1, self.numIntegers do
        self.bits[i] = 0
    end
    self.itemCount = 0
end

--- Estimate the current false positive rate of the patterned bloom filter.
--- @return number fpr Estimated false positive rate.
function PatternedBloomFilter:GetFalsePositiveRate()
    -- Count set bits
    local bitsSet = 0
    for i = 1, self.numIntegers do
        local val = self.bits[i]
        -- Count bits in val
        while val > 0 do
            bitsSet = bitsSet + 1
            val = band(val, val - 1) -- Clear lowest set bit
        end
    end

    -- FPR ≈ (bitsSet / numBits)^k
    -- For patterned bloom, k ≈ bitsPerPattern
    local fillRatio = bitsSet / self.numBits
    return fillRatio ^ self.bitsPerPattern
end

-------------------------------------------------------------------------------
-- TESTS: Verify Patterned Bloom Filter correctness
-------------------------------------------------------------------------------

--[[ -- Uncomment to run tests when loading this file

local function RunPatternedBloomFilterTests()
    print("=== PatternedBloomFilter Tests ===")

    -- Test 1: Basic insertion and membership
    local pbf = PatternedBloomFilter.New(100, 0.01)
    assert(not pbf:Contains("item1"), "Test 1 Failed: Empty filter should not contain items")

    pbf:Insert("item1")
    pbf:Insert("item2")
    pbf:Insert("item3")
    assert(pbf:Contains("item1"), "Test 1 Failed: Should contain inserted item1")
    assert(pbf:Contains("item2"), "Test 1 Failed: Should contain inserted item2")
    assert(pbf:Contains("item3"), "Test 1 Failed: Should contain inserted item3")
    print("Test 1 PASSED: Basic insertion and membership")

    -- Test 2: False positives vs true negatives
    local testPbf = PatternedBloomFilter.New(100000, 0.01)
    for i = 1, 50000 do
        local item = "test_" .. i
        testPbf:Insert(item)
    end

    local falsePositives = 0
    local testCount = 100000
    for i = 50001, 50000 + testCount do
        local item = "test_" .. i
        if testPbf:Contains(item) then
            falsePositives = falsePositives + 1
        end
    end

    local actualFPR = falsePositives / testCount
    local estimatedFPR = testPbf:GetFalsePositiveRate()
    print(string.format("Test 2 PASSED: FP Rate - Actual: %.4f, Estimated: %.4f", actualFPR, estimatedFPR))
    assert(actualFPR < 0.1, "Test 2 Failed: False positive rate too high")

    -- Test 3: Export and Import
    local pbf3 = PatternedBloomFilter.New(100, 0.01)
    for i = 1, 100 do
        pbf3:Insert("export_" .. i)
    end

    local exported = pbf3:Export()
    local imported = PatternedBloomFilter.Import(exported)

    for i = 1, 100 do
        assert(imported:Contains("export_" .. i), "Test 3 Failed: Imported filter should contain export_" .. i)
    end
    print("Test 3 PASSED: Export and Import")

    -- Test 4: Clear functionality
    local pbf4 = PatternedBloomFilter.New(100, 0.01)
    pbf4:Insert("clear1")
    pbf4:Insert("clear2")
    assert(pbf4:Contains("clear1"), "Test 4 Failed: Should contain clear1 before clear")

    pbf4:Clear()
    assert(not pbf4:Contains("clear1"), "Test 4 Failed: Should not contain clear1 after clear")
    assert(not pbf4:Contains("clear2"), "Test 4 Failed: Should not contain clear2 after clear")
    print("Test 4 PASSED: Clear functionality")

    -- Test 5: No false negatives (critical property)
    local pbf5 = PatternedBloomFilter.New(100000, 0.01)
    local items = {}
    for i = 1, 100000 do
        items[i] = "item_" .. i
        pbf5:Insert(items[i])
    end

    for i = 1, 100000 do
        assert(pbf5:Contains(items[i]), "Test 5 Failed: False negative detected for " .. items[i])
    end
    print("Test 5 PASSED: No false negatives")

    -- Test 6: Deterministic pattern generation
    local pbf6a = PatternedBloomFilter.New(100, 0.01, 256, 4)
    local pbf6b = PatternedBloomFilter.New(100, 0.01, 256, 4)

    pbf6a:Insert("pattern_test")
    pbf6b:Insert("pattern_test")

    -- Both should have identical bit patterns
    local export6a = pbf6a:Export()
    local export6b = pbf6b:Export()

    assert(#export6a == #export6b, "Test 6 Failed: Export sizes should match")
    local export6aBits = export6a[7]
    local export6bBits = export6b[7]
    assert(#export6aBits == #export6bBits, "Test 6 Failed: Bit array sizes should match")
    for i = 1, #export6aBits do -- Compare bits
        assert(export6aBits[i] == export6bBits[i], "Test 6 Failed: Bit arrays should be identical")
    end
    print("Test 6 PASSED: Deterministic pattern generation")

    print("=== All PatternedBloomFilter Tests PASSED ===\n")
end

RunPatternedBloomFilterTests()

]] --
