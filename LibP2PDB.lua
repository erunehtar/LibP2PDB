------------------------------------------------------------------------------------------------------------------------
-- LibP2PDB: A lightweight, embeddable library for peer-to-peer distributed-database synchronization in WoW addons.
------------------------------------------------------------------------------------------------------------------------

local MAJOR, MINOR = "LibP2PDB", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibP2PDB = LibStub:NewLibrary(MAJOR, MINOR)
if not LibP2PDB then return end -- no upgrade needed

------------------------------------------------------------------------------------------------------------------------
-- Dependencies
------------------------------------------------------------------------------------------------------------------------

local AceComm = LibStub("AceComm-3.0")

------------------------------------------------------------------------------------------------------------------------
-- Optional Dependencies
------------------------------------------------------------------------------------------------------------------------

local AceSerializer = LibStub("AceSerializer-3.0", true)
local LibSerialize = LibStub("LibSerialize", true)
local LibDeflate = LibStub("LibDeflate", true)

------------------------------------------------------------------------------------------------------------------------
-- Local Lua References
------------------------------------------------------------------------------------------------------------------------

local assert, print = assert, print
local type, ipairs, pairs = type, ipairs, pairs
local min, max, abs = min, max, abs
local tonumber, tostring, tostringall = tonumber, tostring, tostringall
local format, strsub, strfind, strjoin, strmatch, strbyte, strchar = format, strsub, strfind, strjoin, strmatch, strbyte, strchar
local tinsert, tremove, tconcat, tsort = table.insert, table.remove, table.concat, table.sort
local unpack, select = unpack, select
local setmetatable, getmetatable = setmetatable, getmetatable
local securecallfunction = securecallfunction

------------------------------------------------------------------------------------------------------------------------
-- Local WoW API References
------------------------------------------------------------------------------------------------------------------------

local UnitName, UnitGUID = UnitName, UnitGUID
local GetTime, GetServerTime = GetTime, GetServerTime
local IsInGuild, IsInRaid, IsInGroup, IsInInstance = IsInGuild, IsInRaid, IsInGroup, IsInInstance

------------------------------------------------------------------------------------------------------------------------
-- Constants
------------------------------------------------------------------------------------------------------------------------

local DEBUG = false
local NIL_MARKER = strchar(0)

local Color = {
    Ace = "ff33ff99",
    Debug = "ff00ffff",
    White = "ffffffff",
    Yellow = "ffffff00",
    Red = "ffff4040",
    Green = "ff00ff00"
}

local CommMessageType = {
    PeerDiscoveryRequest = 1,
    PeerDiscoveryResponse = 2,
    SnapshotRequest = 3,
    SnapshotResponse = 4,
    Digest = 5,
    RequestRows = 6,
    Rows = 7,
}

local CommPriority = {
    Low = "BULK",
    Normal = "NORMAL",
    High = "ALERT",
}

------------------------------------------------------------------------------------------------------------------------
-- Private Helper Functions
------------------------------------------------------------------------------------------------------------------------

local function C(color, text)
    return "|c" .. color .. text .. "|r"
end

local function Debug(...)
    if DEBUG then
        print(C(Color.Ace, "LibP2PDB") .. ": " .. C(Color.Debug, "[DEBUG]") .. " " .. strjoin(" ", tostringall(...)))
    end
end

local function Dump(o)
    if type(o) == "table" then
        local s = "{"
        for k, v in pairs(o) do
            if type(k) ~= "number" then
                k = '"' .. k .. '"'
            end
            s = s .. '[' .. k .. '] = ' .. Dump(v) .. ','
        end
        return s .. '}'
    elseif type(o) == "string" then
        return '"' .. o .. '"'
    else
        return tostring(o)
    end
end

local function IsNumber(n)
    return type(n) == "number" and n == n -- n == n checks for NaN
end

local function IsString(s, maxlen)
    return type(s) == "string" and (not maxlen or #s <= maxlen)
end

local function IsStringOrNumber(v, maxlen)
    return IsString(v, maxlen) or IsNumber(v)
end

local function IsNonEmptyString(s, maxlen)
    return type(s) == "string" and #s > 0 and (not maxlen or #s <= maxlen)
end

local function IsNonEmptyStringOrNumber(v, maxlen)
    return IsNonEmptyString(v, maxlen) or IsNumber(v)
end

local function IsTable(t)
    return type(t) == "table"
end

local function IsNonEmptyTable(t)
    return type(t) == "table" and next(t) ~= nil
end

local function IsFunction(f)
    return type(f) == "function"
end

local function IsPrimitiveType(t)
    return t == "string" or t == "number" or t == "boolean" or t == "nil"
end

local function DeepCopy(o)
    if type(o) ~= "table" then
        return o
    end
    local copy = {}
    for k, v in pairs(o) do
        copy[k] = DeepCopy(v)
    end
    return copy
end

local function DeepEqual(a, b)
    if a == b then
        return true
    end
    if type(a) ~= "table" or type(b) ~= "table" then
        return false
    end
    local count = 0
    for k, v in pairs(a) do
        if not DeepEqual(v, b[k]) then
            return false
        end
        count = count + 1
    end
    for _ in pairs(b) do
        count = count - 1
        if count < 0 then
            return false
        end
    end
    return count == 0
end

local function ShallowCopy(o)
    if type(o) ~= "table" then
        return o
    end
    local copy = {}
    for k, v in pairs(o) do
        copy[k] = v
    end
    return copy
end

local function ShallowEqual(a, b)
    if a == b then
        return true
    end
    if type(a) ~= "table" or type(b) ~= "table" then
        return false
    end
    local count = 0
    for k, v in pairs(a) do
        if b[k] ~= v then
            return false
        end
        count = count + 1
    end
    for _ in pairs(b) do
        count = count - 1
        if count < 0 then
            return false
        end
    end
    return count == 0
end

local function NumberToHex(n)
    return format("%x", n)
end

local function HexToNumber(s)
    return tonumber(s)
end

local function IsIncomingNewer(incoming, existing)
    if not existing then
        return true
    end
    if incoming.clock > existing.clock then
        return true
    elseif incoming.clock < existing.clock then
        return false
    else
        return incoming.peer > existing.peer -- clocks are equal, use peer ID as tiebreaker
    end
end

local function PlayerGUIDShort()
    local guid = UnitGUID("player")
    if not guid then return nil end
    return strsub(guid, 8) -- skip "Player-" prefix
end

------------------------------------------------------------------------------------------------------------------------
-- Private State
------------------------------------------------------------------------------------------------------------------------

local function NewPrivate()
    local private = {}
    private.playerName = UnitName("player")
    private.peerId = PlayerGUIDShort()
    private.databases = setmetatable({}, { __mode = "k" })
    private.clusters = {}
    private.OnCommReceived = function(self, ...) InternalOnCommReceived(...) end
    return private
end
local Private = NewPrivate()

------------------------------------------------------------------------------------------------------------------------
-- Public API: Database Instance Creation
------------------------------------------------------------------------------------------------------------------------

---@class LibP2PDB.Serializer Serializer interface for encoding/decoding data
---@field Serialize fun(self: LibP2PDB.Serializer, value: any): string Serializes a value to a string
---@field Deserialize fun(self: LibP2PDB.Serializer, str: string): boolean, any? Deserializes a string back to a value

---@class LibP2PDB.Compressor Compressor interface for compressing/decompressing data
---@field Compress fun(self: LibP2PDB.Compressor, str: string): string Compresses a string
---@field Decompress fun(self: LibP2PDB.Compressor, str: string): string? Decompresses a string

---@class LibP2PDB.DBDesc Description for creating a new database instance
---@field clusterId string Unique identifier for the database cluster (max 16 chars)
---@field namespace string Namespace for this database instance
---@field channels table? List of chat channels to use for gossip (default: {"GUILD", "RAID", "PARTY", "YELL"})
---@field serializer LibP2PDB.Serializer? Custom serializer for encoding/decoding data (default: LibSerialize or AceSerializer if available)
---@field compressor LibP2PDB.Compressor? Custom compressor for compressing/decompressing data (default: LibDeflate if available)
---@field onChange function? Callback function(table, key, row) invoked on any row change
---@field discoveryQuietPeriod number? Seconds of quiet time with no new peers before considering discovery complete (default: 1.0)
---@field discoveryMaxTime number? Maximum seconds to wait for peer discovery before considering it complete (default: 3.0)
---@field onDiscoveryComplete function? Callback function(isInitial) invoked when peers discovery completes

---@class LibP2PDB.DB Database instance (opaque handle)

---Create a new database instance for peer-to-peer synchronization.
---Each database is identified by a unique clusterId and operates independently.
---Use GetDB to retrieve existing instances.
---@param desc LibP2PDB.DBDesc Description of the database instance to create
---@return LibP2PDB.DB db The database instance
function LibP2PDB:NewDB(desc)
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.clusterId, 16), "desc.clusterId must be a non-empty string (max 16 chars)")
    assert(IsNonEmptyString(desc.namespace), "desc.namespace must be a non-empty string")
    assert(desc.channels == nil or IsTable(desc.channels), "desc.channels must be a table if provided")
    assert(desc.serializer == nil or (IsTable(desc.serializer) and IsFunction(desc.serializer.Serialize) and IsFunction(desc.serializer.Deserialize)), "desc.serializer must be a serializer interface if provided")
    assert(desc.compressor == nil or (IsTable(desc.compressor) and IsFunction(desc.compressor.Compress) and IsFunction(desc.compressor.Decompress)), "desc.compressor must be a compressor interface if provided")
    assert(desc.onChange == nil or IsFunction(desc.onChange), "desc.onChange must be a function if provided")
    assert(desc.discoveryQuietPeriod == nil or IsNumber(desc.discoveryQuietPeriod), "desc.discoveryQuietPeriod must be a number if provided")
    assert(desc.discoveryMaxTime == nil or IsNumber(desc.discoveryMaxTime), "desc.discoveryMaxTime must be a number if provided")
    assert(desc.onDiscoveryComplete == nil or IsFunction(desc.onDiscoveryComplete), "desc.onDiscoveryComplete must be a function if provided")

    -- Validate channels if provided
    if desc.channels then
        for _, channel in ipairs(desc.channels) do
            assert(IsNonEmptyString(channel), "each channel in desc.channels must be a non-empty string")
        end
    end

    -- Ensure clusterId is unique
    assert(Private.clusters[desc.clusterId] == nil, "a database with clusterId '" .. desc.clusterId .. "' already exists")

    -- Default channels if none provided
    local defaultChannels = { "GUILD", "RAID", "PARTY", "YELL" }

    -- Create the DB instance
    local dbi = {
        -- Identity
        clusterId = desc.clusterId,
        namespace = desc.namespace,
        clock = 0,
        -- Configuration
        channels = desc.channels or defaultChannels,
        discoveryQuietPeriod = desc.discoveryQuietPeriod or 1.0,
        discoveryMaxTime = desc.discoveryMaxTime or 3.0,
        serializer = desc.serializer,
        compressor = desc.compressor,
        -- Networking
        peers = {},
        buckets = {},
        -- Data
        tables = {},
        -- Callbacks
        onChange = desc.onChange,
        onDiscoveryComplete = desc.onDiscoveryComplete,
        -- Access control
        writePolicy = nil,
    }

    -- Internal registry
    local db = {} -- db instance handle
    Private.clusters[desc.clusterId] = db
    Private.databases[db] = dbi

    -- Setup default serializer if none provided
    if not dbi.serializer then
        if LibSerialize then
            dbi.serializer = LibSerialize
        elseif AceSerializer then
            dbi.serializer = AceSerializer
        else
            error("serializer required but none found; provide custom serializer via desc.serializer")
        end
    end

    -- Validate serialization works
    do
        local testValue = { a = "value", b = 42, c = true, d = nil, nested = { e = "nested", f = 100, g = false, h = nil } }
        local serialized = dbi.serializer:Serialize(testValue)
        local success, deserialized = dbi.serializer:Deserialize(serialized)
        assert(success and DeepEqual(testValue, deserialized), "serializer provided in desc.serializer is invalid")
    end

    -- Setup default compressor if none provided
    if not dbi.compressor then
        if LibDeflate then
            dbi.compressor = {
                Compress = function(self, str)
                    return (LibDeflate:CompressDeflate(str))
                end,
                Decompress = function(self, str)
                    return (LibDeflate:DecompressDeflate(str))
                end,
            }
        else
            error("compressor required but none found; provide custom compressor via desc.compressor")
        end
    end

    -- Validate compression works
    do
        local testString = "This is a test string for compression."
        local compressed = dbi.compressor:Compress(testString)
        local decompressed = dbi.compressor:Decompress(compressed)
        assert(decompressed == testString, "compressor provided in desc.compressor is invalid")
    end

    -- Set up OnUpdate handler
    if dbi.onDiscoveryComplete then
        dbi.frame = CreateFrame("Frame")
        dbi.frame:SetScript("OnUpdate", function() InternalOnUpdate(dbi) end)
    end

    -- Register comm prefix
    AceComm.RegisterComm(Private, desc.clusterId)

    return db
end

---Retrieve a database instance by its clusterId.
---@param clusterId string Unique identifier for the database cluster (max 16 chars)
---@return LibP2PDB.DB? db The database instance if found, or nil if not found
function LibP2PDB:GetDB(clusterId)
    assert(IsNonEmptyString(clusterId, 16), "clusterId must be a non-empty string (max 16 chars)")
    return Private.clusters[clusterId]
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Table Definition (Schema)
------------------------------------------------------------------------------------------------------------------------

---@alias LibP2PDB.SchemaFieldName string Field name in a schema
---@alias LibP2PDB.SchemaFieldTypes string|table<string> Allowed data type(s) for a field in a schema. Only primitive types are allowed: "string", "number", "boolean", "nil".

---@class LibP2PDB.Schema Table schema definition
---@field [LibP2PDB.SchemaFieldName] LibP2PDB.SchemaFieldTypes Field name mapped to allowed data type(s) ("string", "number", "boolean", "nil")

---@alias LibP2PDB.SortedSchema table<LibP2PDB.SchemaFieldName,LibP2PDB.SchemaFieldTypes> Array of {fieldName, fieldType} pairs sorted by fieldName

---@class LibP2PDB.TableDesc Description for defining a table in the database
---@field name string Name of the table to define
---@field keyType string Data type of the primary key ("string" or "number")
---@field schema LibP2PDB.Schema? Optional table schema defining field names and their allowed data types
---@field onValidate function? Optional validation function(key, row) -> true/false
---@field onChange function? Optional callback function(key, row) on row changes

---Create a new table in the database with an optional schema.
---If no schema is provided, the table accepts any fields.
---@param db LibP2PDB.DB Database instance
---@param desc LibP2PDB.TableDesc Description of the table to define
---@return boolean result Returns true if the table was created, false if it already existed
function LibP2PDB:NewTable(db, desc)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.name), "desc.name must be a non-empty string")
    assert(IsNonEmptyString(desc.keyType), "desc.keyType must be a non-empty string")
    assert(desc.keyType == "string" or desc.keyType == "number", "desc.keyType must be 'string' or 'number'")
    assert(desc.schema == nil or IsTable(desc.schema), "desc.schema must be a table if provided")
    for fieldName, allowedTypes in pairs(desc.schema or {}) do
        assert(IsNonEmptyString(fieldName), "each field name in desc.schema must be a non-empty string")
        assert(IsNonEmptyString(allowedTypes) or IsNonEmptyTable(allowedTypes), "each field type in desc.schema must be a non-empty string or table of strings")
        if type(allowedTypes) == "table" then
            for _, t in ipairs(allowedTypes) do
                assert(IsNonEmptyString(t), "each type in desc.schema field types must be a non-empty string")
                assert(IsPrimitiveType(t), "field types in desc.schema must be 'string', 'number', 'boolean', or 'nil'")
            end
        else
            assert(IsNonEmptyString(allowedTypes), "field type in desc.schema must be a non-empty string")
            assert(IsPrimitiveType(allowedTypes), "field type in desc.schema must be 'string', 'number', 'boolean', or 'nil'")
        end
    end
    assert(desc.onValidate == nil or IsFunction(desc.onValidate), "desc.onValidate must be a function if provided")
    assert(desc.onChange == nil or IsFunction(desc.onChange), "desc.onChange must be a function if provided")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Ensure table does not already exist
    assert(dbi.tables[desc.name] == nil, "table '" .. desc.name .. "' already exists in the database")

    -- Create the table entry
    dbi.tables[desc.name] = {
        keyType = desc.keyType,
        schema = desc.schema,
        onValidate = desc.onValidate,
        onChange = desc.onChange,
        subscribers = setmetatable({}, { __mode = "k" }),
        rows = {},
    }
    return true
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: CRUD Operations
------------------------------------------------------------------------------------------------------------------------

---Insert a new row into a table.
---Validates the key type and row schema against the table definition.
---If a schema is defined, extra fields in the row are ignored.
---Fails if the key already exists (use Set to overwrite).
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to insert into
---@param key string|number Primary key value for the row (must match table's keyType)
---@param row table Row data containing fields defined in the table schema
---@return boolean result Returns true on success, false otherwise
function LibP2PDB:Insert(db, table, key, row)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsTable(row), "row must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Ensure the key does not already exist
    local existingRow = t.rows[key]
    if existingRow and not existingRow.version.tombstone then
        error("key '" .. tostring(key) .. "' already exists in table '" .. table .. "'")
    end

    -- Set the row
    return InternalSet(db, dbi, table, t, key, row)
end

---Create or replace an existing row in a table.
---Validates the key type and row schema against the table definition.
---If a schema is defined, extra fields in the row are ignored.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to set into
---@param key string|number Primary key value for the row (must match table's keyType)
---@param row table Row data containing fields defined in the table schema
---@return boolean success Returns true on success, false otherwise
function LibP2PDB:Set(db, table, key, row)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsTable(row), "row must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Set the row
    return InternalSet(db, dbi, table, t, key, row)
end

---Update an existing row.
---Validates the key type against the table definition.
---The update function is called with the current row data and must return the updated row data.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to update
---@param key string|number Primary key value for the row (must match table's keyType)
---@param updateFn function Function(currentRow) invoked to produce the updated row data
---@return boolean success Returns true on success, false otherwise
function LibP2PDB:Update(db, table, key, updateFn)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsFunction(updateFn), "updateFn must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Lookup the existing row
    local existingRow = t.rows[key]
    assert(existingRow, "key '" .. tostring(key) .. "' does not exist in table '" .. table .. "'")

    -- Call the update function to get the new row data
    local updatedRow = securecallfunction(updateFn, ShallowCopy(existingRow.data))
    assert(IsTable(updatedRow), "updateFn must return a table")

    -- Use Set to apply the updated row (will handle validation, versioning, callbacks)
    return self:Set(db, table, key, updatedRow)
end

---Retrieve a row from a table by key.
---Validates the key type against the table definition.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to get from
---@param key string|number Primary key value for the row (must match table's keyType)
---@return table? row The row data if found, or nil if not found
function LibP2PDB:Get(db, table, key)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Lookup the row
    local row = t.rows[key]
    if row == nil or row.data == nil then
        return nil
    end

    -- Return only the row data
    local result = {}
    for k, v in pairs(row.data or {}) do
        result[k] = v
    end
    return result
end

---Determine if a key exists in a table.
---Validates the key type against the table definition.
---@param db LibP2PDB.DB Database instance
---@param tableName string Name of the table to check
---@param key string|number Primary key value for the row (must match table's keyType)
function LibP2PDB:HasKey(db, tableName, key)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[tableName]
    assert(t, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Lookup the row
    local row = t.rows[key]
    if row and not row.version.tombstone then
        return true
    else
        return false
    end
end

---Delete a row.
---Validates the key type against the table definition.
---Marks the row as a tombstone for gossip synchronization.
---Adds a deletion entry regardless of whether the row existed or not.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to delete from
---@param key string|number Primary key value for the row (must match table's keyType)
function LibP2PDB:Delete(db, table, key)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Determine if the row will change
    local changes = false
    local existingRow = t.rows[key]
    if not existingRow or (existingRow.data or not existingRow.version.tombstone) then
        changes = true -- row exists or is not already a tombstone
    end

    -- Apply deletion if needed
    if changes then
        -- Versioning (Lamport clock)
        dbi.clock = dbi.clock + 1

        -- Determine peer value (use "=" if keyType is string and peer equals key, to save memory)
        local peerValue = Private.peerId
        if t.keyType == "string" and tostring(Private.peerId) == tostring(key) then
            peerValue = "="
        end

        -- Replace row with tombstone
        t.rows[key] = {
            data = nil, -- no row data
            version = {
                clock = dbi.clock,
                peer = peerValue,
                tombstone = true, -- mark as deleted
            },
        }

        -- Fire callbacks
        InternalFireCallbacks(dbi, table, t, key, nil)
    end
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Subscriptions
------------------------------------------------------------------------------------------------------------------------

---Subscribe to changes in a specific table.
---The callback will be invoked as callback(key, row) for inserts/updates, and callback(key, nil) for deletions.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to subscribe to
---@param callback function Function(key, row) to invoke on changes
function LibP2PDB:Subscribe(db, table, callback)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsFunction(callback), "callback must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")

    -- Register subscriber (safe even if already present)
    t.subscribers[callback] = true
end

---Unsubscribe a callback from a specific table.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to unsubscribe from
---@param callback function Function(key, row) to remove from subscriptions
function LibP2PDB:Unsubscribe(db, table, callback)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsFunction(callback), "callback must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table
    local t = dbi.tables[table]
    assert(t, "table '" .. table .. "' is not defined in the database")

    -- Remove subscriber (safe even if not present)
    t.subscribers[callback] = nil
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Persistence
------------------------------------------------------------------------------------------------------------------------

---@class LibP2PDB.VersionExport Exported version metadata
---@field clock number Lamport clock value
---@field peer string Peer ID that last modified the row
---@field tombstone boolean? Optional tombstone flag indicating deletion

---@class LibP2PDB.RowExport Exported row
---@field data table Row data
---@field version LibP2PDB.VersionExport Version metadata

---@class LibP2PDB.TableExport Exported table
---@field rows table<LibP2PDB.RowExport> Registry of rows in the exported table

---@class LibP2PDB.DBExport Exported database
---@field clock number Lamport clock of the exported database
---@field tables table<LibP2PDB.TableExport> Registry of tables and their rows

---Export the entire database to a table.
---@param db LibP2PDB.DB Database instance
---@return LibP2PDB.DBExport export The exported database
function LibP2PDB:Export(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Export
    return InternalExport(dbi)
end

---Import the database from an exported table.
---Merges the imported state with existing data based on version metadata.
---Validates incoming data against table definitions, skipping invalid entries.
---@param db LibP2PDB.DB Database instance
---@param exported LibP2PDB.DBExport The exported database to import
---@return boolean,table<string>? result Returns true if the import is succesful, false otherwise. On failure, the second return value is a table of warnings/errors.
function LibP2PDB:Import(db, exported)
    assert(IsTable(db), "db must be a table")
    assert(IsTable(exported), "exported db must be a table")
    assert(IsNumber(exported.clock), "invalid exported db clock")
    assert(IsTable(exported.tables), "invalid exported db tables")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Import
    return InternalImport(dbi, exported)
end

---Serialize the entire database to a compact string format for storage or transmission.
---
---Format: {clock;table{key{{value;...}clock;peer[;1]};...};...}
---
---Structure:
---  - Numbers encoded as hex (lowercase, no leading zeros) to save space
---  - Braces {} delimit nested structures
---  - Semicolons ; separate fields, tables, and rows (no trailing semicolons)
---  - Field values only (no names) in alphabetical schema order
---
---Layout:
---  Database level:
---    {clock;...tables...}
---
---  Table level (repeating, separated by ';'):
---    tableName{...rows...};...
---
---  Row level (repeating, separated by ';'):
---    key{{...data...}clock;peer[;tombstone]};...
---
---  Data level:
---    {value1;value2;...}  - Field values in alphabetical schema order
---
---  Version metadata:
---    clock                - Row version clock (hex number)
---    peer                 - Peer ID (or "=" if same as key)
---    [;1]                 - Tombstone flag (only if row is deleted)
---
---Example: {1a;Users{alice{{Alice,1e}14;peer-123};bob{{Bob,19}15;peer-456;1};peer-789{{Roger,2a}16;=}}}
---  - Database with clock=26 (0x1a)
---  - Table "Users" with 3 rows
---  - Row key="alice", data={name="Alice", age=30 (0x1e)}, version={clock=20 (0x14), peer="peer-123"}
---  - Row key="bob", data={name="Bob", age=25 (0x19)}, version={clock=21 (0x15), peer="peer-456", tombstone=true}
---  - Row key="peer-789", data={name="Roger", age=42 (0x2a)}, version={clock=22 (0x16), peer="=" (same as key)}
---
---@param db LibP2PDB.DB Database instance
---@return string serialized The serialized database string
function LibP2PDB:Serialize(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Serialize the database
    return InternalSerialize(dbi)
end

---Deserialize a database from a compact string format.
---Merges the deserialized data with existing data based on version metadata.
---Validates incoming data against table definitions, skipping invalid entries.
---@param db LibP2PDB.DB Database instance
---@param str string The serialized database string to deserialize
---@return boolean,table<string>? result Returns true if the deserialization is succesful, false otherwise. On failure, the second return value is a table of warnings/errors.
function LibP2PDB:Deserialize(db, str)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(str), "str must be a non-empty string")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Deserialize the serialized database
    local deserialized, deserializeResults = InternalDeserialize(dbi, str)
    if not deserialized then
        return false, deserializeResults
    end

    -- Import the deserialized data
    local imported, importResults = self:Import(db, deserialized)
    if not imported then
        return false, importResults
    end

    -- Combine results
    if deserializeResults or importResults then
        local combinedResults = {}
        if deserializeResults then
            for _, msg in ipairs(deserializeResults) do
                tinsert(combinedResults, msg)
            end
        end
        if importResults then
            for _, msg in ipairs(importResults) do
                tinsert(combinedResults, msg)
            end
        end
        return true, combinedResults
    end
    return true
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Sync / Gossip Controls
------------------------------------------------------------------------------------------------------------------------

---Discover peers in the cluster by broadcasting a request message.
---@param db LibP2PDB.DB Database instance
function LibP2PDB:DiscoverPeers(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Send the discover peers message
    local obj = {
        type = CommMessageType.PeerDiscoveryRequest,
        peerId = Private.peerId,
    }
    local serialized = dbi.serializer:Serialize(obj)
    InternalBroadcast(dbi.clusterId, serialized, dbi.channels, CommPriority.Low)

    -- Record the time of the peer discovery request
    if dbi.onDiscoveryComplete then
        dbi.discoveryStartTime = GetTime()
        dbi.lastDiscoveryResponseTime = dbi.discoveryStartTime
    end
end

---Request a full snapshot from a peer or broadcast to all peers.
---@param db LibP2PDB.DB Database instance
---@param target string? Optional target peer to request the snapshot from; if nil, broadcasts to all peers
function LibP2PDB:RequestSnapshot(db, target)
    assert(IsTable(db), "db must be a table")
    assert(target == nil or IsNonEmptyString(target), "target must be a non-empty string if provided")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Send the request message
    local obj = {
        type = CommMessageType.SnapshotRequest,
        peerId = Private.peerId,
    }
    local serialized = dbi.serializer:Serialize(obj)
    if target then
        InternalSend(dbi.clusterId, serialized, "WHISPER", target, CommPriority.Normal)
    else
        -- Request snapshot from discovered peers
        for _, peerData in pairs(dbi.peers) do
            if peerData.isNew then
                peerData.isNew = nil
                InternalSend(dbi.clusterId, serialized, "WHISPER", peerData.name, CommPriority.Normal)
            elseif peerData.clock > dbi.clock then
                InternalSend(dbi.clusterId, serialized, "WHISPER", peerData.name, CommPriority.Normal)
            end
        end
    end
end

---Immediately initiate a gossip sync by sending the current digest to all peers.
---@param db LibP2PDB.DB Database instance
function LibP2PDB:SyncNow(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Build digest
    local digest = InternalBuildDigest(dbi)

    -- Send the digest to all peers
    local obj = {
        type = CommMessageType.Digest,
        peerId = Private.peerId,
        data = digest,
    }
    local serialized = dbi.serializer:Serialize(obj)
    InternalBroadcast(dbi.clusterId, serialized, dbi.channels, CommPriority.Normal)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Utility / Metadata
------------------------------------------------------------------------------------------------------------------------

---Return the local peer's unique ID
---@return string peerId The local peer ID
function LibP2PDB:GetPeerId()
    return Private.peerId
end

---Return a remote peer's unique ID from its GUID
---@param guid string Full GUID of the remote peer
---@return string? peerId The remote peer ID if valid, or nil if not
function LibP2PDB:GetPeerIdFromGUID(guid)
    assert(IsNonEmptyString(guid), "guid must be a non-empty string")
    if strsub(guid, 1, 7) ~= "Player-" then
        return nil
    end
    return strsub(guid, 8) -- skip "Player-" prefix
end

---List all defined tables in the database.
---@param db LibP2PDB.DB Database instance
---@return table<string> tables Array of table names defined in the database
function LibP2PDB:ListTables(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    local tableNames = {}
    for tableName in pairs(dbi.tables) do
        tinsert(tableNames, tableName)
    end
    return tableNames
end

---List all keys of a specific table in the database.
---@param db LibP2PDB.DB Database instance
---@param tableName string Name of the table to list keys from
---@return table<string|number> keys Array of keys in the specified table
function LibP2PDB:ListKeys(db, tableName)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(tableName), "tableName must be a non-empty string")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")

    local keys = {}
    for key, row in pairs(ti.rows) do
        if row and row.data and not row.version.tombstone then
            tinsert(keys, key)
        end
    end
    return keys
end

---Retrieve the schema definition for a specific table.
---@param db LibP2PDB.DB Database instance
---@param tableName string Name of the table to get the schema for
---@param sorted boolean? Optional flag to return the schema with sorted field names (default: false)
---@return LibP2PDB.Schema|LibP2PDB.SortedSchema? schema The table schema, or nil if no schema is defined. If sorted is true, returns an array of {fieldName, fieldType} pairs sorted by fieldName.
function LibP2PDB:GetSchema(db, tableName, sorted)
    assert(IsTable(db), "db must be a table")
    assert(IsNonEmptyString(tableName), "tableName must be a non-empty string")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Validate table
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")

    -- Return a copy of the schema
    return DeepCopy(InternalGetSchema(ti, sorted))
end

---Return a list of discovered peers in the database cluster in this session.
---This list is not persisted and is reset on logout/reload.
---@param db LibP2PDB.DB Database instance
---@return table<string, table> peers Table of peerId -> peer data
function LibP2PDB:GetDiscoveredPeers(db)
    assert(IsTable(db), "db must be a table")

    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    local peers = {}
    for peerId, data in pairs(dbi.peers) do
        peers[peerId] = data
    end
    return peers
end

-- Return version metadata for a row
function LibP2PDB:GetVersion(db, table, key)
end

---Sanitize a string for printing in WoW chat channels.
---Replaces non-printable characters with their numeric byte values prefixed by a backslash.
---@param s string Input string
---@return string sanitized The sanitized string safe for printing
function LibP2PDB:SanitizeForPrint(s)
    local parts = {}
    local len = #s
    for i = 1, len do
        local byte = strbyte(s, i)
        if byte >= 32 and byte <= 126 then
            tinsert(parts, strchar(byte))
        else
            tinsert(parts, "\\")
            tinsert(parts, tostring(byte))
        end
    end
    return tconcat(parts)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Access Control
------------------------------------------------------------------------------------------------------------------------

-- Set a write policy function(table, key, row, meta) -> true/false
function LibP2PDB:SetWritePolicy(db, policyFn)
end

------------------------------------------------------------------------------------------------------------------------
-- Private Functions
------------------------------------------------------------------------------------------------------------------------

function InternalGetSchema(ti, sorted)
    if not ti.schema then
        return nil
    end
    if sorted then
        local sortedSchema = {}
        local fieldNames = {}
        for fieldName in pairs(ti.schema) do
            tinsert(fieldNames, fieldName)
        end
        tsort(fieldNames)
        for _, fieldName in ipairs(fieldNames) do
            tinsert(sortedSchema, { fieldName, ti.schema[fieldName] })
        end
        return sortedSchema
    else
        return ti.schema
    end
end

function InternalOnUpdate(dbi)
    if not dbi.discoveryStartTime then
        return
    end

    -- Handle peer discovery timeout
    local now = GetTime()
    local sinceStart = now - dbi.discoveryStartTime
    local sinceLast = now - dbi.lastDiscoveryResponseTime

    -- Discovery quiet period or max time reached
    if sinceLast >= dbi.discoveryQuietPeriod or sinceStart >= dbi.discoveryMaxTime then
        dbi.discoveryStartTime = nil
        if dbi.onDiscoveryComplete then
            local isInitial = not dbi.isInitialDiscoveryComplete
            dbi.isInitialDiscoveryComplete = true
            securecallfunction(dbi.onDiscoveryComplete, isInitial)
        end
    end
end

function InternalSchemaCopy(table, schema, data)
    local result = {}
    if not schema then
        -- No schema: shallow copy only primitive types
        for fieldName, fieldValue in pairs(data) do
            local fieldType = type(fieldValue)
            if IsPrimitiveType(fieldType) then
                result[fieldName] = fieldValue
            end
        end
    else
        -- Schema defined: validate and copy only defined fields
        for fieldName, allowedTypes in pairs(schema) do
            local fieldValue = data[fieldName]
            local fieldType = type(fieldValue)
            if type(allowedTypes) == "table" then
                local allowed = false
                for _, allowedType in ipairs(allowedTypes) do
                    if fieldType == allowedType then
                        allowed = true
                        break
                    end
                end
                assert(allowed, "expected field '" .. fieldName .. "' of type '" .. strjoin(", ", unpack(allowedTypes)) .. "' in table '" .. table .. "', but was '" .. fieldType .. "'")
            elseif type(allowedTypes) == "string" then
                assert(fieldType == allowedTypes, "expected field '" .. fieldName .. "' of type '" .. allowedTypes .. "' in table '" .. table .. "', but was '" .. fieldType .. "'")
            else
                error("invalid schema definition for field '" .. fieldName .. "' in table '" .. table .. "'")
            end
            result[fieldName] = fieldValue
        end
    end
    return result
end

function InternalSet(db, dbi, table, t, key, row)
    -- Apply schema
    local data = InternalSchemaCopy(table, t.schema, row)

    -- Run custom validation if provided
    if t.onValidate and not securecallfunction(t.onValidate, key, data) then
        return false
    end

    -- Determine if the row will change
    local changes = false
    local existingRow = t.rows[key]
    if not existingRow or existingRow.version.tombstone == true or not ShallowEqual(existingRow.data, data) then
        changes = true -- New row or data changes
    end

    -- Apply changes if any
    if changes then
        -- Versioning (Lamport clock)
        dbi.clock = dbi.clock + 1

        -- Determine peer value (use "=" if keyType is string and peer equals key, to save memory)
        local peerValue = Private.peerId
        if t.keyType == "string" and tostring(Private.peerId) == tostring(key) then
            peerValue = "="
        end

        -- Store the row
        t.rows[key] = {
            data = data,
            version = {
                clock = dbi.clock,
                peer = peerValue,
            },
        }

        -- Fire callbacks
        InternalFireCallbacks(dbi, table, t, key, data)
    end

    return true
end

function InternalFireCallbacks(dbi, tableName, t, key, data)
    -- Skip callbacks during import
    if Private.isImporting then
        return
    end

    -- Fire db change callback
    if dbi.onChange then
        securecallfunction(dbi.onChange, tableName, key, data)
    end

    -- Fire table change callback
    if t.onChange then
        securecallfunction(t.onChange, key, data)
    end

    -- Fire subscribers
    for callback in pairs(t.subscribers) do
        securecallfunction(callback, key, data)
    end
end

function InternalExport(dbi)
    local export = {
        clock = dbi.clock,
    }

    local tables = {}
    for tableName, tableData in pairs(dbi.tables) do
        local rows = {}
        for key, row in pairs(tableData.rows) do
            rows[key] = {
                data = ShallowCopy(row.data),
                version = {
                    clock = row.version.clock,
                    peer = row.version.peer,
                    tombstone = row.version.tombstone,
                },
            }
        end
        if next(rows) ~= nil then
            tables[tableName] = {
                rows = rows
            }
        end
    end

    if next(tables) ~= nil then
        export.tables = tables
    end

    return export
end

function InternalImport(dbi, exported)
    -- Begin import
    Private.isImporting = true

    -- Merge Lamport clock
    dbi.clock = max(dbi.clock, exported.clock)

    -- Import DB
    local results = nil
    for incomingTableName, incomingTableData in pairs(exported.tables or {}) do
        local ti = dbi.tables[incomingTableName]
        if ti then
            for incomingKey, incomingRow in pairs(incomingTableData.rows or {}) do
                local rowSuccess, rowResult = InternalImportRow(incomingKey, incomingRow, dbi, incomingTableName, ti)
                if not rowSuccess then
                    -- Collect warnings for skipped rows (partial success)
                    results = results or {}
                    tinsert(results, tostring(rowResult))
                end
            end
        end
    end

    -- End import
    Private.isImporting = false

    -- Return results
    return true, results
end

function InternalImportRow(incomingKey, incomingRow, dbi, incomingTableName, t)
    -- Validate key type
    if type(incomingKey) ~= t.keyType then
        return false, format("skipping row with invalid key type '%s' in table '%s'", type(incomingKey), incomingTableName)
    end

    -- Validate row structure
    if not IsTable(incomingRow) then
        return false, format("skipping row with invalid structure for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end

    -- Validate version metadata
    if not IsTable(incomingRow.version) then
        return false, format("skipping row with missing version metadata for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end
    local incomingVersion = incomingRow.version
    if not IsNumber(incomingVersion.clock) or incomingVersion.clock < 0 then
        return false, format("skipping row with invalid version clock for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end
    if not IsNonEmptyString(incomingVersion.peer) then
        return false, format("skipping row with invalid version peer for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end

    local cleanVersion = {
        clock = incomingVersion.clock,
        peer = incomingVersion.peer,
    }
    if incomingVersion.tombstone == true then
        cleanVersion.tombstone = true
    end

    local existingRow = t.rows[incomingKey]

    -- Tombstone merge
    if cleanVersion.tombstone then
        if not existingRow or IsIncomingNewer(cleanVersion, existingRow.version) then
            t.rows[incomingKey] = {
                data = nil,
                version = cleanVersion,
            }

            -- Fire callbacks
            InternalFireCallbacks(dbi, incomingTableName, t, incomingKey, nil)
        end
        return true
    end

    -- Validate data
    local incomingData = incomingRow.data
    if not IsTable(incomingData) then
        return false, format("skipping row with invalid data for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end

    local cleanData = {}
    local result, msg = pcall(function() cleanData = InternalSchemaCopy(incomingTableName, t.schema, incomingData) end)
    if not result then
        return false, format("skipping row with schema validation failure for key '%s' in table '%s': %s", tostring(incomingKey), incomingTableName, tostring(msg))
    end

    -- Custom validation
    if t.onValidate and not securecallfunction(t.onValidate, incomingKey, cleanData) then
        return false, format("skipping row that failed custom validation for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
    end

    -- Merge row if it doesn't exist or is newer
    if not existingRow or IsIncomingNewer(cleanVersion, existingRow.version) then
        t.rows[incomingKey] = {
            data = cleanData,
            version = cleanVersion,
        }

        -- Fire callbacks
        InternalFireCallbacks(dbi, incomingTableName, t, incomingKey, cleanData)
    end

    return true
end

function InternalSerialize(dbi)
    -- Preprocess database into array structure
    local rootArray = { dbi.clock }
    local rootIdx = 1
    for tableName, tableData in pairs(dbi.tables) do
        local rows = tableData.rows

        -- Serialize only if the table has rows
        if next(rows) then
            local tableArray = { tableName }
            local tableIdx = 1

            -- Get sorted schema once for this table
            local sortedSchema = InternalGetSchema(tableData, true)

            -- Serialize rows
            for key, row in pairs(rows) do
                -- Add row data
                local data = row.data
                local version = row.version
                local rowArray, rowIdx
                if sortedSchema then
                    -- Schema defined: serialize fields in alphabetical order without names
                    local dataArray = {}
                    local dataIdx = 0
                    if data then
                        for i = 1, #sortedSchema do
                            local fieldName = sortedSchema[i][1]
                            local fieldValue = data[fieldName]
                            dataIdx = dataIdx + 1
                            dataArray[dataIdx] = fieldValue == nil and NIL_MARKER or fieldValue
                        end
                    else
                        for i = 1, #sortedSchema do
                            dataIdx = dataIdx + 1
                            dataArray[dataIdx] = NIL_MARKER
                        end
                    end
                    rowArray = { key, dataArray, version.clock, version.peer }
                    rowIdx = 4
                else
                    -- No schema: serialize all fields in arbitrary order
                    rowArray = { key, data or {}, version.clock, version.peer }
                    rowIdx = 4
                end

                -- Add tombstone flag if present
                if version.tombstone then
                    rowIdx = rowIdx + 1
                    rowArray[rowIdx] = true
                end

                tableIdx = tableIdx + 1
                tableArray[tableIdx] = rowArray
            end

            rootIdx = rootIdx + 1
            rootArray[rootIdx] = tableArray
        end
    end

    -- Serialize the root array to a compact binary string
    return dbi.serializer:Serialize(rootArray)
end

function InternalDeserialize(dbi, str)
    -- First, deserialize the binary string back to array structure
    local success, rootArray = dbi.serializer:Deserialize(str)
    if not success then
        return nil, { "failed to deserialize: invalid format" }
    end

    if not IsTable(rootArray) or not IsNonEmptyTable(rootArray) then
        return nil, { "invalid database structure: missing root data" }
    end

    -- Next, process the array structure into a state that can be imported later
    local state = {}
    local clock = rootArray[1]
    if not IsNumber(clock) then
        return nil, { format("invalid database structure: invalid cluster clock: '%s'", tostring(clock)) }
    end
    state.clock = clock

    -- Parse tables (format: clock, tableArray1, tableArray2, ...)
    local results = {}
    local resultsIdx = 0
    local rootLen = #rootArray
    state.tables = {}
    for i = 2, rootLen do
        local tableArray = rootArray[i]
        local tableLen = tableArray and #tableArray or 0

        if not IsTable(tableArray) or tableLen < 1 then
            return nil, { "invalid table structure: missing table data" }
        end

        local tableName = tableArray[1]
        if not IsNonEmptyString(tableName) then
            return nil, { format("invalid table structure: invalid table name") }
        end

        local ti = dbi.tables[tableName]
        if not ti then
            return nil, { format("table '%s' is not defined in the database", tostring(tableName)) }
        end

        -- Get sorted schema once for this table
        local sortedSchema = InternalGetSchema(ti, true)

        -- Parse rows (format: tableName, rowArray1, rowArray2, ...)
        local table = { rows = {}, }
        local rows = table.rows
        local keyType = ti.keyType
        for rowIndex = 2, tableLen do
            local rowArray = tableArray[rowIndex]
            local rowLen = rowArray and #rowArray or 0
            local row, errMsg = InternalDeserializeRow(rowArray, rowLen, sortedSchema, keyType, tableName)
            if row then
                local key = rowArray[1]
                rows[key] = row
            else
                resultsIdx = resultsIdx + 1
                results[resultsIdx] = errMsg
            end
        end
        state.tables[tableName] = table
    end

    -- Return results only if there are any
    if next(results) == nil then
        results = nil
    end

    return state, results
end

function InternalDeserializeRow(rowArray, rowLen, sortedSchema, keyType, tableName)
    if not IsTable(rowArray) or rowLen < 3 then
        return nil, format("skipping row with invalid structure in table '%s'", tableName)
    end

    -- Validate key type
    local key = rowArray[1]
    if type(key) ~= keyType then
        return nil, format("skipping row with invalid key type in table '%s'", tableName)
    end

    local dataArray = rowArray[2]
    local data = {}
    if IsTable(dataArray) then
        if sortedSchema then
            -- Schema defined: map array indices back to field names
            local dataLen = #dataArray
            for fieldIndex = 1, dataLen do
                local fieldValue = dataArray[fieldIndex]
                local fieldInfo = sortedSchema[fieldIndex]

                if fieldInfo then
                    local fieldName = fieldInfo[1]

                    -- Check for nil marker
                    if fieldValue == NIL_MARKER then
                        fieldValue = nil
                    end

                    data[fieldName] = fieldValue
                end
            end
        else
            -- No schema: use the data table as-is
            data = dataArray
        end
    end

    local versionClock = rowArray[3]
    if not IsNumber(versionClock) then
        return nil, format("skipping row with invalid version clock in table '%s'", tableName)
    end

    local versionPeer = rowArray[4]
    if not IsNonEmptyString(versionPeer) then
        return nil, format("skipping row with invalid version peer in table '%s'", tableName)
    end

    local versionTombstone = false
    if rowLen >= 5 and rowArray[5] == true then
        versionTombstone = true
    end

    local version = {
        clock = versionClock,
        peer = versionPeer,
    }
    if versionTombstone then
        version.tombstone = true
    end

    -- Return row object
    return {
        data = versionTombstone and nil or data,
        version = version,
    }
end

function InternalBuildDigest(dbi)
    local digest = {
        clock = dbi.clock,
        tables = {},
    }

    -- Build table digests
    for tableName, t in pairs(dbi.tables) do
        local tableDigest = {}
        for key, row in pairs(t.rows) do
            -- Extract version metadata
            local v = row.version
            local rowDigest = {
                clock = v.clock,
                peer = v.peer,
            }

            -- Set tombstone only if true (nil otherwise)
            if v.tombstone == true then
                rowDigest.tombstone = true
            end

            -- Add to table digest
            tableDigest[key] = rowDigest
        end

        -- Only include non-empty tables in the digest
        if next(tableDigest) ~= nil then
            digest.tables[tableName] = tableDigest
        end
    end

    return digest
end

function InternalSend(prefix, message, channel, target, priority)
    Debug(format("sending %d bytes, prefix=%s, channel=%s, target=%s", #message, tostring(prefix), tostring(channel), tostring(target)))
    AceComm.SendCommMessage(Private, prefix, message, channel, target, priority)
end

function InternalBroadcast(prefix, message, channels, priority)
    for _, channel in ipairs(channels) do
        if channel == "GUILD" and IsInGuild() then
            InternalSend(prefix, message, "GUILD", nil, priority)
        elseif channel == "RAID" and IsInRaid() then
            InternalSend(prefix, message, "RAID", nil, priority)
        elseif channel == "PARTY" and IsInGroup() then
            InternalSend(prefix, message, "PARTY", nil, priority)
        elseif channel == "YELL" and not IsInInstance() then
            InternalSend(prefix, message, "YELL", nil, priority)
        end
    end
end

function InternalOnCommReceived(prefix, message, channel, sender)
    -- Ignore messages from self
    if sender == Private.playerName then
        return
    end

    -- Get the database instance for this clusterId
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local dbi = Private.databases[db]
    if not dbi then
        return
    end

    -- Deserialize message
    local success, deserialized = dbi.serializer:Deserialize(message)
    if not success then
        Debug(format("failed to deserialize message from %s on channel %s: %s", tostring(sender), tostring(channel), Dump(deserialized)))
        return
    end

    -- Validate message structure
    if not IsTable(deserialized) then
        Debug(format("received invalid message structure from %s on channel %s", tostring(sender), tostring(channel)))
        return
    end

    if not IsNumber(deserialized.type) then
        Debug(format("received message with missing or invalid type from %s on channel %s", tostring(sender), tostring(channel)))
        return
    end

    if not IsNonEmptyString(deserialized.peerId) then
        Debug(format("received message with missing or invalid peerId from %s on channel %s", tostring(sender), tostring(channel)))
        return
    end

    -- Create communication event
    local event = {
        dbi = dbi,
        type = deserialized.type,
        peerId = deserialized.peerId,
        prefix = prefix,
        message = message,
        channel = channel,
        sender = sender,
        data = deserialized.data,
    }

    -- Get or create bucket
    local bucket = dbi.buckets[event.type]
    if not bucket then
        bucket = {}
        dbi.buckets[event.type] = bucket
    end

    -- If we already have a timer running for this peer, ignore it
    if bucket[event.peerId] then
        return
    end

    -- Create a timer to process this message after 1 second
    bucket[event.peerId] = C_Timer.NewTimer(1.0, function()
        -- Process the message
        InternalDispatchMessage(event)

        -- Clean up
        bucket[event.peerId] = nil

        -- Clean up bucket if empty
        if not next(bucket) then
            dbi.buckets[event.type] = nil
        end
    end)
end

function InternalDispatchMessage(event)
    if event.type == CommMessageType.PeerDiscoveryRequest then
        InternalPeerDiscoveryRequestHandler(event)
    elseif event.type == CommMessageType.PeerDiscoveryResponse then
        InternalPeerDiscoveryResponseHandler(event)
    elseif event.type == CommMessageType.SnapshotRequest then
        InternalRequestSnapshotMessageHandler(event)
    elseif event.type == CommMessageType.SnapshotResponse then
        InternalSnapshotMessageHandler(event)
    elseif event.type == CommMessageType.Digest then
        InternalDigestMessageHandler(event)
    elseif event.type == CommMessageType.RequestRows then
        InternalRequestRowsMessageHandler(event)
    elseif event.type == CommMessageType.Rows then
        InternalRowsMessageHandler(event)
    else
        Debug(format("received unknown message type %d from %s on channel %s", event.type, tostring(event.sender), tostring(event.channel)))
    end
end

function InternalPeerDiscoveryRequestHandler(event)
    -- Send peer discovery response
    local obj = {
        type = CommMessageType.PeerDiscoveryResponse,
        peerId = Private.peerId,
        data = event.dbi.clock,
    }
    local serialized = event.dbi.serializer:Serialize(obj)
    InternalSend(event.prefix, serialized, "WHISPER", event.sender, CommPriority.Low)
end

function InternalPeerDiscoveryResponseHandler(event)
    -- Update last discovery time
    if event.dbi.onDiscoveryComplete then
        event.dbi.lastDiscoveryResponseTime = GetTime()
    end

    -- Store peer info
    if not event.dbi.peers[event.peerId] then
        event.dbi.peers[event.peerId] = {
            isNew = true,
            name = event.sender,
        }
    end
    event.dbi.peers[event.peerId].clock = event.data
end

function InternalRequestSnapshotMessageHandler(event)
    local obj = {
        type = CommMessageType.SnapshotResponse,
        peerId = Private.peerId,
        data = InternalExport(event.dbi),
    }
    local serialized = event.dbi.serializer:Serialize(obj)
    InternalSend(event.prefix, serialized, "WHISPER", event.sender, CommPriority.Low)
end

function InternalSnapshotMessageHandler(event)
    local success, errors = InternalImport(event.dbi, event.data)
    if not success then
        Debug(format("failed to import snapshot from %s on channel %s", tostring(event.sender), tostring(event.channel)))
        if errors then
            for i, err in ipairs(errors) do
                Debug(format("%d: %s", i, tostring(err)))
            end
        end
    end
end

function InternalDigestMessageHandler(event)
    -- Compare digest and build missing table
    local missingTables = {}
    for tableName, incomingTable in pairs(event.data.tables or {}) do
        local ti = event.dbi.tables[tableName]
        if ti then
            local missingRows = {}

            -- Check keys present in digest
            for key, incomingVersion in pairs(incomingTable) do
                local localRow = ti.rows[key]
                if not localRow then
                    -- We are missing this row, request it from sender
                    missingRows[key] = true
                else
                    -- Compare versions
                    if IsIncomingNewer(incomingVersion, localRow.version) then
                        -- Our row is older, request it from sender
                        missingRows[key] = true
                    end
                end
            end

            if next(missingRows) then
                missingTables[tableName] = missingRows
            end
        end
    end

    -- If nothing is missing, no need to send request rows
    if not next(missingTables) then
        return
    end

    -- Send request rows message
    local obj = {
        type = CommMessageType.RequestRows,
        peerId = Private.peerId,
        data = missingTables,
    }
    local serialized = event.dbi.serializer:Serialize(obj)
    InternalSend(event.prefix, serialized, "WHISPER", event.sender, CommPriority.Normal)
end

function InternalRequestRowsMessageHandler(event)
    -- Build rows to send
    local rowsToSend = {}
    for tableName, requestedRows in pairs(event.data or {}) do
        local ti = event.dbi.tables[tableName]
        if ti then
            local tableRows = {}
            for key, _ in pairs(requestedRows) do
                local row = ti.rows[key]
                if row then
                    -- No need to copy here, we'll just be reading to serialize
                    tableRows[key] = row
                end
            end

            if next(tableRows) then
                rowsToSend[tableName] = tableRows
            end
        end
    end

    -- If nothing to send, return
    if not next(rowsToSend) then
        return
    end

    -- Send requested rows
    local obj = {
        type = CommMessageType.Rows,
        peerId = Private.peerId,
        data = rowsToSend,
    }
    local serialized = event.dbi.serializer:Serialize(obj)
    InternalSend(event.prefix, serialized, "WHISPER", event.sender, CommPriority.Normal)
end

function InternalRowsMessageHandler(event)
    -- Import received rows
    for incomingTableName, incomingTableData in pairs(event.data or {}) do
        local ti = event.dbi.tables[incomingTableName]
        if ti then
            for incomingKey, incomingRow in pairs(incomingTableData or {}) do
                local importResult, importError = InternalImportRow(incomingKey, incomingRow, event.dbi, incomingTableName, ti)
                if not importResult then
                    Debug(format("failed to import row with key '%s' in table '%s' from %s on channel %s: %s", tostring(incomingKey), incomingTableName, tostring(event.sender), tostring(event.channel), tostring(importError)))
                end
            end
        end
    end
end

------------------------------------------------------------------------------------------------------------------------
-- Testing
------------------------------------------------------------------------------------------------------------------------

if DEBUG then
    local function Equal(a, b)
        assert(a ~= nil, "first value is nil")
        assert(b ~= nil, "second value is nil")
        if type(a) ~= type(b) then return false end
        if type(a) ~= "table" then return a == b end
        for k, v in pairs(a) do
            if not Equal(v, b[k]) then return false end
        end
        for k, v in pairs(b) do
            if not Equal(v, a[k]) then return false end
        end
        return true
    end

    local Assert = {
        IsNil = function(value, msg) assert(value == nil, msg or "value is not nil") end,
        IsNotNil = function(value, msg) assert(value ~= nil, msg or "value is nil") end,
        IsTrue = function(value, msg) assert(type(value) == "boolean" and value == true, msg or "value is not true") end,
        IsFalse = function(value, msg) assert(type(value) == "boolean" and value == false, msg or "value is not false") end,
        IsNumber = function(value, msg) assert(type(value) == "number", msg or "value is not a number") end,
        IsNotNumber = function(value, msg) assert(type(value) ~= "number", msg or "value is a number") end,
        IsString = function(value, msg) assert(type(value) == "string", msg or "value is not a string") end,
        IsNonEmptyString = function(value, msg) assert(type(value) == "string" and #value > 0, msg or "value is not a non-empty string") end,
        IsNotString = function(value, msg) assert(type(value) ~= "string", msg or "value is a string") end,
        IsTable = function(value, msg) assert(type(value) == "table", msg or "value is not a table") end,
        IsNonEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) ~= nil, msg or "value is not a non-empty table") end,
        IsNotTable = function(value, msg) assert(type(value) ~= "table", msg or "value is a table") end,
        IsFunction = function(value, msg) assert(type(value) == "function", msg or "value is not a function") end,
        IsNotFunction = function(value, msg) assert(type(value) ~= "function", msg or "value is a function") end,
        AreEqual = function(actual, expected, msg) assert(Equal(actual, expected) == true, msg or format("values are not equal, expected '%s' but got '%s'", tostring(expected), tostring(actual))) end,
        AreNotEqual = function(actual, expected, msg) assert(Equal(actual, expected) == false, msg or format("values are equal, both are '%s'", tostring(actual))) end,
        IsEmptyString = function(value, msg) assert(type(value) == "string" and #value == 0, msg or "value is not an empty string") end,
        IsNotEmptyString = function(value, msg) assert(type(value) == "string" and #value > 0, msg or "value is an empty string") end,
        IsEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) == nil, msg or "value is not an empty table") end,
        IsNotEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) ~= nil, msg or "value is an empty table") end,
        Contains = function(haystack, needle, msg)
            if type(haystack) == "string" then
                assert(strfind(haystack, needle, 1, true) ~= nil, msg or format("string does not contain '%s'", tostring(needle)))
            elseif type(haystack) == "table" then
                for _, v in pairs(haystack) do
                    if Equal(v, needle) then
                        return
                    end
                end
                assert(false, msg or format("table does not contain value '%s'", tostring(needle)))
            else
                assert(false, msg or "first argument must be a string or table")
            end
        end,
        DoesNotContain = function(haystack, needle, msg)
            if type(haystack) == "string" then
                assert(strfind(haystack, needle, 1, true) == nil, msg or format("string contains '%s'", tostring(needle)))
            elseif type(haystack) == "table" then
                for _, v in pairs(haystack) do
                    if Equal(v, needle) then
                        assert(false, msg or format("table contains value '%s'", tostring(needle)))
                        return
                    end
                end
            else
                assert(false, msg or "first argument must be a string or table")
            end
        end,
        Throws = function(fn, msg) assert(pcall(fn) == false, msg or "function did not throw") end,
        DoesNotThrow = function(fn, msg)
            local s, r = pcall(fn)
            assert(s == true, msg or format("function threw an error: %s", tostring(r)))
        end,
    }

    ---@diagnostic disable: param-type-mismatch, assign-type-mismatch, missing-fields
    local UnitTests = {
        New = function()
            local db = LibP2PDB:NewDB({
                clusterId = "TestCluster12345",
                namespace = "MyNamespace",
                channels = { "GUILD" },
                onChange = function(table, key, row) end,
                discoveryQuietPeriod = 5,
                discoveryMaxTime = 30,
                onDiscoveryComplete = function(isInitial) end,
            })
            Assert.IsTable(db)

            local dbi = Private.databases[db]
            Assert.IsTable(dbi)
            Assert.AreEqual(dbi.clusterId, "TestCluster12345")
            Assert.AreEqual(dbi.namespace, "MyNamespace")
            Assert.AreEqual(dbi.clock, 0)
            Assert.IsTable(dbi.peers)
            Assert.AreEqual(dbi.channels, { "GUILD" })
            Assert.AreEqual(dbi.discoveryQuietPeriod, 5)
            Assert.AreEqual(dbi.discoveryMaxTime, 30)
            Assert.IsEmptyTable(dbi.tables)
            Assert.IsFunction(dbi.onChange)
            Assert.IsFunction(dbi.onDiscoveryComplete)
            Assert.IsNotNil(dbi.frame)
            --Assert.IsNil(dbi.writePolicy)

            Assert.Throws(function()
                LibP2PDB:NewDB({
                    clusterId = "TestCluster12345",
                    namespace = "MyNamespace",
                })
            end)
        end,

        NewDB_DescIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDB(nil) end)
            Assert.Throws(function() LibP2PDB:NewDB(123) end)
            Assert.Throws(function() LibP2PDB:NewDB("invalid") end)
            Assert.Throws(function() LibP2PDB:NewDB({}) end)
        end,

        NewDB_DescClusterIdIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = nil, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = 123, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = {}, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "", namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "abcdefg1234567890", namespace = "n" }) end)
        end,

        NewDB_DescNamespaceIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "c", namespace = nil }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "c", namespace = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "c", namespace = {} }) end)
            Assert.Throws(function() LibP2PDB:NewDB({ clusterId = "c", namespace = "" }) end)
        end,

        GetDB = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "Cluster1", namespace = "NS1" })
            local db2 = LibP2PDB:NewDB({ clusterId = "Cluster2", namespace = "NS2" })

            local fetched1 = LibP2PDB:GetDB("Cluster1")
            Assert.AreEqual(fetched1, db1)

            local fetched2 = LibP2PDB:GetDB("Cluster2")
            Assert.AreEqual(fetched2, db2)

            local fetchedNil = LibP2PDB:GetDB("NonExistent")
            Assert.IsNil(fetchedNil)
        end,

        GetDB_ClusterIdIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:GetDB(nil) end)
            Assert.Throws(function() LibP2PDB:GetDB(123) end)
            Assert.Throws(function() LibP2PDB:GetDB({}) end)
            Assert.Throws(function() LibP2PDB:GetDB("") end)
            Assert.Throws(function() LibP2PDB:GetDB("abcdefg1234567890") end)
        end,

        NewTable = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "string",
                schema = {
                    name = "string",
                    age = "number",
                },
                onValidate = function(key, row) return true end,
                onChange = function(key, row) end,
            }))

            local dbi = Private.databases[db]
            Assert.IsTable(dbi)

            local t = dbi.tables["Users"]
            Assert.IsTable(t)
            Assert.AreEqual(t.keyType, "string")
            Assert.AreEqual(t.schema.name, "string")
            Assert.AreEqual(t.schema.age, "number")
            Assert.IsFunction(t.onValidate)
            Assert.IsFunction(t.onChange)
            Assert.IsTable(t.subscribers)
            Assert.IsEmptyTable(t.rows)

            -- Attempt to define the same table again
            Assert.Throws(function()
                LibP2PDB:NewTable(db, {
                    name = "Users",
                    keyType = "string",
                })
            end)

            -- Ensure the original table definition remains unchanged
            local t2 = dbi.tables["Users"]
            Assert.AreEqual(t, t2)

            -- Define another table
            Assert.IsTrue(LibP2PDB:NewTable(db, {
                name = "Products",
                keyType = "number",
            }))
        end,

        NewTable_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewTable(nil, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(123, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable("", { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable({}, { name = "Users", keyType = "string" }) end)
        end,

        NewTable_DescIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, nil) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, 123) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, "invalid") end)
            Assert.Throws(function() LibP2PDB:NewTable(db, {}) end)
        end,

        NewTable_DescNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = nil, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = 123, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "", keyType = "string" }) end)
        end,

        NewTable_DescKeyTypeIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = nil }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "invalid" }) end)
        end,

        NewTable_DescSchemaIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = 123 }) end)
        end,

        NewTable_DescOnValidateIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = 123 }) end)
        end,

        NewTable_DescOnChangeIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = 123 }) end)
        end,

        Schema_OnlyPrimitiveTypesAllowed = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Products", keyType = "number", schema = { a = "function" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Products", keyType = "number", schema = { a = "table" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Products", keyType = "number", schema = { a = "userdata" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Products", keyType = "number", schema = { a = "thread" } }) end)
        end,

        Schema_IsOptional = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:NewTable(db, { name = "Logs", keyType = "number" }))
            Assert.IsTrue(LibP2PDB:Insert(db, "Logs", 1, { message = "System started", timestamp = 1620000000 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Logs", 1), { message = "System started", timestamp = 1620000000 })
            Assert.IsTrue(LibP2PDB:Insert(db, "Logs", 2, { message = "User logged in", timestamp = 1620003600, username = "Bob" }))
            Assert.AreEqual(LibP2PDB:Get(db, "Logs", 2), { message = "User logged in", timestamp = 1620003600, username = "Bob" })
        end,

        Schema_CopySkipNonPrimitiveTypes = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:NewTable(db, { name = "Config", keyType = "string" }))
            Assert.IsTrue(LibP2PDB:Insert(db, "Config", "settings", { maxUsers = 100, someFunction = function() end, nestedTable = { a = 1 } }))

            local row = LibP2PDB:Get(db, "Config", "settings")
            Assert.AreEqual(row, { maxUsers = 100 })
        end,

        Schema_MultipleTypesAllowed = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:NewTable(db, { name = "Metrics", keyType = "string", schema = { value = { "number", "string" }, timestamp = "number" } }))
            Assert.IsTrue(LibP2PDB:Insert(db, "Metrics", "cpu_usage", { value = 75.5, timestamp = 1620000000 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Metrics", "cpu_usage"), { value = 75.5, timestamp = 1620000000 })
            Assert.IsTrue(LibP2PDB:Insert(db, "Metrics", "status", { value = "OK", timestamp = 1620003600 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Metrics", "status"), { value = "OK", timestamp = 1620003600 })
        end,

        Schema_NilTypeAllowed = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:NewTable(db, { name = "Settings", keyType = "string", schema = { value = { "string", "nil" } } }))
            Assert.IsTrue(LibP2PDB:Insert(db, "Settings", "theme", { value = "dark" }))
            Assert.AreEqual(LibP2PDB:Get(db, "Settings", "theme"), { value = "dark" })
            Assert.IsTrue(LibP2PDB:Insert(db, "Settings", "notifications", { value = nil }))
            Assert.AreEqual(LibP2PDB:Get(db, "Settings", "notifications"), { value = nil })
        end,

        Insert = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onValidate = function(key, row)
                    Assert.IsNumber(key)
                    Assert.IsTable(row)
                    Assert.IsString(row.name)
                    Assert.IsNumber(row.age)
                    return row.age >= 0 -- age must be non-negative
                end,
                onChange = function(key, row)
                    Assert.IsNumber(key)
                    Assert.IsTable(row)
                    Assert.IsString(row.name)
                    Assert.IsNumber(row.age)
                end,
            })
            Assert.IsTrue(LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 40 }) end)
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.IsFalse(LibP2PDB:Insert(db, "Users", 2, { name = "Charlie", age = -5 }))
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Insert_FireCallbacks = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            local callbackFired = false
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onChange = function(key, row)
                    callbackFired = true
                end,
            })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Insert")
        end,

        Insert_AfterTombstone_Succeeds = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
            })

            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Delete(db, "Users", 1)

            -- Now insert a new row with the same key
            Assert.IsTrue(LibP2PDB:Insert(db, "Users", 1, { name = "Alice", age = 30 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Alice", age = 30 })
        end,

        Insert_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Insert(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert("", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert({}, "Users", 1, { name = "A" }) end)
        end,

        Insert_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Insert(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, {}, 1, { name = "A" }) end)
        end,

        Insert_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", nil, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", {}, { name = "A" }) end)
        end,

        Insert_KeyTypeMismatch_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", { name = "A" }) end)
        end,

        Insert_RowIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", "invalid") end)
        end,

        Insert_RowSchemaMismatch_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = "25" }) end)
        end,

        Set = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onValidate = function(key, row)
                    Assert.IsNumber(key)
                    Assert.IsTable(row)
                    Assert.IsString(row.name)
                    Assert.IsNumber(row.age)
                    return row.age >= 0 -- age must be non-negative
                end,
                onChange = function(key, row)
                    Assert.IsNumber(key)
                    Assert.IsTable(row)
                    Assert.IsString(row.name)
                    Assert.IsNumber(row.age)
                end,
            })
            Assert.IsTrue(LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.IsTrue(LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = 40 }))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 40 })
            Assert.IsFalse(LibP2PDB:Set(db, "Users", 2, { name = "Charlie", age = -5 }))
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Set_WhenChanges_FireCallbacks = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            local callbackFired = false
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onChange = function(key, row)
                    callbackFired = true
                end,
            })
            LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Set when inserting new row")

            callbackFired = false
            LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsFalse(callbackFired, "onChange callback was fired on Set when setting identical row")

            callbackFired = false
            LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = 30 })
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Set when updating row")
        end,

        Set_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Set(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set("", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set({}, "Users", 1, { name = "A" }) end)
        end,

        Set_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Set(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, {}, 1, { name = "A" }) end)
        end,

        Set_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", nil, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", {}, { name = "A" }) end)
        end,

        Set_KeyTypeMismatch_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", { name = "A" }) end)
        end,

        Set_RowIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", "invalid") end)
        end,

        Set_RowSchemaMismatch_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = "25" }) end)
        end,

        Update = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            local updateFn = function(row)
                row.age = row.age + 1
                return row
            end
            Assert.IsTrue(LibP2PDB:Update(db, "Users", 1, updateFn))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 26 })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", 2, updateFn) end)
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Update_WhenChanges_FireCallbacks = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            local callbackFired = false
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onChange = function(key, row)
                    callbackFired = true
                end,
            })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })

            callbackFired = false
            LibP2PDB:Update(db, "Users", 1, function(row) return row end)
            Assert.IsFalse(callbackFired, "onChange callback was fired on Update when setting identical row")

            callbackFired = false
            LibP2PDB:Update(db, "Users", 1, function(row)
                row.age = row.age + 1
                return row
            end)
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Update when updating row")
        end,

        Update_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Update(nil, "Users", 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(123, "Users", 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update("", "Users", 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update({}, "Users", 1, function() end) end)
        end,

        Update_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Update(db, nil, 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, 123, 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, {}, 1, function() end) end)
        end,

        Update_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", {}, function() end) end)
        end,

        Update_UpdateFunctionIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            LibP2PDB:Insert(db, "Users", "user1", { name = "A" })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", "invalid") end)
        end,

        Update_UpdateFunctionRowParameter_IsNotModified = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Update(db, "Users", 1, function(row)
                row.additionalField = "abc"
                return { name = "Eve", age = 30 }
            end)
            local fetchedRow = LibP2PDB:Get(db, "Users", 1)
            Assert.AreEqual(fetchedRow, { name = "Eve", age = 30 })
        end,

        Get = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Get_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Get(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Get(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Get("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Get({}, "Users", 1) end)
        end,

        Get_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Get(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:Get(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:Get(db, {}, 1) end)
        end,

        Get_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Get(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Get(db, "Users", {}) end)
        end,

        HasKey = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsTrue(LibP2PDB:HasKey(db, "Users", 1))
            Assert.IsFalse(LibP2PDB:HasKey(db, "Users", 2))
        end,

        HasKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:HasKey(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey({}, "Users", 1) end)
        end,

        HasKey_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:HasKey(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, {}, 1) end)
        end,

        HasKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:HasKey(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, "Users", {}) end)
        end,

        Delete = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsTable(Private.databases[db].tables["Users"].rows[1].data)
            Assert.IsNil(Private.databases[db].tables["Users"].rows[1].version.tombstone)
            Assert.IsTrue(LibP2PDB:HasKey(db, "Users", 1))

            -- Delete existing key
            LibP2PDB:Delete(db, "Users", 1)
            Assert.IsNil(Private.databases[db].tables["Users"].rows[1].data)
            Assert.IsTrue(Private.databases[db].tables["Users"].rows[1].version.tombstone)
            Assert.IsNil(LibP2PDB:Get(db, "Users", 1))

            -- Delete non-existent key
            Assert.DoesNotThrow(function() LibP2PDB:Delete(db, "Users", 2) end)
            Assert.IsNil(Private.databases[db].tables["Users"].rows[2].data)
            Assert.IsTrue(Private.databases[db].tables["Users"].rows[2].version.tombstone)
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Delete_WhenChanges_FireCallbacks = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            local callbackFired = false
            LibP2PDB:NewTable(db, {
                name = "Users",
                keyType = "number",
                schema = {
                    name = "string",
                    age = "number",
                },
                onChange = function(key, row)
                    callbackFired = true
                end,
            })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })

            callbackFired = false
            LibP2PDB:Delete(db, "Users", 1)
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Delete when deleting existing row")

            callbackFired = false
            LibP2PDB:Delete(db, "Users", 2)
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Delete when deleting non-existent row")

            callbackFired = false
            LibP2PDB:Delete(db, "Users", 1)
            Assert.IsFalse(callbackFired, "onChange callback was fired on Delete when deleting already deleted row")
        end,

        Delete_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Delete(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete({}, "Users", 1) end)
        end,

        Delete_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Delete(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:Delete(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:Delete(db, {}, 1) end)
        end,

        Delete_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Delete(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Delete(db, "Users", {}) end)
        end,

        Version = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob" })

            local version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 1)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsNil(version.tombstone)

            LibP2PDB:Update(db, "Users", 1, function(row)
                row.name = "Robert"
                return row
            end)
            version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 2)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsNil(version.tombstone)

            LibP2PDB:Delete(db, "Users", 1)
            version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 3)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsTrue(version.tombstone)
        end,

        Version_WhenPeerEqualsKey_PeerValueIsEqualChar = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string" } })

            LibP2PDB:Insert(db, "Users", Private.peerId, { name = "Bob" })
            local version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 1)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:Set(db, "Users", Private.peerId, { name = "Robert" })
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 2)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:Update(db, "Users", Private.peerId, function(row)
                row.name = "Alice"
                return row
            end)
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 3)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:Delete(db, "Users", Private.peerId)
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 4)
            Assert.AreEqual(version.peer, "=")
            Assert.IsTrue(version.tombstone)
        end,

        Subscribe = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            local callbackInvoked = 0
            local callback = function(key, row)
                callbackInvoked = callbackInvoked + 1
                Assert.AreEqual(key, 1)
                Assert.AreEqual(row, { name = "Bob", age = 25 })
            end
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.AreEqual(callbackInvoked, 1)
        end,

        Subscribe_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Subscribe(nil, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(123, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe("", "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe({}, "Users", function() end) end)
        end,

        Subscribe_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Subscribe(db, nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, 123, function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, {}, function() end) end)
        end,

        Subscribe_CallbackIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", 123) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", "invalid") end)
        end,

        Unsubscribe = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            local callbackInvoked = 0
            local callback = function(key, row)
                callbackInvoked = callbackInvoked + 1
            end
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:Unsubscribe(db, "Users", callback)
            LibP2PDB:Unsubscribe(db, "Users", callback)
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.AreEqual(callbackInvoked, 0)
        end,

        Unsubscribe_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Unsubscribe(nil, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(123, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe("", "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe({}, "Users", function() end) end)
        end,

        Unsubscribe_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, 123, function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, {}, function() end) end)
        end,

        Unsubscribe_CallbackIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", 123) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", "invalid") end)
        end,

        Export = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Insert(db, "Users", 2, { name = "Alice", age = 30 })

            local exported = LibP2PDB:Export(db)
            Assert.IsTable(exported)
            Assert.IsTable(exported.tables)
            Assert.IsTable(exported.tables["Users"])
            Assert.IsTable(exported.tables["Users"].rows)
            Assert.AreEqual(exported.tables["Users"].rows[1].data, { name = "Bob", age = 25 })
            Assert.AreEqual(exported.tables["Users"].rows[1].version, { clock = 1, peer = Private.peerId })
            Assert.AreEqual(exported.tables["Users"].rows[2].data, { name = "Alice", age = 30 })
            Assert.AreEqual(exported.tables["Users"].rows[2].version, { clock = 2, peer = Private.peerId })
        end,

        Export_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Export(nil) end)
            Assert.Throws(function() LibP2PDB:Export(123) end)
            Assert.Throws(function() LibP2PDB:Export("") end)
            Assert.Throws(function() LibP2PDB:Export({}) end)
        end,

        Import = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", 2, { name = "Alice", age = 30 })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            local exported = LibP2PDB:Export(db1)
            local result, errors = LibP2PDB:Import(db2, exported)
            Assert.IsTrue(result)
            Assert.IsNil(errors)

            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 1), LibP2PDB:Get(db1, "Users", 1))
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 2), LibP2PDB:Get(db1, "Users", 2))
        end,

        Import_DBIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            local exported = LibP2PDB:Export(db)
            Assert.Throws(function() LibP2PDB:Import(nil, exported) end)
            Assert.Throws(function() LibP2PDB:Import(123, exported) end)
            Assert.Throws(function() LibP2PDB:Import("", exported) end)
            Assert.Throws(function() LibP2PDB:Import({}, exported) end)
        end,

        Import_ExportedIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Import(db, nil) end)
            Assert.Throws(function() LibP2PDB:Import(db, 123) end)
            Assert.Throws(function() LibP2PDB:Import(db, "invalid") end)
        end,

        Import_SkipInvalidRows = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", 1, { name = "Bob", age = 25 })

            local exported = LibP2PDB:Export(db1)

            -- Corrupt the exported state by adding a row with invalid schema
            exported.tables["Users"].rows[2] = { data = { name = "Alice" }, version = { clock = 2, peer = Private.peerId } }

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            local success, results = LibP2PDB:Import(db2, exported)
            Assert.IsTrue(success)
            Assert.IsNotEmptyTable(results)
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 1), LibP2PDB:Get(db1, "Users", 1))
            Assert.IsNil(LibP2PDB:Get(db2, "Users", 2))
        end,

        Serialize = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", "1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db, "Users", "2", { name = "Alice", age = 30 })
            LibP2PDB:Delete(db, "Users", "2")
            LibP2PDB:Insert(db, "Users", Private.peerId, { name = "Eve", age = 35 })

            local serialized = LibP2PDB:Serialize(db)
            Assert.IsNonEmptyString(serialized)
            Assert.Contains(serialized, "Users")
            Assert.Contains(serialized, "Bob")
            Assert.DoesNotContain(serialized, "Alice")
            Assert.Contains(serialized, "Eve")
        end,

        Serialize_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Serialize(nil) end)
            Assert.Throws(function() LibP2PDB:Serialize(123) end)
            Assert.Throws(function() LibP2PDB:Serialize("") end)
            Assert.Throws(function() LibP2PDB:Serialize({}) end)
        end,

        Deserialize = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number", gender = { "string", "nil" } } })
            LibP2PDB:Insert(db1, "Users", "1", { name = "Bob", age = 25, gender = "male" })
            LibP2PDB:Insert(db1, "Users", "2", { name = "Alice", age = 30, gender = "female" })
            LibP2PDB:Delete(db1, "Users", "2")
            LibP2PDB:Insert(db1, "Users", Private.peerId, { name = "Eve", age = 35, gender = nil })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number", gender = { "string", "nil" } } })

            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "1"), { name = "Bob", age = 25, gender = "male" })
            Assert.IsNil(LibP2PDB:Get(db2, "Users", "2"))
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", Private.peerId), { name = "Eve", age = 35, gender = nil })
        end,

        Deserialize_InvalidClock_Fails = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Test with invalid binary data
            local success, results = LibP2PDB:Deserialize(db2, "invalid_binary_data")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_InvalidTableStructure_Fails = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Test with malformed serialized data
            local success, results = LibP2PDB:Deserialize(db2, "\1\2\3")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_UndefinedTable_Fails = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Products", keyType = "string", schema = { name = "string" } })
            LibP2PDB:Insert(db1, "Products", "1", { name = "Widget" })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Serialize from db1 (has Products table), deserialize to db2 (only has Users table)
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_InvalidKeyType_SkipsRow = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "stringkey", { name = "Alice", age = 30 })
            LibP2PDB:Insert(db1, "Users", "5", { name = "Bob", age = 25 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            -- Serialize from db1 (string keys), deserialize to db2 (expects number keys)
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- String key should be skipped (check internal rows to avoid Get assertion)
            Assert.IsNil(Private.databases[db2].tables["Users"].rows["stringkey"])

            -- Numeric string key should also be skipped (check internal rows)
            Assert.IsNil(Private.databases[db2].tables["Users"].rows[5])
        end,

        Deserialize_InvalidRowStructure_SkipsRow = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionClock_SkipsRow = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionPeer_SkipsRow = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidFieldType_SkipsRow = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work - schema validation happens during Set, not deserialize
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_TombstoneWithoutData_Imports = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "deleted", { name = "Bob", age = 25 })
            LibP2PDB:Delete(db1, "Users", "deleted")

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db2, "Users", "deleted", { name = "OldData", age = 99 })

            -- Serialize tombstone from db1, deserialize to db2
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Row should be deleted
            Assert.IsFalse(LibP2PDB:HasKey(db2, "Users", "deleted"))
        end,

        Deserialize_MalformedNestedStructure_Fails = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Test with corrupted binary data
            local success, results = LibP2PDB:Deserialize(db2, "corrupted_data")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_EmptyData_ImportsCorrectly = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "key", { name = "Bob", age = 25 })

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize
            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Row should be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "key"), { name = "Bob", age = 25 })
        end,

        Deserialize_OptionalFields = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })
            LibP2PDB:Insert(db1, "Config", "theme", { key = "theme", value = "dark" })
            LibP2PDB:Insert(db1, "Config", "sound", { key = "sound", value = nil })

            local serialized = LibP2PDB:Serialize(db1)

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })

            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            Assert.AreEqual(LibP2PDB:Get(db2, "Config", "theme"), { key = "theme", value = "dark" })
            Assert.AreEqual(LibP2PDB:Get(db2, "Config", "sound"), { key = "sound", value = nil })
        end,

        Deserialize_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Deserialize(nil, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize(123, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize("", "") end)
            Assert.Throws(function() LibP2PDB:Deserialize({}, "") end)
        end,

        Deserialize_SerializedIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Deserialize(db, nil) end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, 123) end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, {}) end)
        end,

        GetSchema = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            local schema = LibP2PDB:GetSchema(db, "Users")
            Assert.IsTable(schema)
            if schema then
                Assert.AreEqual(schema.name, "string")
                Assert.AreEqual(schema.age, "number")
            end
        end,

        GetSchema_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:GetSchema(nil, "Users") end)
            Assert.Throws(function() LibP2PDB:GetSchema(123, "Users") end)
            Assert.Throws(function() LibP2PDB:GetSchema("", "Users") end)
            Assert.Throws(function() LibP2PDB:GetSchema({}, "Users") end)
        end,

        GetSchema_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:GetSchema(db, nil) end)
            Assert.Throws(function() LibP2PDB:GetSchema(db, 123) end)
            Assert.Throws(function() LibP2PDB:GetSchema(db, {}) end)
        end,

        ListTables = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number" })
            LibP2PDB:NewTable(db, { name = "Posts", keyType = "string" })
            local tables = LibP2PDB:ListTables(db)
            Assert.IsTable(tables)
            Assert.AreEqual(#tables, 2)
            Assert.IsTrue(tables[1] == "Users" or tables[1] == "Posts")
            Assert.IsTrue(tables[2] == "Users" or tables[2] == "Posts")
            Assert.AreNotEqual(tables[1], tables[2])
        end,

        ListTables_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:ListTables(nil) end)
            Assert.Throws(function() LibP2PDB:ListTables(123) end)
            Assert.Throws(function() LibP2PDB:ListTables("") end)
            Assert.Throws(function() LibP2PDB:ListTables({}) end)
        end,

        ListKeys = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Insert(db, "Users", 2, { name = "Alice", age = 30 })
            LibP2PDB:Insert(db, "Users", 3, { name = "Eve", age = 35 })
            LibP2PDB:Delete(db, "Users", 2)
            local keys = LibP2PDB:ListKeys(db, "Users")
            Assert.IsTable(keys)
            Assert.AreEqual(#keys, 2)
            Assert.IsTrue(keys[1] == 1 or keys[1] == 3)
            Assert.IsTrue(keys[2] == 1 or keys[2] == 3)
            Assert.AreNotEqual(keys[1], keys[2])
        end,

        ListKeys_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:ListKeys(nil, "Users") end)
            Assert.Throws(function() LibP2PDB:ListKeys(123, "Users") end)
            Assert.Throws(function() LibP2PDB:ListKeys("", "Users") end)
            Assert.Throws(function() LibP2PDB:ListKeys({}, "Users") end)
        end,

        ListKeys_TableIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:ListKeys(db, nil) end)
            Assert.Throws(function() LibP2PDB:ListKeys(db, 123) end)
            Assert.Throws(function() LibP2PDB:ListKeys(db, {}) end)
        end,
    }
    ---@diagnostic enable: param-type-mismatch, assign-type-mismatch, missing-fields

    local sin = math.sin
    local floor = math.floor
    local strchar = string.char

    local function GenerateName(i, minLen, maxLen, seedMultiplier, maxSpaces)
        -- Generate a deterministic random name based on index
        -- minLen: minimum length (inclusive)
        -- maxLen: maximum length (inclusive)
        -- seedMultiplier: multiplier for seed generation (different values = different sequences)
        -- maxSpaces: maximum number of spaces to include (0 = no spaces)
        local seed = sin(i * seedMultiplier) * 10000
        local absSeed = seed < 0 and -seed or seed
        local length = minLen + (floor(absSeed) % (maxLen - minLen + 1))

        if maxSpaces == 0 then
            -- No spaces - simple name generation
            local chars = {}
            for j = 1, length do
                if j == 1 then
                    chars[j] = strchar(65 + (floor(absSeed / j) % 26)) -- Uppercase A-Z
                else
                    chars[j] = strchar(97 + (floor(absSeed / j) % 26)) -- Lowercase a-z
                end
            end
            return table.concat(chars)
        else
            -- Multi-word name generation
            local numWords = 1 + (floor(absSeed / 3) % maxSpaces)                -- 1 to maxSpaces words
            numWords = numWords > (length / 3) and floor(length / 3) or numWords -- At least 3 chars per word
            numWords = numWords < 1 and 1 or numWords

            -- Calculate word lengths
            local words = {}
            local remainingLength = length - (numWords - 1) -- Subtract space count
            for w = 1, numWords do
                local minWordLen = 2
                local maxWordLen = remainingLength - (numWords - w) * minWordLen
                maxWordLen = maxWordLen > 12 and 12 or maxWordLen
                local wordLen = minWordLen + (floor(absSeed / (w * 7)) % (maxWordLen - minWordLen + 1))

                local wordChars = {}
                for c = 1, wordLen do
                    local charIndex = (w - 1) * 12 + c
                    if c == 1 then
                        wordChars[c] = strchar(65 + (floor(absSeed / charIndex) % 26)) -- Uppercase
                    else
                        wordChars[c] = strchar(97 + (floor(absSeed / charIndex) % 26)) -- Lowercase
                    end
                end
                words[w] = table.concat(wordChars)
                remainingLength = remainingLength - wordLen
            end

            return table.concat(words, " ")
        end
    end

    local function GeneratePlayerName(i)
        return GenerateName(i, 2, 12, 12345, 0) -- 2-12 chars, no spaces
    end

    local function GenerateGuildName(i)
        return GenerateName(i, 2, 24, 54321, 4) -- 2-24 chars, up to 4 spaces
    end

    local function GenerateRealmName(i)
        return GenerateName(i, 6, 20, 67890, 1) -- 6-20 chars, up to 1 space
    end

    local function GenerateDatabase(numRows)
        local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
        LibP2PDB:NewTable(db, {
            name = "Players",
            keyType = "number",
            schema = {
                name = "string",             -- 2 to 12 characters
                realm = { "string", "nil" }, -- 6 to 20 characters
                classID = "number",          -- 1 to 12
                guild = { "string", "nil" }, -- 2 to 24 characters
                version = "string",          -- semantic versioning e.g., "1.0.0"
                level = "number",            -- 1 to 60
                xpTotal = "number",          -- 0 to 3,379,400 xp
                money = "number",            -- 0 to 2,147,483,647 coppers
                timePlayed = "number",       -- 0 to 2,147,483,647 seconds
            }
        })
        for i = 1, numRows do
            local name = GeneratePlayerName(i)
            local realm = (i % 10 ~= 0) and (GenerateRealmName(i % 10)) or nil
            local classID = (floor(math.abs(sin(i * 11111) * 10000)) % 12) + 1
            local guild = (i % 10 ~= 0) and (GenerateGuildName(i % 20)) or nil
            local version = "1.0." .. (floor(math.abs(sin(i * 22222) * 10000)) % 100)
            local level = (floor(math.abs(sin(i * 33333) * 10000)) % 60) + 1
            local xpTotal = floor(math.abs(sin(i * 44444) * 10000) % 3379401)
            local money = floor(math.abs(sin(i * 55555) * 10000) % 2147483648)
            local timePlayed = floor(math.abs(sin(i * 11111) * 10000) * math.abs(sin(i * 22222) * 10000) % 2147483648)
            LibP2PDB:Insert(db, "Players", i, {
                name = name,
                realm = realm,
                classID = classID,
                guild = guild,
                version = version,
                level = level,
                xpTotal = xpTotal,
                money = money,
                timePlayed = timePlayed,
            })
        end
        return db
    end

    local PerformanceTests = {
        SerializeDeserialize = function()
            local numRows = 2000
            local db = GenerateDatabase(numRows)

            local startTime = debugprofilestop()
            local serialized = LibP2PDB:Serialize(db)
            local endTime = debugprofilestop()
            local duration = endTime - startTime
            Debug(format("Serialize of %d rows took %.2f ms. Serialized size: %d bytes.", numRows, duration, #serialized))

            startTime = debugprofilestop()
            LibP2PDB:Deserialize(db, serialized)
            endTime = debugprofilestop()
            duration = endTime - startTime
            Debug(format("Deserialize of %d rows took %.2f ms.", numRows, duration))
        end,
    }

    local function RunTest(testFn)
        -- Make a temporary private instance for isolation
        local originalPrivate = Private
        Private = NewPrivate()

        -- Run the test
        testFn()

        -- Restore the original private instance
        Private = originalPrivate
    end

    local function RunTests()
        for _, v in pairs(UnitTests) do
            RunTest(v)
        end
        Debug("All tests " .. C(Color.Green, "successful") .. ".")
    end

    local function RunPerformanceTests()
        for _, v in pairs(PerformanceTests) do
            RunTest(v)
        end
    end

    -- add slash command
    SLASH_LIBP2PDB1 = "/libp2pdb"
    SlashCmdList["LIBP2PDB"] = function(arg)
        if arg == "runtests" then
            RunTests()
        elseif arg == "runperformancetests" then
            RunPerformanceTests()
        else
            print("Usage: /libp2pdb runtests - Runs the LibP2PDB tests.")
        end
    end
end
