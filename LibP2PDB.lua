------------------------------------------------------------------------------------------------------------------------
-- LibP2PDB: A lightweight, embeddable library for peer-to-peer distributed-database synchronization in WoW addons.
------------------------------------------------------------------------------------------------------------------------

local MAJOR, MINOR = "LibP2PDB", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibP2PDB = LibStub:NewLibrary(MAJOR, MINOR)
if not LibP2PDB then return end -- no upgrade needed

local AceComm = LibStub("AceComm-3.0")
assert(AceComm, MAJOR .. " requires AceComm-3.0")

local AceSerializer = LibStub("AceSerializer-3.0")
assert(AceSerializer, MAJOR .. " requires AceSerializer-3.0")

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

local enableDebugging = false

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
    if enableDebugging then
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

---@class LibP2PDB.DBDesc Description for creating a new database instance
---@field clusterId string Unique identifier for the database cluster (max 16 chars)
---@field namespace string Namespace for this database instance
---@field channels table|nil List of chat channels to use for gossip (default: {"GUILD", "RAID", "PARTY", "YELL"})
---@field discoveryQuietPeriod number|nil Seconds of quiet time with no new peers before considering discovery complete (default: 1.0)
---@field discoveryMaxTime number|nil Maximum seconds to wait for peer discovery before considering it complete (default: 3.0)
---@field onChange function|nil Callback function(table, key, row) invoked on any row change
---@field onDiscoveryComplete function|nil Callback function(isInitial) invoked when peers discovery completes

---@class LibP2PDB.DB Database instance

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
    assert(desc.discoveryQuietPeriod == nil or IsNumber(desc.discoveryQuietPeriod), "desc.discoveryQuietPeriod must be a number if provided")
    assert(desc.discoveryMaxTime == nil or IsNumber(desc.discoveryMaxTime), "desc.discoveryMaxTime must be a number if provided")
    assert(desc.onChange == nil or IsFunction(desc.onChange), "desc.onChange must be a function if provided")
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
        -- Networking
        peers = {},
        buckets = {},
        channels = desc.channels or defaultChannels,
        discoveryQuietPeriod = desc.discoveryQuietPeriod or 1.0,
        discoveryMaxTime = desc.discoveryMaxTime or 3.0,
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
---@return LibP2PDB.DB|nil db The database instance if found, or nil if not found
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
---@field schema LibP2PDB.Schema|nil Optional table schema defining field names and their allowed data types
---@field onValidate function|nil Optional validation function(key, row) -> true/false
---@field onChange function|nil Optional callback function(key, row) on row changes

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
---@return table|nil row The row data if found, or nil if not found
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
---@field tombstone boolean|nil Optional tombstone flag indicating deletion

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
---@return boolean,nil|table<string> result Returns true if the import is succesful, false otherwise. On failure, the second return value is a table of warnings/errors.
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
---@return boolean,nil|table<string> result Returns true if the deserialization is succesful, false otherwise. On failure, the second return value is a table of warnings/errors.
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
    local serialized = AceSerializer:Serialize(obj)
    InternalBroadcast(dbi.clusterId, serialized, dbi.channels, CommPriority.Low)

    -- Record the time of the peer discovery request
    if dbi.onDiscoveryComplete then
        dbi.discoveryStartTime = GetTime()
        dbi.lastDiscoveryResponseTime = dbi.discoveryStartTime
    end
end

---Request a full snapshot from a peer or broadcast to all peers.
---@param db LibP2PDB.DB Database instance
---@param target string|nil Optional target peer to request the snapshot from; if nil, broadcasts to all peers
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
    local serialized = AceSerializer:Serialize(obj)
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
    local serialized = AceSerializer:Serialize(obj)
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
---@return string|nil peerId The remote peer ID if valid, or nil if not
function LibP2PDB:GetPeerIdFromGUID(guid)
    assert(IsNonEmptyString(guid), "guid must be a non-empty string")
    if strsub(guid, 1, 7) ~= "Player-" then
        return nil
    end
    return strsub(guid, 8) -- skip "Player-" prefix
end

---Retrieve the schema definition for a specific table.
---@param db LibP2PDB.DB Database instance
---@param tableName string Name of the table to get the schema for
---@param sorted boolean|nil Optional flag to return the schema with sorted field names (default: false)
---@return LibP2PDB.Schema|LibP2PDB.SortedSchema|nil schema The table schema, or nil if no schema is defined. If sorted is true, returns an array of {fieldName, fieldType} pairs sorted by fieldName.
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

-- List all defined tables
function LibP2PDB:ListTables(db)
end

-- List all keys in a table
function LibP2PDB:ListKeys(db, table)
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
    local parts = {}

    -- Start cluster
    tinsert(parts, "{")

    -- Add clock (in hex)
    tinsert(parts, tostring(NumberToHex(dbi.clock)))

    -- Add each table
    for tableName, t in pairs(dbi.tables) do
        -- Only include table if it has rows
        if next(t.rows) then
            -- Separator
            tinsert(parts, ";")

            -- Add table name
            tinsert(parts, tostring(tableName))

            -- Start table
            tinsert(parts, "{")

            -- Add each row
            local first = true
            for key, row in pairs(t.rows) do
                -- Separator
                if not first then
                    tinsert(parts, ";")
                end
                first = false

                -- Add key (convert to hex if number)
                local keyStr
                if type(key) == "number" then
                    keyStr = tostring(NumberToHex(key))
                else
                    keyStr = tostring(key)
                end
                tinsert(parts, keyStr)

                -- Start row
                tinsert(parts, "{")

                -- Add data fields
                tinsert(parts, "{")
                if row.data then
                    if t.schema then
                        local sortedSchema = InternalGetSchema(t, true)
                        for _, fieldInfo in ipairs(sortedSchema or {}) do
                            local fieldName = fieldInfo[1]
                            local fieldValue = row.data[fieldName]
                            -- Always add separator except for first field
                            if #parts > 0 and parts[#parts] ~= "{" then
                                tinsert(parts, ";")
                            end
                            -- Always serialize all fields, use \0 marker for nil
                            if fieldValue == nil then
                                -- \0 represents nil (unlikely to appear in normal strings)
                                tinsert(parts, "\0")
                            elseif type(fieldValue) == "number" then
                                tinsert(parts, tostring(NumberToHex(fieldValue)))
                            elseif type(fieldValue) == "boolean" then
                                tinsert(parts, fieldValue and "1" or "0")
                            elseif type(fieldValue) == "string" then
                                tinsert(parts, fieldValue)
                            else
                                assert(false, "unsupported field value type: " .. type(fieldValue))
                            end
                        end
                    else
                        -- not supported
                        error("serialization without schema is not supported")
                    end
                end
                tinsert(parts, "}")

                -- Add version
                tinsert(parts, tostring(NumberToHex(row.version.clock)))
                tinsert(parts, ";")
                tinsert(parts, tostring(row.version.peer))
                if row.version.tombstone then
                    tinsert(parts, ";1")
                end

                -- End row
                tinsert(parts, "}")
            end

            -- End table
            tinsert(parts, "}")
        end
    end

    -- End cluster
    tinsert(parts, "}")

    return tconcat(parts)
end

function InternalDeserialize(dbi, str)
    -- First, parse the string into a nested array structure
    local rootArray = nil
    local currentArray = nil
    local arrayStack = {}
    local currentToken = nil
    local len = #str
    for i = 1, len do
        local b = strbyte(str, i)
        local c = strchar(b)
        if c == "{" then -- start new array
            if currentToken ~= nil then
                if currentArray then
                    tinsert(currentArray, currentToken)
                end
                currentToken = nil
            end
            local newArray = {}
            if currentArray then
                tinsert(currentArray, newArray)
            else
                rootArray = newArray
            end
            tinsert(arrayStack, newArray)
            currentArray = newArray
        elseif c == "}" then -- end current array
            if #arrayStack == 0 then
                return nil, { "malformed structure: extra closing brace" }
            end
            if currentToken ~= nil then
                if currentArray then
                    tinsert(currentArray, currentToken)
                end
                currentToken = nil
            end
            tremove(arrayStack, #arrayStack)
            currentArray = arrayStack[#arrayStack]
        elseif c == ";" then -- token separator
            if currentToken ~= nil then
                if currentArray then
                    tinsert(currentArray, currentToken)
                end
                currentToken = nil
            end
        else -- part of a token
            if currentToken == nil then
                currentToken = c
            else
                currentToken = currentToken .. c
            end
        end
    end

    -- Validate that all braces were properly matched
    if #arrayStack > 0 then
        return nil, { "malformed structure: unclosed braces" }
    end

    if not rootArray then
        return nil, { "malformed structure: no root array found" }
    end

    -- Second, process the parsed structure into a state that can be imported later
    local state = {}
    local cluster = rootArray
    if not cluster or not IsNonEmptyTable(cluster) then
        return nil, { "invalid database structure: missing cluster data" }
    end

    -- Parse clock
    local clockHex = tostring(cluster[1])
    local clock = HexToNumber("0x" .. clockHex)
    if not IsNumber(clock) then
        return nil, { format("invalid database structure: invalid cluster clock: '%s'", tostring(clockHex)) }
    end
    state.clock = clock

    -- Parse tables (format is flat: clock;tableName{data};tableName{data};...)
    local results = {}
    state.tables = {}
    local i = 2
    while i <= #cluster do
        local tableName = tostring(cluster[i])
        local tableData = cluster[i + 1]

        if not IsTable(tableData) then
            -- Invalid table structure is a catastrophic failure
            return nil, { format("invalid table structure for table '%s'", tostring(tableName)) }
        end

        local ti = dbi.tables[tableName]
        if not ti then
            -- Undefined table is a catastrophic failure
            return nil, { format("table '%s' is not defined in the database", tostring(tableName)) }
        end

        local tableExport = {
            rows = {},
        }

        -- Get sorted schema once for this table
        local sortedSchema = InternalGetSchema(ti, true)

        -- Parse rows (format is flat: key{data};key{data};...)
        local rowIndex = 1
        while rowIndex <= #tableData do
            local keyToken = tableData[rowIndex]
            local rowEntry = tableData[rowIndex + 1]

            local shouldProcessRow = true

            if not IsTable(rowEntry) or #rowEntry < 2 then
                -- Skip this row and continue
                tinsert(results, format("skipping row with invalid structure in table '%s'", tostring(tableName)))
                shouldProcessRow = false
            end

            if shouldProcessRow then
                local key
                if ti.keyType == "number" then
                    key = HexToNumber("0x" .. tostring(keyToken))
                    if not IsNumber(key) then
                        -- Invalid hex number for key
                        tinsert(results, format("skipping row with invalid key in table '%s'", tostring(tableName)))
                        shouldProcessRow = false
                    end
                else
                    key = tostring(keyToken)
                end

                if shouldProcessRow then
                    local dataEntry = rowEntry[1]
                    local data = {}
                    if IsTable(dataEntry) and #dataEntry > 0 then
                        -- Map array indices back to field names using sorted schema
                        if sortedSchema then
                            for fieldIndex = 1, #dataEntry do
                                local fieldToken = dataEntry[fieldIndex]
                                local fieldValue

                                -- Get schema info for this field
                                local fieldInfo = sortedSchema[fieldIndex]
                                if fieldInfo then
                                    local fieldName = fieldInfo[1]
                                    local allowedTypes = fieldInfo[2]

                                    -- Normalize allowed types to a table
                                    local typeList = {}
                                    if type(allowedTypes) == "string" then
                                        typeList[allowedTypes] = true
                                    elseif type(allowedTypes) == "table" then
                                        for _, t in ipairs(allowedTypes) do
                                            typeList[t] = true
                                        end
                                    end

                                    -- Parse based on schema and value
                                    if typeList["nil"] and tostring(fieldToken) == "\0" then
                                        -- \0 marker represents nil
                                        fieldValue = nil
                                    elseif typeList["number"] and strmatch(tostring(fieldToken), "^[0-9a-fA-F]+$") then
                                        -- Parse as number if schema allows and looks like hex
                                        fieldValue = HexToNumber("0x" .. tostring(fieldToken))
                                    elseif typeList["boolean"] and (tostring(fieldToken) == "1" or tostring(fieldToken) == "0") then
                                        -- Parse as boolean if schema allows and is 0 or 1
                                        fieldValue = (tostring(fieldToken) == "1")
                                    elseif typeList["string"] then
                                        -- Parse as string if schema allows
                                        fieldValue = tostring(fieldToken)
                                    end

                                    data[fieldName] = fieldValue
                                end
                            end
                        end
                    end

                    local versionClockHex = tostring(rowEntry[2])
                    local versionClock = HexToNumber("0x" .. versionClockHex)
                    if not IsNumber(versionClock) then
                        -- Skip this row and continue
                        tinsert(results, format("skipping row with invalid version clock in table '%s'", tostring(tableName)))
                        shouldProcessRow = false
                    end

                    if shouldProcessRow then
                        local versionPeerToken = rowEntry[3]
                        if not versionPeerToken or not IsNonEmptyString(tostring(versionPeerToken)) then
                            -- Skip this row and continue
                            tinsert(results, format("skipping row with invalid version peer in table '%s'", tostring(tableName)))
                            shouldProcessRow = false
                        end

                        if shouldProcessRow then
                            local versionPeer
                            if tostring(versionPeerToken) == "=" then
                                versionPeer = tostring(key)
                            else
                                versionPeer = tostring(versionPeerToken)
                            end

                            local versionTombstone = false
                            if #rowEntry >= 4 then
                                local tombstoneToken = tostring(rowEntry[4])
                                if tombstoneToken == "1" then
                                    versionTombstone = true
                                end
                            end
                            local version = {
                                clock = versionClock,
                                peer = versionPeer,
                            }
                            if versionTombstone then
                                version.tombstone = true
                            end
                            tableExport.rows[key] = {
                                data = versionTombstone and nil or data,
                                version = version,
                            }
                        end
                    end
                end
            end

            rowIndex = rowIndex + 2
        end

        state.tables[tableName] = tableExport

        -- Next table
        i = i + 2
    end

    -- Return results only if there are any
    if next(results) == nil then
        results = nil
    end
    return state, results
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

    -- Deserialize message
    local success, deserialized = AceSerializer:Deserialize(message)
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
    local serialized = AceSerializer:Serialize(obj)
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
    local serialized = AceSerializer:Serialize(obj)
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
    local serialized = AceSerializer:Serialize(obj)
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
    local serialized = AceSerializer:Serialize(obj)
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

if enableDebugging then
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
        IsNotString = function(value, msg) assert(type(value) ~= "string", msg or "value is a string") end,
        IsTable = function(value, msg) assert(type(value) == "table", msg or "value is not a table") end,
        IsNotTable = function(value, msg) assert(type(value) ~= "table", msg or "value is a table") end,
        IsFunction = function(value, msg) assert(type(value) == "function", msg or "value is not a function") end,
        IsNotFunction = function(value, msg) assert(type(value) ~= "function", msg or "value is a function") end,
        AreEqual = function(actual, expected, msg) assert(Equal(actual, expected) == true, msg or format("values are not equal, expected '%s' but got '%s'", tostring(expected), tostring(actual))) end,
        AreNotEqual = function(actual, expected, msg) assert(Equal(actual, expected) == false, msg or format("values are equal, both are '%s'", tostring(actual))) end,
        IsEmptyString = function(value, msg) assert(type(value) == "string" and #value == 0, msg or "value is not an empty string") end,
        IsNotEmptyString = function(value, msg) assert(type(value) == "string" and #value > 0, msg or "value is an empty string") end,
        IsEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) == nil, msg or "value is not an empty table") end,
        IsNotEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) ~= nil, msg or "value is an empty table") end,
        Throws = function(fn, msg) assert(pcall(fn) == false, msg or "function did not throw") end,
        DoesNotThrow = function(fn, msg)
            local s, r = pcall(fn)
            assert(s == true, msg or format("function threw an error: %s", tostring(r)))
        end,
    }

    ---@diagnostic disable: param-type-mismatch, assign-type-mismatch, missing-fields
    local LibP2PDBTests = {
        New = function()
            local db = LibP2PDB:NewDB({
                clusterId = "TestCluster12345",
                namespace = "MyNamespace",
                channels = { "GUILD" },
                discoveryQuietPeriod = 5,
                discoveryMaxTime = 30,
                onChange = function(table, key, row) end,
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
            local firstRow = "1{{19;Bob}1;" .. Private.peerId .. "}"
            local secondRow = "2{{}3;" .. Private.peerId .. ";1}"
            local thirdRow = Private.peerId .. "{{23;Eve}4;=}"

            -- Rows may be in any order, so we check all possibilities
            local expected1 = "{4;Users{" .. firstRow .. ";" .. secondRow .. ";" .. thirdRow .. "}}"
            local expected2 = "{4;Users{" .. firstRow .. ";" .. thirdRow .. ";" .. secondRow .. "}}"
            local expected3 = "{4;Users{" .. secondRow .. ";" .. firstRow .. ";" .. thirdRow .. "}}"
            local expected4 = "{4;Users{" .. secondRow .. ";" .. thirdRow .. ";" .. firstRow .. "}}"
            local expected5 = "{4;Users{" .. thirdRow .. ";" .. firstRow .. ";" .. secondRow .. "}}"
            local expected6 = "{4;Users{" .. thirdRow .. ";" .. secondRow .. ";" .. firstRow .. "}}"
            Assert.IsTrue(serialized == expected1 or serialized == expected2 or serialized == expected3 or
                serialized == expected4 or serialized == expected5 or serialized == expected6)
        end,

        Serialize_EmptyStringVsNil = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })
            LibP2PDB:Insert(db, "Config", "empty", { key = "empty", value = "" })
            LibP2PDB:Insert(db, "Config", "null", { key = "null", value = nil })

            local serialized = LibP2PDB:Serialize(db)
            Assert.IsString(serialized)

            -- Verify empty string is present (not \0)
            Assert.IsTrue(serialized:find("empty;") ~= nil, "should contain 'empty;' for empty string")

            -- Verify \0 is present for nil
            Assert.IsTrue(serialized:find("\0") ~= nil, "should contain \\0 for nil value")
        end,

        Serialize_OptionalFields = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })
            LibP2PDB:Insert(db, "Config", "theme", { key = "theme", value = "dark" })
            LibP2PDB:Insert(db, "Config", "sound", { key = "sound", value = nil })

            local serialized = LibP2PDB:Serialize(db)
            Assert.IsString(serialized)
            -- Verify \0 is present in serialized string for nil value
            Assert.IsTrue(serialized:find("\0") ~= nil, "serialized string should contain \\0 for nil value")
        end,

        Serialize_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Serialize(nil) end)
            Assert.Throws(function() LibP2PDB:Serialize(123) end)
            Assert.Throws(function() LibP2PDB:Serialize("") end)
            Assert.Throws(function() LibP2PDB:Serialize({}) end)
        end,

        Deserialize = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", "1", { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", "2", { name = "Alice", age = 30 })
            LibP2PDB:Delete(db1, "Users", "2")
            LibP2PDB:Insert(db1, "Users", Private.peerId, { name = "Eve", age = 35 })
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            local serialized = LibP2PDB:Serialize(db1)
            local success, results = LibP2PDB:Deserialize(db2, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", "1"), { name = "Bob", age = 25 })
            Assert.IsNil(LibP2PDB:Get(db2, "Users", "2"))
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", Private.peerId), { name = "Eve", age = 35 })
        end,

        Deserialize_InvalidClock_Fails = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Missing clock
            local success, results = LibP2PDB:Deserialize(db, "{}")
            Assert.IsFalse(success)
            Assert.IsTable(results)

            -- Invalid clock (not hex)
            success, results = LibP2PDB:Deserialize(db, "{xyz}")
            Assert.IsFalse(success)
            Assert.IsTable(results)

            -- Negative clock
            local success, results = LibP2PDB:Deserialize(db, "{-1}")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_InvalidTableStructure_Fails = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Table data is not a table (missing braces)
            local success, results = LibP2PDB:Deserialize(db, "{5;Users;invalid}")
            Assert.IsFalse(success)
            Assert.IsTable(results)

            -- Table name without data
            local success, results = LibP2PDB:Deserialize(db, "{5;Users}")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_UndefinedTable_Fails = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Table "Products" doesn't exist
            local success, results = LibP2PDB:Deserialize(db, "{5;Products{{1}1;peer}}")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_InvalidKeyType_SkipsRow = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            -- Key should be number but is string (xyz contains non-hex chars)
            local serialized = "{3;Users{xyz{{1e;Alice}1;peer};5{{1e;Alice}2;peer}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- First row with string key should be skipped
            Assert.IsNil(Private.databases[db].tables["Users"].rows["xyz"])

            -- Second row with number key should be imported
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 5), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidRowStructure_SkipsRow = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Row missing version data (needs at least data + clock + peer)
            local serialized = "{2;Users{key1{{19;Bob}};key2{{1e;Alice}5;peer}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- First row should be skipped
            Assert.IsNil(LibP2PDB:Get(db, "Users", "key1"))

            -- Second row should be imported
            Assert.AreEqual(LibP2PDB:Get(db, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionClock_SkipsRow = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Invalid version clock (not hex)
            local serialized = "{3;Users{key1{{19;Bob}xyz;peer};key2{{1e;Alice}5;peer}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- First row should be skipped
            Assert.IsNil(LibP2PDB:Get(db, "Users", "key1"))

            -- Second row should be imported
            Assert.AreEqual(LibP2PDB:Get(db, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionPeer_SkipsRow = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Empty peer
            local serialized = "{3;Users{key1{{19;Bob}5;};key2{{1e;Alice}5;peer-456}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- First row should be skipped
            Assert.IsNil(LibP2PDB:Get(db, "Users", "key1"))

            -- Second row should be imported
            Assert.AreEqual(LibP2PDB:Get(db, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidFieldType_SkipsRow = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Age field should be number but age is first in alphabetical order so: {age;name}
            -- Providing string for age (first field)
            local serialized = "{3;Users{key1{{notanumber;Bob}5;peer};key2{{1e;Alice}5;peer}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- First row should be skipped (age is not a number)
            Assert.IsNil(LibP2PDB:Get(db, "Users", "key1"))

            -- Second row should be imported
            Assert.AreEqual(LibP2PDB:Get(db, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_TombstoneWithoutData_Imports = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Insert then receive tombstone
            LibP2PDB:Insert(db, "Users", "deleted", { name = "Bob", age = 25 })
            local serialized = "{5;Users{deleted{{}3;peer;1}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsNil(results)

            -- Row should be deleted
            Assert.IsFalse(LibP2PDB:HasKey(db, "Users", "deleted"))
        end,

        Deserialize_MalformedNestedStructure_Fails = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Unmatched braces
            local success, results = LibP2PDB:Deserialize(db, "{5;Users{key{{data}1;peer}")
            Assert.IsFalse(success)
            Assert.IsTable(results)

            -- Extra closing brace
            success, results = LibP2PDB:Deserialize(db, "{5;Users{key{{data}1;peer}}}}}")
            Assert.IsFalse(success)
            Assert.IsTable(results)
        end,

        Deserialize_EmptyData_ImportsCorrectly = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Row with empty data array (all nil fields)
            local serialized = "{2;Users{key{{;}5;peer}}}"
            local success, results = LibP2PDB:Deserialize(db, serialized)
            Assert.IsTrue(success)
            Assert.IsTable(results)

            -- Row should be skipped
            Assert.IsNil(Private.databases[db].tables["Users"].rows["key"])
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
    }
    ---@diagnostic enable: param-type-mismatch, assign-type-mismatch, missing-fields

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
        for _, v in pairs(LibP2PDBTests) do
            RunTest(v)
        end
        Debug("All tests " .. C(Color.Green, "successful") .. ".")
    end

    -- add slash command
    SLASH_LIBP2PDB1 = "/libp2pdb"
    SlashCmdList["LIBP2PDB"] = function(arg)
        if arg == "runtests" then
            RunTests()
        else
            print("Usage: /libp2pdb runtests - Runs the LibP2PDB tests.")
        end
    end
end
