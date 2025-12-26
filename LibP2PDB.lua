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
local tostring, tostringall = tostring, tostringall
local format, strsub, strfind, strjoin = format, strsub, strfind, strjoin
local unpack, tinsert, CopyTable = unpack, table.insert, CopyTable

------------------------------------------------------------------------------------------------------------------------
-- Local WoW API References
------------------------------------------------------------------------------------------------------------------------

local UnitGUID = UnitGUID

------------------------------------------------------------------------------------------------------------------------
-- Constants
------------------------------------------------------------------------------------------------------------------------

local Color = {
    Ace = "ff33ff99",
    Debug = "ff00ffff",
    White = "ffffffff",
    Yellow = "ffffff00",
    Red = "ffff4040",
    Green = "ff00ff00"
}

local CommMessageType = {
    RequestSnapshot = 1,
    Snapshot = 2,
    Digest = 3,
    RequestRows = 4,
    Rows = 5,
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

local enableDebugging = false
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

local function ternary(c, a, b)
    if c then return a else return b end
end

local function shallow_equal(a, b)
    if a == b then return true end
    if type(a) ~= "table" or type(b) ~= "table" then return false end
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

local function IsNotNil(v)
    return v ~= nil
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

local function NewPrivate()
    local private = {}
    private.clusters = {}
    private.databases = setmetatable({}, { __mode = "k" })
    private.peerId = PlayerGUIDShort()
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
---@field onChange function|nil Callback function(table, key, row) invoked on any row change

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
    assert(desc.onChange == nil or IsFunction(desc.onChange), "desc.onChange must be a function if provided")

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
        channels = desc.channels or defaultChannels,
        -- Data
        tables = {},
        -- Callbacks
        onChange = desc.onChange,
        -- Access control
        writePolicy = nil,
    }

    -- Internal registry
    local db = {}
    Private.clusters[desc.clusterId] = db
    Private.databases[db] = dbi

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

---@class LibP2PDB.TableDesc Description for defining a table in the database
---@field name string Name of the table to define
---@field keyType string Data type of the primary key ("string" or "number")
---@field schema table<string, string|table<string>>|nil Optional table schema defining field names and their allowed data types
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
---@return boolean result Returns true on success, false otherwise
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
---@param updateFn function Function(currentRow) -> updatedRow
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
    local updatedRow = updateFn(CopyTable(existingRow.data))
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
    return ternary(row ~= nil and not row.version.tombstone, true, false)
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

    -- Lookup the existing row
    local changed = false
    local existingRow = t.rows[key]
    if existingRow and existingRow.data and existingRow.version and not existingRow.version.tombstone then
        changed = true
    end

    -- Versioning (Lamport clock)
    dbi.clock = dbi.clock + 1

    -- Replace row with tombstone
    t.rows[key] = {
        data = nil, -- no row data
        version = {
            clock = dbi.clock,
            peer = Private.peerId,
            tombstone = true, -- mark as deleted
        },
    }

    -- Fire callbacks
    if changed then
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

---@class LibP2PDB.VersionState Exported version metadata
---@field clock number Lamport clock value
---@field peer string Peer ID that last modified the row
---@field tombstone boolean|nil Optional tombstone flag indicating deletion

---@class LibP2PDB.RowState Exported row state
---@field data table Row data
---@field version LibP2PDB.VersionState Version metadata

---@class LibP2PDB.TableState Exported table state
---@field rows table<LibP2PDB.RowState> Registry of rows in the exported table

---@class LibP2PDB.DBState Exported database state
---@field clock number Lamport clock of the exported database
---@field tables table<LibP2PDB.TableState> Registry of tables and their rows

---Export the entire DB state as a serializable table.
---@param db LibP2PDB.DB Database instance
---@return LibP2PDB.DBState state The exported database state
function LibP2PDB:Export(db)
    assert(IsTable(db), "db must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Build export state
    local state = {
        clock = dbi.clock,
        tables = {},
    }
    for tableName, tableData in pairs(dbi.tables) do
        state.tables[tableName] = {
            rows = {},
        }
        for key, row in pairs(tableData.rows) do
            state.tables[tableName].rows[key] = {
                data = CopyTable(row.data),
                version = CopyTable(row.version),
            }
        end
    end
    return state
end

---Import the DB state from an exported table.
---Merges the imported state with existing data based on version metadata.
---Validates incoming data against table definitions, skipping invalid entries.
---@param db LibP2PDB.DB Database instance
---@param state LibP2PDB.DBState The exported database state to import
---@return boolean,table<string>|nil result Returns true on success, false otherwise. On failure, a table of error messages is returned as the second value.
function LibP2PDB:Import(db, state)
    assert(IsTable(db), "db must be a table")
    assert(IsTable(state), "exportedDB must be a table")
    assert(IsNumber(state.clock), "invalid exportedDB clock")
    assert(IsTable(state.tables), "invalid exportedDB tables")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Begin import
    Private.isImporting = true

    -- Merge Lamport clock
    dbi.clock = max(dbi.clock, state.clock)

    -- Import DB state
    local result, errors = true, nil
    for incomingTableName, incomingTableData in pairs(state.tables or {}) do
        local t = dbi.tables[incomingTableName]
        if t then
            for incomingKey, incomingRow in pairs(incomingTableData.rows or {}) do
                local importResult, importError = InternalImportRow(incomingKey, incomingRow, dbi, incomingTableName, t)
                if not importResult then
                    result = false
                    errors = errors or {}
                    tinsert(errors, "error processing row with key '" .. tostring(incomingKey) .. "' in table '" .. incomingTableName .. "': " .. tostring(importError))
                end
            end
        end
    end

    -- End import
    Private.isImporting = false

    return result, errors
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Sync / Gossip Controls
------------------------------------------------------------------------------------------------------------------------

---Request a full snapshot from a specific peer
---@param db LibP2PDB.DB Database instance
---@param target string|nil Optional target peer ID to request the snapshot from; if nil, broadcasts to all peers
function LibP2PDB:RequestSnapshot(db, target)
    assert(IsTable(db), "db must be a table")
    assert(target == nil or IsNonEmptyString(target), "target must be a non-empty string if provided")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Send the request message
    local obj = {
        type = CommMessageType.RequestSnapshot,
    }
    local serialized = AceSerializer:Serialize(obj)
    InternalSend(dbi.clusterId, serialized, "WHISPER", target, CommPriority.Normal)
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
        data = digest,
    }
    local serialized = AceSerializer:Serialize(obj)
    InternalSendToAllPeers(dbi.clusterId, serialized, dbi.channels, CommPriority.Normal)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Utility / Metadata
------------------------------------------------------------------------------------------------------------------------

-- Return the local peer's unique ID
function LibP2PDB:GetPeerId(db)
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

function InternalSchemaCopy(table, schema, data)
    local result = {}
    if not schema then
        -- No schema: shallow copy only primitives
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
    if t.onValidate and not t.onValidate(key, data) then
        return false
    end

    -- Determine if the row will change
    local changes = false
    local existingRow = t.rows[key]
    if not existingRow or (not existingRow.data and existingRow.version.tombstone == true) then
        changes = true -- new row
    else
        -- Check for data changes
        for fieldName, fieldValue in pairs(data) do
            if existingRow.data[fieldName] ~= fieldValue then
                changes = true
                break
            end
        end
        if not changes then
            for fieldName, fieldValue in pairs(existingRow.data) do
                if data[fieldName] ~= fieldValue then
                    changes = true
                    break
                end
            end
        end
    end

    if changes then
        -- Versioning (Lamport clock)
        dbi.clock = dbi.clock + 1

        -- Store the row
        t.rows[key] = {
            data = data,
            version = {
                clock = dbi.clock,
                peer = Private.peerId,
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
        dbi.onChange(tableName, key, data)
    end

    -- Fire table change callback
    if t.onChange then
        t.onChange(key, data)
    end

    -- Fire subscribers
    for callback in pairs(t.subscribers) do
        callback(key, data)
    end
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
    local incomingVersion = incomingRow.version
    if not IsTable(incomingVersion) or not IsNumber(incomingVersion.clock) or not IsNonEmptyString(incomingVersion.peer) then
        return false, format("skipping row with invalid version metadata for key '%s' in table '%s'", tostring(incomingKey), incomingTableName)
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
    if t.onValidate and not t.onValidate(incomingKey, cleanData) then
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

function InternalSendToAllPeers(prefix, message, channels, priority)
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
    local success, deserialized = AceSerializer:Deserialize(message)
    if not success then
        Debug(format("failed to deserialize message from %s on channel %s: %s", tostring(sender), tostring(channel), Dump(deserialized)))
        return
    end

    if not IsTable(deserialized) or not IsNumber(deserialized.type) then
        Debug(format("received invalid message structure from %s on channel %s", tostring(sender), tostring(channel)))
        return
    end

    if deserialized.type == CommMessageType.RequestSnapshot then
        InternalRequestSnapshotMessageHandler(prefix, channel, sender)
    elseif deserialized.type == CommMessageType.Snapshot then
        InternalSnapshotMessageHandler(prefix, channel, sender, deserialized.data)
    elseif deserialized.type == CommMessageType.Digest then
        InternalDigestMessageHandler(prefix, channel, sender, deserialized.data)
    elseif deserialized.type == CommMessageType.RequestRows then
        InternalRequestRowsMessageHandler(prefix, channel, sender, deserialized.data)
    elseif deserialized.type == CommMessageType.Rows then
        InternalRowsMessageHandler(prefix, channel, sender, deserialized.data)
    else
        Debug(format("received unknown message type %d from %s on channel %s", deserialized.type, tostring(sender), tostring(channel)))
    end
end

function InternalRequestSnapshotMessageHandler(prefix, channel, sender)
    -- Get the DB instance
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received request snapshot message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local obj = {
        type = CommMessageType.Snapshot,
        data = LibP2PDB:Export(db),
    }
    local serialized = AceSerializer:Serialize(obj)
    InternalSend(prefix, serialized, "WHISPER", sender, CommPriority.Low)
end

function InternalSnapshotMessageHandler(prefix, channel, sender, data)
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received snapshot message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local success, errors = LibP2PDB:Import(db, data)
    if not success then
        Debug(format("failed to import snapshot from %s on channel %s", tostring(sender), tostring(channel)))
        if errors then
            for i, err in ipairs(errors) do
                Debug(format("%d: %s", i, tostring(err)))
            end
        end
    else
        Debug(format("successfully imported snapshot from %s on channel %s", tostring(sender), tostring(channel)))
    end
end

function InternalDigestMessageHandler(prefix, channel, sender, data)
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received digest message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Compare digest and build missing table
    local missingTables = {}
    for tableName, incomingTable in pairs(data.tables or {}) do
        local t = dbi.tables[tableName]
        if t then
            local missingRows = {}

            -- Check keys present in digest
            for key, incomingVersion in pairs(incomingTable) do
                local localRow = t.rows[key]
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
        data = missingTables,
    }
    local serialized = AceSerializer:Serialize(obj)
    InternalSend(prefix, serialized, "WHISPER", sender, CommPriority.Normal)
end

function InternalRequestRowsMessageHandler(prefix, channel, sender, data)
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received request rows message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Build rows to send
    local rowsToSend = {}
    for tableName, requestedRows in pairs(data or {}) do
        local t = dbi.tables[tableName]
        if t then
            local tableRows = {}
            for key, _ in pairs(requestedRows) do
                local row = t.rows[key]
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
        data = rowsToSend,
    }
    local serialized = AceSerializer:Serialize(obj)
    InternalSend(prefix, serialized, "WHISPER", sender, CommPriority.Normal)
end

function InternalRowsMessageHandler(prefix, channel, sender, data)
    local db = LibP2PDB:GetDB(prefix)
    if not db then
        Debug(format("received rows message for unknown clusterId '%s' from %s on channel %s", tostring(prefix), tostring(sender), tostring(channel)))
        return
    end

    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database instance")

    -- Import received rows
    local anyErrors = false
    for incomingTableName, incomingTableData in pairs(data or {}) do
        local t = dbi.tables[incomingTableName]
        if t then
            for incomingKey, incomingRow in pairs(incomingTableData or {}) do
                local importResult, importError = InternalImportRow(incomingKey, incomingRow, dbi, incomingTableName, t)
                if not importResult then
                    anyErrors = true
                    Debug(format("failed to import row with key '%s' in table '%s' from %s on channel %s: %s", tostring(incomingKey), incomingTableName, tostring(sender), tostring(channel), tostring(importError)))
                end
            end
        end
    end

    if not anyErrors then
        Debug(format("successfully imported rows from %s on channel %s", tostring(sender), tostring(channel)))
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
        IsNil = function(value, msg) assert(value == nil, ternary(msg, msg, "value is not nil")) end,
        IsNotNil = function(value, msg) assert(value ~= nil, ternary(msg, msg, "value is nil")) end,
        IsTrue = function(value, msg) assert(type(value) == "boolean" and value == true, ternary(msg, msg, "value is not true")) end,
        IsFalse = function(value, msg) assert(type(value) == "boolean" and value == false, ternary(msg, msg, "value is not false")) end,
        IsNumber = function(value, msg) assert(type(value) == "number", ternary(msg, msg, "value is not a number")) end,
        IsNotNumber = function(value, msg) assert(type(value) ~= "number", ternary(msg, msg, "value is a number")) end,
        IsString = function(value, msg) assert(type(value) == "string", ternary(msg, msg, "value is not a string")) end,
        IsNotString = function(value, msg) assert(type(value) ~= "string", ternary(msg, msg, "value is a string")) end,
        IsTable = function(value, msg) assert(type(value) == "table", ternary(msg, msg, "value is not a table")) end,
        IsNotTable = function(value, msg) assert(type(value) ~= "table", ternary(msg, msg, "value is a table")) end,
        IsFunction = function(value, msg) assert(type(value) == "function", ternary(msg, msg, "value is not a function")) end,
        IsNotFunction = function(value, msg) assert(type(value) ~= "function", ternary(msg, msg, "value is a function")) end,
        AreEqual = function(actual, expected, msg) assert(Equal(actual, expected) == true, ternary(msg, msg, "values are not equal")) end,
        AreNotEqual = function(actual, expected, msg) assert(Equal(actual, expected) == false, ternary(msg, msg, "values are equal")) end,
        IsEmptyString = function(value, msg) assert(type(value) == "string" and #value == 0, ternary(msg, msg, "value is not an empty string")) end,
        IsNotEmptyString = function(value, msg) assert(type(value) == "string" and #value > 0, ternary(msg, msg, "value is an empty string")) end,
        IsEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) == nil, ternary(msg, msg, "value is not an empty table")) end,
        IsNotEmptyTable = function(value, msg) assert(type(value) == "table" and next(value) ~= nil, ternary(msg, msg, "value is an empty table")) end,
        Throws = function(fn, msg) assert(pcall(fn) == false, ternary(msg, msg, "function did not throw")) end,
        DoesNotThrow = function(fn, msg)
            local s, r = pcall(fn)
            assert(s == true, ternary(msg, msg, format("function threw an error: %s", tostring(r))))
        end,
    }

    local LibP2PDBTests = {
        New = function()
            local db = LibP2PDB:NewDB({
                clusterId = "TestCluster12345",
                namespace = "MyNamespace",
                channels = { "GUILD" },
                onChange = function(table, key, row) end,
            })
            Assert.IsTable(db)

            local dbi = Private.databases[db]
            Assert.IsTable(dbi)
            Assert.AreEqual(dbi.clusterId, "TestCluster12345")
            Assert.AreEqual(dbi.namespace, "MyNamespace")
            Assert.AreEqual(dbi.clock, 0)
            Assert.AreEqual(dbi.channels, { "GUILD" })
            Assert.IsEmptyTable(dbi.tables)
            Assert.IsFunction(dbi.onChange)
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
            Assert.Throws(function() LibP2PDB:NewTable("invalid", { name = "Users", keyType = "string" }) end)
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
            Assert.Throws(function() LibP2PDB:Insert("invalid", "Users", 1, { name = "A" }) end)
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
            Assert.Throws(function() LibP2PDB:Set("invalid", "Users", 1, { name = "A" }) end)
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
            Assert.Throws(function() LibP2PDB:Update("invalid", "Users", 1, function() end) end)
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

        Update_UpdateFunctionRowIsACopy = function()
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
            Assert.Throws(function() LibP2PDB:Get("invalid", "Users", 1) end)
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
            Assert.Throws(function() LibP2PDB:HasKey("invalid", "Users", 1) end)
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
            LibP2PDB:Delete(db, "Users", 2)
            Assert.IsFalse(callbackFired, "onChange callback was fired on Delete when deleting non-existent row")

            callbackFired = false
            LibP2PDB:Delete(db, "Users", 1)
            Assert.IsTrue(callbackFired, "onChange callback was not fired on Delete when deleting existing row")

            callbackFired = false
            LibP2PDB:Delete(db, "Users", 1)
            Assert.IsFalse(callbackFired, "onChange callback was fired on Delete when deleting already deleted row")
        end,

        Delete_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Delete(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete("invalid", "Users", 1) end)
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
            Assert.Throws(function() LibP2PDB:Subscribe("invalid", "Users", function() end) end)
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
            Assert.Throws(function() LibP2PDB:Unsubscribe("invalid", "Users", function() end) end)
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

            local state = LibP2PDB:Export(db)
            Assert.IsTable(state)
            Assert.IsTable(state.tables)
            Assert.IsTable(state.tables["Users"])
            Assert.IsTable(state.tables["Users"].rows)
            Assert.AreEqual(state.tables["Users"].rows[1].data, { name = "Bob", age = 25 })
            Assert.AreEqual(state.tables["Users"].rows[1].version, { clock = 1, peer = Private.peerId })
            Assert.AreEqual(state.tables["Users"].rows[2].data, { name = "Alice", age = 30 })
            Assert.AreEqual(state.tables["Users"].rows[2].version, { clock = 2, peer = Private.peerId })
        end,

        Export_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Export(nil) end)
            Assert.Throws(function() LibP2PDB:Export(123) end)
            Assert.Throws(function() LibP2PDB:Export("invalid") end)
        end,

        Import = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:Insert(db1, "Users", 2, { name = "Alice", age = 30 })

            local state = LibP2PDB:Export(db1)
            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            local result, errors = LibP2PDB:Import(db2, state)
            Assert.IsTrue(result)
            Assert.IsNil(errors)

            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 1), LibP2PDB:Get(db1, "Users", 1))
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 2), LibP2PDB:Get(db1, "Users", 2))
        end,

        Import_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Import(nil, {}) end)
            Assert.Throws(function() LibP2PDB:Import(123, {}) end)
            Assert.Throws(function() LibP2PDB:Import("invalid", {}) end)
        end,

        Import_StateIsInvalid_Throws = function()
            local db = LibP2PDB:NewDB({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Import(db, nil) end)
            Assert.Throws(function() LibP2PDB:Import(db, 123) end)
            Assert.Throws(function() LibP2PDB:Import(db, "invalid") end)
        end,

        Import_SkipInvalidRows = function()
            local db1 = LibP2PDB:NewDB({ clusterId = "c1", namespace = "n1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db1, "Users", 1, { name = "Bob", age = 25 })

            local state = LibP2PDB:Export(db1)

            -- Corrupt the exported state by adding a row with invalid schema
            state.tables["Users"].rows[2] = { data = { name = "Alice" }, version = { clock = 2, peer = Private.peerId } }

            local db2 = LibP2PDB:NewDB({ clusterId = "c2", namespace = "n2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            local result, errors = LibP2PDB:Import(db2, state)
            Assert.IsFalse(result)
            Assert.IsTable(errors)
            Assert.IsNotEmptyTable(errors)

            -- Valid row should still be imported
            Assert.AreEqual(LibP2PDB:Get(db2, "Users", 1), LibP2PDB:Get(db1, "Users", 1))
            Assert.IsNil(LibP2PDB:Get(db2, "Users", 2))
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
        for _, v in pairs(LibP2PDBTests) do
            RunTest(v)
        end
        Debug("All tests " .. C(Color.Green, "successful") .. ".")
    end

    _G.LibP2PDB = { RunTests = RunTests }
end
