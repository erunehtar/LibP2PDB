-- LibP2PDB: A lightweight, embeddable library for peer-to-peer distributed-database synchronization in WoW addons.

local MAJOR, MINOR = "LibP2PDB", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibP2PDB = LibStub:NewLibrary(MAJOR, MINOR)
if not LibP2PDB then return end            -- no upgrade needed

local ChatThrottleLib = _G.ChatThrottleLib -- ChatThrottleLib does not use LibStub
assert(ChatThrottleLib, MAJOR .. " requires ChatThrottleLib")

------------------------------------------------------------------------------------------------------------------------
-- Local Lua References
------------------------------------------------------------------------------------------------------------------------

local assert, print = assert, print
local type, ipairs, pairs = type, ipairs, pairs
local format, strsub, strfind = format, strsub, strfind

------------------------------------------------------------------------------------------------------------------------
-- Local WoW API References
------------------------------------------------------------------------------------------------------------------------

local UnitGUID = UnitGUID

------------------------------------------------------------------------------------------------------------------------
-- Private Helper Functions
------------------------------------------------------------------------------------------------------------------------

local function dump(o)
    if type(o) == "table" then
        local s = "{"
        for k, v in pairs(o) do
            if type(k) ~= "number" then k = '"' .. k .. '"' end
            s = s .. '[' .. k .. '] = ' .. dump(v) .. ','
        end
        return s .. '}'
    elseif type(o) == "string" then
        return '"' .. o .. '"'
    else
        return tostring(o)
    end
end

local function IsNotNil(v)
    return v ~= nil
end

local function IsString(s, maxlen)
    return type(s) == "string" and (not maxlen or #s <= maxlen)
end

local function IsNonEmptyString(s, maxlen)
    return type(s) == "string" and #s > 0 and (not maxlen or #s <= maxlen)
end

local function IsNonEmptyStringOrNumber(v)
    local t = type(v)
    return (t == "string" and #v > 0) or (t == "number" and v == v) -- v == v checks for NaN
end

local function IsTable(t)
    return type(t) == "table"
end

local function IsNonEmptyTable(t)
    return type(t) == "table" and next(t)
end

local function IsFunction(f)
    return type(f) == "function"
end

local function PlayerGUIDShort()
    local guid = UnitGUID("player")
    if not guid then return nil end
    return strsub(guid, 8) -- skip "Player-" prefix
end

------------------------------------------------------------------------------------------------------------------------
-- Private Variables
------------------------------------------------------------------------------------------------------------------------

local function NewPrivate()
    local private = {}
    private.clusters = setmetatable({}, { __mode = "k" })
    private.databases = setmetatable({}, { __mode = "k" })
    private.peerId = PlayerGUIDShort()
    return private
end
local Private = NewPrivate()

------------------------------------------------------------------------------------------------------------------------
-- Public API: Database Instance Creation
------------------------------------------------------------------------------------------------------------------------

---@class LibP2PDB.DBDesc Description for creating a new database instance
---@field clusterId string Unique identifier for the database cluster (max 16 chars)
---@field namespace string Namespace for this database instance
---@field channels table|nil List of chat channels to use for gossip (default: {"GUILD", "PARTY", "RAID", "YELL"})
---@field onChange function|nil Callback function(table, key, row) invoked on any row change

---@class LibP2PDB.DB Database instance
---@field clusterId string Unique identifier for the database cluster (max 16 chars)
---@field namespace string Namespace for this database instance
---@field clock number Lamport clock for versioning
---@field channels table List of chat channels used for gossip (default: {"GUILD", "PARTY", "RAID", "YELL"})
---@field tables table Registry of defined tables in the database
---@field onChange function|nil Callback function(table, key, row) invoked on any row change
---@field writePolicy function|nil Access control function(table, key, row) -> true/false

---Create a new database instance for peer-to-peer synchronization.
---If a database with the same clusterId already exists, returns the existing instance.
---Each database is identified by a unique clusterId and operates independently.
---@param desc LibP2PDB.DBDesc Description of the database instance to create
---@return LibP2PDB.DB db The database instance
function LibP2PDB:New(desc)
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.clusterId, 16), "desc.clusterId must be a non-empty string (max 16 chars)")
    assert(IsNonEmptyString(desc.namespace), "desc.namespace must be a non-empty string")
    assert(desc.channels == nil or IsNonEmptyTable(desc.channels), "desc.channels must be a non-empty table if provided")
    assert(desc.onChange == nil or IsFunction(desc.onChange), "desc.onChange must be a function if provided")

    -- Validate channels if provided
    if desc.channels then
        for _, channel in ipairs(desc.channels) do
            assert(IsNonEmptyString(channel), "each channel in desc.channels must be a non-empty string")
        end
    end

    -- If a database with the same clusterId exists, return it
    if Private.clusters[desc.clusterId] then
        return Private.clusters[desc.clusterId]
    end

    -- Default channels if none provided
    local defaultChannels = { "GUILD", "PARTY", "RAID", "YELL" }

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
    Private.clusters[desc.clusterId] = dbi
    Private.databases[db] = dbi
    return db
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Table Definition (Schema)
------------------------------------------------------------------------------------------------------------------------

---@class LibP2PDB.TableDesc Description for defining a table in the database
---@field name string Name of the table to define
---@field keyType string Data type of the primary key ("string" or "number")
---@field schema table<string, string>|nil Optional table schema defining field names and their data types
---@field onValidate function|nil Optional validation function(key, row) -> true/false
---@field onChange function|nil Optional callback function(key, row) on row changes

---Define a typed table inside the database.
---If no schema is provided, the table accepts any fields.
---If a table with the same name already exists, this is a no-op.
---@param db LibP2PDB.DB Database instance
---@param desc LibP2PDB.TableDesc Description of the table to define
---@return boolean result Returns true if the table was created, false if it already existed
function LibP2PDB:DefineTable(db, desc)
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.name), "desc.name must be a non-empty string")
    assert(IsNonEmptyString(desc.keyType), "desc.keyType must be a non-empty string")
    assert(desc.keyType == "string" or desc.keyType == "number", "desc.keyType must be 'string' or 'number'")
    assert(desc.schema == nil or IsTable(desc.schema), "desc.schema must be a table if provided")
    for fieldName, fieldType in pairs(desc.schema or {}) do
        assert(IsNonEmptyString(fieldName))
        assert(IsNonEmptyString(fieldType))
    end
    assert(desc.onValidate == nil or IsFunction(desc.onValidate))
    assert(desc.onChange == nil or IsFunction(desc.onChange))

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- If a table with the same name already exists, don't do anything
    if dbi.tables[desc.name] then
        return false
    end

    -- Create the table entry
    dbi.tables[desc.name] = {
        keyType = desc.keyType,
        schema = desc.schema,
        onValidate = desc.onValidate,
        onChange = desc.onChange,
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
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsNonEmptyTable(row), "row must be a non-empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- Validate table, key type, and row schema
    local t = dbi.tables[table]
    assert(IsTable(t), "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")
    assert(IsTable(row), "row must be a table")

    -- Ensure the key does not already exist
    assert(t.rows[key] == nil, "key '" .. tostring(key) .. "' already exists in table '" .. table .. "'")

    -- If a schema is defined, clean the row to only include schema-defined fields
    local cleanRow
    if not t.schema then
        cleanRow = row -- no schema, accept all fields
    else
        cleanRow = {}
        for fieldName, fieldType in pairs(t.schema) do
            local fieldValue = row[fieldName]
            assert(IsNotNil(fieldValue), "missing required field '" .. fieldName .. "' in table '" .. table .. "'")
            assert(type(fieldValue) == fieldType, "expected field '" .. fieldName .. "' of type '" .. fieldType .. "' in table '" .. table .. "', but was '" .. type(fieldValue) .. "'")
            cleanRow[fieldName] = fieldValue
        end
    end

    -- Run custom validation if provided
    if t.onValidate and not t.onValidate(key, cleanRow) then
        return false
    end

    -- Versioning (Lamport clock)
    dbi.clock = dbi.clock + 1

    -- Store the row
    t.rows[key] = {
        data = cleanRow,
        version = {
            clock = dbi.clock,
            peer = Private.peerId,
        },
    }

    -- Fire table change callback
    if t.onChange then
        t.onChange(key, cleanRow)
    end

    -- Fire db change callback
    if dbi.onChange then
        dbi.onChange(table, key, cleanRow)
    end

    return true
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
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsNonEmptyTable(row), "row must be a non-empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- Validate table, key type, and row schema
    local t = dbi.tables[table]
    assert(IsTable(t), "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")
    assert(IsTable(row), "row must be a table")

    -- If a schema is defined, clean the row to only include schema-defined fields
    local cleanRow
    if not t.schema then
        cleanRow = row -- no schema, accept all fields
    else
        cleanRow = {}
        for fieldName, fieldType in pairs(t.schema) do
            local fieldValue = row[fieldName]
            assert(IsNotNil(fieldValue), "missing required field '" .. fieldName .. "' in table '" .. table .. "'")
            assert(type(fieldValue) == fieldType, "expected field '" .. fieldName .. "' of type '" .. fieldType .. "' in table '" .. table .. "', but was '" .. type(fieldValue) .. "'")
            cleanRow[fieldName] = fieldValue
        end
    end

    -- Run custom validation if provided
    if t.onValidate and not t.onValidate(key, cleanRow) then
        return false
    end

    -- Versioning (Lamport clock)
    dbi.clock = dbi.clock + 1

    -- Store the row
    t.rows[key] = {
        data = cleanRow,
        version = {
            clock = dbi.clock,
            peer = Private.peerId,
            tombstone = nil,
        },
    }

    -- Fire table change callback
    if t.onChange then
        t.onChange(key, cleanRow)
    end

    -- Fire db change callback
    if dbi.onChange then
        dbi.onChange(table, key, cleanRow)
    end

    return true
end

---Update an existing row.
---Validates the key type against the table definition.
---The update function is called with the current row data and must return the updated row data.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to update
---@param key string|number Primary key value for the row (must match table's keyType)
---@param updateFn function Function(currentRow) -> updatedRow
function LibP2PDB:Update(db, table, key, updateFn)
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")
    assert(IsFunction(updateFn), "updateFn must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(IsTable(t), "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Lookup the existing row
    local existingRow = t.rows[key]
    assert(existingRow, "key '" .. tostring(key) .. "' does not exist in table '" .. table .. "'")

    -- Call the update function to get the new row data
    local updatedRow = updateFn(existingRow.data)
    assert(IsNonEmptyTable(updatedRow), "updateFn must return a non-empty table")

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
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(IsTable(t), "table '" .. table .. "' is not defined in the database")
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

---Delete a row.
---Validates the key type against the table definition.
---Marks the row as a tombstone for gossip synchronization.
---@param db LibP2PDB.DB Database instance
---@param table string Name of the table to delete from
---@param key string|number Primary key value for the row (must match table's keyType)
---@return boolean result Returns true if the row was deleted, false if it did not exist
function LibP2PDB:Delete(db, table, key)
    assert(IsTable(db), "db must be a valid database instance")
    assert(IsNonEmptyString(table), "table name must be a non-empty string")
    assert(IsNonEmptyStringOrNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(IsNonEmptyTable(dbi), "db is not a recognized database instance")

    -- Validate table and key type
    local t = dbi.tables[table]
    assert(IsTable(t), "table '" .. table .. "' is not defined in the database")
    assert(type(key) == t.keyType, "expected key of type '" .. t.keyType .. "' for table '" .. table .. "', but was '" .. type(key) .. "'")

    -- Lookup the existing row
    if t.rows[key] == nil then
        return false -- nothing to delete
    end

    -- Versioning (Lamport clock)
    dbi.clock = dbi.clock + 1

    -- Replace row with tombstone
    t.rows[key] = {
        data = nil, -- no row data
        version = {
            clock = dbi.clock,
            peer = Private.peerId,
            tombstone = true, -- mark deletion for gossip
        },
    }

    -- Fire table change callback
    if t.onChange then
        t.onChange(key, nil)
    end

    -- Fire db change callback
    if dbi.onChange then
        dbi.onChange(table, key, nil)
    end

    return true
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Subscriptions
------------------------------------------------------------------------------------------------------------------------

-- Subscribe to changes in a table
function LibP2PDB:Subscribe(db, table, callback)
end

-- Unsubscribe from table changes
function LibP2PDB:Unsubscribe(db, table, callback)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Persistence (SavedVariables)
------------------------------------------------------------------------------------------------------------------------

-- Export the entire DB state as a serializable table
function LibP2PDB:Export(db)
end

-- Load DB state from SavedVariables
function LibP2PDB:Import(db, savedTable)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Sync / Gossip Controls
------------------------------------------------------------------------------------------------------------------------

-- Request a full snapshot from a specific peer
function LibP2PDB:RequestSnapshot(db, target)
end

-- Force a digest broadcast
function LibP2PDB:ForceDigest(db)
end

-- Force a full sync cycle
function LibP2PDB:ForceSync(db)
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
-- Testing
------------------------------------------------------------------------------------------------------------------------

local testing = false -- set to true to enable tests
if testing then
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
        IsNil = function(value) assert(value == nil, "value is not nil") end,
        IsNotNil = function(value) assert(value ~= nil, "value is nil") end,
        IsTrue = function(value) assert(type(value) == "boolean" and value == true, "value is not true") end,
        IsFalse = function(value) assert(type(value) == "boolean" and value == false, "value is not false") end,
        IsNumber = function(value) assert(type(value) == "number", "value is not a number") end,
        IsNotNumber = function(value) assert(type(value) ~= "number", "value is a number") end,
        IsString = function(value) assert(type(value) == "string", "value is not a string") end,
        IsNotString = function(value) assert(type(value) ~= "string", "value is a string") end,
        IsTable = function(value) assert(type(value) == "table", "value is not a table") end,
        IsNotTable = function(value) assert(type(value) ~= "table", "value is a table") end,
        IsFunction = function(value) assert(type(value) == "function", "value is not a function") end,
        IsNotFunction = function(value) assert(type(value) ~= "function", "value is a function") end,
        AreEqual = function(actual, expected) assert(Equal(actual, expected) == true, "values are not equal") end,
        AreNotEqual = function(actual, expected) assert(Equal(actual, expected) == false, "values are equal") end,
        IsEmptyString = function(value) assert(type(value) == "string" and #value == 0, "value is not an empty string") end,
        IsNotEmptyString = function(value) assert(type(value) == "string" and #value > 0, "value is an empty string") end,
        IsEmptyTable = function(value) assert(type(value) == "table" and next(value) == nil, "value is not an empty table") end,
        IsNotEmptyTable = function(value) assert(type(value) == "table" and next(value) ~= nil, "value is an empty table") end,
        Throws = function(fn) assert(pcall(fn) == false, "function did not throw") end,
        DoesNotThrow = function(fn) assert(pcall(fn) == true, "function threw an error") end,
    }

    local LibP2PDBTests = {
        New = function()
            local db = LibP2PDB:New({
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
        end,

        New_DescIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:New(nil) end)
            Assert.Throws(function() LibP2PDB:New(123) end)
            Assert.Throws(function() LibP2PDB:New("invalid") end)
            Assert.Throws(function() LibP2PDB:New({}) end)
        end,

        New_DescClusterIdIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:New({ clusterId = nil, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = 123, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = {}, namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = "", namespace = "n" }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = "abcdefg1234567890", namespace = "n" }) end)
        end,

        New_DescNamespaceIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:New({ clusterId = "c", namespace = nil }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = "c", namespace = 123 }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = "c", namespace = {} }) end)
            Assert.Throws(function() LibP2PDB:New({ clusterId = "c", namespace = "" }) end)
        end,

        DefineTable = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.IsTrue(LibP2PDB:DefineTable(db, {
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
            Assert.IsEmptyTable(t.rows)

            -- Attempt to define the same table again
            Assert.IsFalse(LibP2PDB:DefineTable(db, {
                name = "Users",
                keyType = "string",
            }))

            -- Ensure the original table definition remains unchanged
            local t2 = dbi.tables["Users"]
            Assert.AreEqual(t, t2)

            -- Define another table
            Assert.IsTrue(LibP2PDB:DefineTable(db, {
                name = "Products",
                keyType = "number",
            }))
        end,

        DefineTable_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:DefineTable(nil, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:DefineTable(123, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:DefineTable("invalid", { name = "Users", keyType = "string" }) end)
        end,

        DefineTable_DescIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, nil) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, 123) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, "invalid") end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, {}) end)
        end,

        DefineTable_DescNameIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = nil, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = 123, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "", keyType = "string" }) end)
        end,

        DefineTable_DescKeyTypeIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = nil }) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = 123 }) end)
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = "invalid" }) end)
        end,

        DefineTable_DescSchemaIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = "string", schema = 123 }) end)
        end,

        DefineTable_DescOnValidateIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = "string", onValidate = 123 }) end)
        end,

        DefineTable_DescOnChangeIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:DefineTable(db, { name = "Users", keyType = "string", onChange = 123 }) end)
        end,

        Insert = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, {
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

        Insert_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Insert(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert("invalid", "Users", 1, { name = "A" }) end)
        end,

        Insert_TableIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Insert(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, {}, 1, { name = "A" }) end)
        end,

        Insert_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", nil, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", {}, { name = "A" }) end)
        end,

        Insert_KeyTypeMismatch_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", { name = "A" }) end)
        end,

        Insert_RowIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", "user1", "invalid") end)
        end,

        Insert_RowSchemaMismatch_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = "25" }) end)
        end,

        Set = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, {
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

        Set_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Set(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set("invalid", "Users", 1, { name = "A" }) end)
        end,

        Set_TableIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Set(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, {}, 1, { name = "A" }) end)
        end,

        Set_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", nil, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", {}, { name = "A" }) end)
        end,

        Set_KeyTypeMismatch_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", { name = "A" }) end)
        end,

        Set_RowIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", "user1", "invalid") end)
        end,

        Set_RowSchemaMismatch_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:Set(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:Set(db, "Users", 1, { name = "Bob", age = "25" }) end)
        end,

        Update = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            local updateFn = function(row) row.age = row.age + 1 return row end
            Assert.IsTrue(LibP2PDB:Update(db, "Users", 1, updateFn))
            Assert.AreEqual(LibP2PDB:Get(db, "Users", 1), { name = "Bob", age = 26 })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", 2, updateFn) end)
            Assert.IsNil(LibP2PDB:Get(db, "Users", 2))
        end,

        Update_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Update(nil, "Users", 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(123, "Users", 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update("invalid", "Users", 1, function() end) end)
        end,

        Update_TableIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Update(db, nil, 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, 123, 1, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, {}, 1, function() end) end)
        end,

        Update_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", {}, function() end) end)
        end,

        Update_UpdateFunctionIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            LibP2PDB:Insert(db, "Users", "user1", { name = "A" })
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", nil) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", 123) end)
            Assert.Throws(function() LibP2PDB:Update(db, "Users", "user1", "invalid") end)
        end,

        Get = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
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
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Get(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:Get(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:Get(db, {}, 1) end)
        end,

        Get_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Get(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Get(db, "Users", {}) end)
        end,

        Delete = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:Insert(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.IsTrue(LibP2PDB:Delete(db, "Users", 1))
            Assert.IsNil(LibP2PDB:Get(db, "Users", 1))
            Assert.IsFalse(LibP2PDB:Delete(db, "Users", 2))
        end,

        Delete_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Delete(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:Delete("invalid", "Users", 1) end)
        end,

        Delete_TableIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            Assert.Throws(function() LibP2PDB:Delete(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:Delete(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:Delete(db, {}, 1) end)
        end,

        Delete_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:New({ clusterId = "c", namespace = "n" })
            LibP2PDB:DefineTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Delete(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Delete(db, "Users", {}) end)
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
        print("All LibP2PDB tests |cff00ff00passed|r.")
    end

    _G.LibP2PDB = { RunTests = RunTests }
end
