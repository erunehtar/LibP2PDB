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

--- @enum LibP2PDB.Color Color codes for console output.
local Color = {
    Ace = "ff33ff99",
    Debug = "ff00ffff",
    White = "ffffffff",
    Yellow = "ffffff00",
    Red = "ffff4040",
    Green = "ff00ff00",
    Blue = "ff8080ff",
}

--- @enum LibP2PDB.CommMessageType Communication message types.
local CommMessageType = {
    PeerDiscoveryRequest = 1,
    PeerDiscoveryResponse = 2,
    SnapshotRequest = 3,
    SnapshotResponse = 4,
    Digest = 5,
    RequestRows = 6,
    Rows = 7,
}

--- @enum LibP2PDB.CommPriority Communication priorities.
local CommPriority = {
    Low = "BULK",
    Normal = "NORMAL",
    High = "ALERT",
}

------------------------------------------------------------------------------------------------------------------------
-- Private Helper Functions
------------------------------------------------------------------------------------------------------------------------

--- Format text with color codes for console output.
--- @param color LibP2PDB.Color Color code.
--- @param text string Text to color
local function C(color, text)
    return "|c" .. color .. text .. "|r"
end

--- Print function.
--- @param fmt string Format string.
--- @param ... any Format arguments.
local function Print(fmt, ...)
    local success, message = pcall(format, fmt, ...)
    if not success then
        message = fmt
    end
    print(format("%s: %s", C(Color.Ace, "LibP2PDB"), message))
end

--- Debug print function, only outputs if DEBUG is true.
--- @param fmt string Format string.
--- @param ... any Format arguments.
local function Debug(fmt, ...)
    if DEBUG then
        local success, message = pcall(format, fmt, ...)
        if not success then
            message = fmt
        end
        Print("%s %s", C(Color.Debug, "[DEBUG]"), message)
    end
end

--- Error print function, only outputs if DEBUG is true.
--- @param fmt string Format string.
--- @param ... any Format arguments.
local function Error(fmt, ...)
    if DEBUG and not DISABLE_ERROR_REPORTING then
        local success, message = pcall(format, fmt, ...)
        if not success then
            message = fmt
        end
        Print("%s %s", C(Color.Red, "[ERROR]"), message)
    end
end

--- Dump a value to a string representation for debugging.
--- @param value any Value to dump.
local function Dump(value)
    if type(value) == "table" then
        local s = "{"
        for k, v in pairs(value) do
            if type(k) ~= "number" then
                k = '"' .. k .. '"'
            end
            s = s .. '[' .. k .. '] = ' .. Dump(v) .. ','
        end
        return s .. '}'
    elseif type(value) == "string" then
        return '"' .. value .. '"'
    else
        return tostring(value)
    end
end

--- Determine if a value is nil.
--- @param value any Value to check if it's nil.
--- @return boolean isNil True if the value is nil, false otherwise.
local function IsNil(value)
    return value == nil
end

--- Determine if a value is a string.
--- @param value any Value to check if it's a string.
--- @param maxlen number? Optional maximum length.
--- @return boolean isString True if the value is a string, false otherwise.
local function IsString(value, maxlen)
    return type(value) == "string" and (not maxlen or #value <= maxlen)
end

--- Determine if a value is a string or nil.
--- @param value any Value to check if it's a string or nil.
--- @param maxlen number? Optional maximum length.
--- @return boolean isStringOrNil True if the value is a string or nil, false otherwise.
local function IsStringOrNil(value, maxlen)
    return value == nil or IsString(value, maxlen)
end

--- Determine if a value is an empty string.
--- @param value any Value to check if it's an empty string.
--- @return boolean isEmptyString True if the value is an empty string, false otherwise.
local function IsEmptyString(value)
    return IsString(value) and #value == 0
end

--- Determine if a value is an empty string or nil.
--- @param value any Value to check if it's an empty string or nil.
--- @return boolean isEmptyStringOrNil True if the value is an empty string or nil, false otherwise.
local function IsEmptyStringOrNil(value)
    return value == nil or IsEmptyString(value)
end

--- Determine if a value is a non-empty string.
--- @param value any Value to check if it's a non-empty string.
--- @param maxlen number? Optional maximum length.
--- @return boolean isNonEmptyString True if the value is a non-empty string, false otherwise.
local function IsNonEmptyString(value, maxlen)
    return IsString(value, maxlen) and #value > 0
end

--- Determine if a value is a non-empty string or nil.
--- @param value any Value to check if it's a non-empty string or nil.
--- @param maxlen number? Optional maximum length.
--- @return boolean isNonEmptyStringOrNil True if the value is a non-empty string or nil, false otherwise.
local function IsNonEmptyStringOrNil(value, maxlen)
    return value == nil or IsNonEmptyString(value, maxlen)
end

--- Determine if a value is a number.
--- @param value any Value to check if it's a number.
--- @param minValue number? Optional minimum value.
--- @param maxValue number? Optional maximum value.
--- @return boolean isNumber True if the value is a number, false otherwise.
local function IsNumber(value, minValue, maxValue)
    return type(value) == "number" and value == value and (not minValue or value >= minValue) and (not maxValue or value <= maxValue) -- n == n checks for NaN
end

--- Determine if a value is a number or nil.
--- @param value any Value to check if it's a number or nil.
--- @param minValue number? Optional minimum value.
--- @param maxValue number? Optional maximum value.
--- @return boolean isNumberOrNil True if the value is a number or nil, false otherwise.
local function IsNumberOrNil(value, minValue, maxValue)
    return value == nil or IsNumber(value, minValue, maxValue)
end

--- Determine if a value is a boolean.
--- @param value any Value to check if it's a boolean.
--- @return boolean isBoolean True if the value is a boolean, false otherwise.
local function IsBoolean(value)
    return type(value) == "boolean"
end

--- Determine if a value is a boolean or nil.
--- @param value any Value to check if it's a boolean or nil.
--- @return boolean isBooleanOrNil True if the value is a boolean or nil, false otherwise.
local function IsBooleanOrNil(value)
    return value == nil or IsBoolean(value)
end

--- Determine if a value is a table.
--- @param value any Value to check if it's a table.
--- @return boolean isTable True if the value is a table, false otherwise.
local function IsTable(value)
    return type(value) == "table"
end

--- Determine if a value is a table or nil.
--- @param value any Value to check if it's a table or nil.
--- @return boolean isTableOrNil True if the value is a table or nil, false otherwise.
local function IsTableOrNil(value)
    return value == nil or IsTable(value)
end

--- Determine if a value is an empty table.
--- @param value any Value to check if it's an empty table.
--- @return boolean isEmptyTable True if the value is an empty table, false otherwise.
local function IsEmptyTable(value)
    return IsTable(value) and next(value) == nil
end

--- Determine if a value is an empty table or nil.
--- @param value any Value to check if it's an empty table or nil.
--- @return boolean isEmptyTableOrNil True if the value is an empty table or nil, false otherwise.
local function IsEmptyTableOrNil(value)
    return value == nil or IsEmptyTable(value)
end

--- Determine if a value is a non-empty table.
--- @param value any Value to check if it's a non-empty table.
--- @return boolean isNonEmptyTable True if the value is a non-empty table, false otherwise.
local function IsNonEmptyTable(value)
    return IsTable(value) and next(value) ~= nil
end

--- Determine if a value is a non-empty table or nil.
--- @param value any Value to check if it's a non-empty table or nil.
--- @return boolean isNonEmptyTableOrNil True if the value is a non-empty table or nil, false otherwise.
local function IsNonEmptyTableOrNil(value)
    return value == nil or IsNonEmptyTable(value)
end

--- Determine if a value is a primitive type.
--- @param value any Value to check if it's a primitive type.
--- @return boolean isPrimitive True if the value is a primitive type, false otherwise.
local function IsPrimitive(value)
    local t = type(value)
    return t == "boolean" or t == "string" or t == "number" or t == "nil"
end

--- Determine if a value is a primitive type or nil.
--- @param value any Value to check if it's a primitive type or nil.
--- @return boolean isPrimitiveOrNil True if the value is a primitive type or nil, false otherwise.
local function IsPrimitiveOrNil(value)
    return value == nil or IsPrimitive(value)
end

--- Determine if a type name is a primitive type.
--- @param typeName string Type name to check if it's a primitive type.
--- @return boolean isPrimitiveType True if the type name is a primitive type, false otherwise.
local function IsPrimitiveType(typeName)
    return typeName == "boolean" or typeName == "string" or typeName == "number" or typeName == "nil"
end

--- Determine if a value is a function.
--- @param value any Value to check if it's a function.
--- @return boolean isFunction True if the value is a function, false otherwise.
local function IsFunction(value)
    return type(value) == "function"
end

--- Determine if a value is a function or nil.
--- @param value any Value to check if it's a function or nil.
--- @return boolean isFunctionOrNil True if the value is a function or nil, false otherwise.
local function IsFunctionOrNil(value)
    return value == nil or IsFunction(value)
end

--- Determine if a value implements an interface (has required function names).
--- @param value any Value to check if it implements the interface.
--- @param ... string Function names required by the interface.
local function IsInterface(value, ...)
    if type(value) ~= "table" then
        return false
    end
    local interface = { ... }
    for _, fnName in ipairs(interface) do
        local fn = value[fnName]
        if not IsFunction(fn) then
            return false
        end
    end
    return true
end

--- Determine if a value implements an interface or is nil.
--- @param value any Value to check if it implements the interface or is nil.
--- @param ... string Function names required by the interface.
local function IsInterfaceOrNil(value, ...)
    return value == nil or IsInterface(value, ...)
end

--- Determine if an array contains a value.
--- @param array any[] Array to search.
--- @param value any Value to search for.
local function Contains(array, value)
    local len = #array
    for i = 1, len do
        if array[i] == value then
            return true
        end
    end
    return false
end

--- Deep copy a value (recursively copies all nested tables).
--- @param value any Value to deep copy.
--- @return any copy The deep copied value.
local function DeepCopy(value)
    if type(value) ~= "table" then
        return value
    end
    local copy = {}
    for k, v in pairs(value) do
        copy[k] = DeepCopy(v)
    end
    return copy
end

--- Deep compare two values for equality.
--- @param a any First value to compare.
--- @param b any Second value to compare.
--- @return boolean isEqual True if the values are deeply equal, false otherwise.
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

--- Shallow copy a value (non-recursively copies nested tables).
--- @param value any Value to shallow copy.
--- @return any copy The shallow copied value.
local function ShallowCopy(value)
    if type(value) ~= "table" then
        return value
    end
    local copy = {}
    for k, v in pairs(value) do
        copy[k] = v
    end
    return copy
end

--- Shallow compare two values for equality.
--- @param a any First value to compare.
--- @param b any Second value to compare.
--- @return boolean isEqual True if the values are shallowly equal, false otherwise.
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

--- Report a non-fatal error without stopping execution.
--- @param dbi LibP2PDB.DBInstance? Database instance if available.
--- @param fmt string Format string.
--- @param ... any Format arguments.
--- @return string message The error message.
local function ReportError(dbi, fmt, ...)
    local success, message = pcall(format, fmt, ...)
    if not success then
        message = fmt
    end
    if dbi and dbi.onError then
        dbi.onError(message, debugstack(2))
    else
        Error(message)
        LAST_ERROR = message
    end
    return message
end

--- Safely call a function with error handling.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param func function Function to call.
--- @param ... any Additional arguments to pass to the function to call.
--- @return boolean success True if the function executed successfully, false on error.
--- @return ... Results from the function if successful, or nil on error.
local function SafeCall(dbi, func, ...)
    assert(IsTable(dbi))
    assert(IsFunction(func))

    -- Use xpcall to catch errors
    local results = {
        xpcall(func, function(err)
            if dbi and dbi.onError then
                dbi.onError(tostring(err), debugstack(2))
            else
                Error(tostring(err))
                LAST_ERROR = tostring(err)
            end
        end, ...)
    }

    -- If the function threw an error, return false with no results
    if not results[1] then
        return false
    end

    -- Return success and all results from the function
    return true, unpack(results, 2)
end

------------------------------------------------------------------------------------------------------------------------
-- Private State
------------------------------------------------------------------------------------------------------------------------

--- @type table
local Private = {}

--- @type string
Private.playerName = assert(UnitName("player"), "unable to get player name")

--- @type string
Private.playerGUID = assert(UnitGUID("player"), "unable to get player GUID")

--- @type string
Private.peerId = strsub(Private.playerGUID, 8) -- skip "Player-" prefix

--- @type table<string, LibP2PDB.DBHandle>
Private.prefixes = setmetatable({}, { __mode = "v" })

--- @type table<LibP2PDB.DBHandle, LibP2PDB.DBInstance>
Private.databases = setmetatable({}, { __mode = "k" })

--- @type Frame
Private.frame = CreateFrame("Frame", "LibP2PDB")
Private.frame:SetScript("OnUpdate", function(self)
    for _, dbi in pairs(Private.databases) do
        Private:OnUpdate(dbi)
    end
end)

------------------------------------------------------------------------------------------------------------------------
-- Public API: Database Instance Creation
------------------------------------------------------------------------------------------------------------------------

--- @class LibP2PDB.DBHandle Database handle.

--- @class LibP2PDB.DBDesc Description for creating a new database.
--- @field prefix string Unique communication prefix for the database (max 16 chars).
--- @field onError LibP2PDB.DBOnErrorCallback? Optional callback function(errMsg, stack) invoked on errors.
--- @field channels string[]? Optional array of custom channels to use for broadcasts, in addition to default channels (GUILD, RAID, PARTY, YELL).
--- @field serializer LibP2PDB.Serializer? Optional custom serializer for encoding/decoding data (default: LibSerialize or AceSerializer if available).
--- @field compressor LibP2PDB.Compressor? Optional custom compressor for compressing/decompressing data (default: LibDeflate if available).
--- @field encoder LibP2PDB.Encoder? Optional custom encoder for encoding/decoding data for chat channels and print (default: LibDeflate if available).
--- @field onChange LibP2PDB.DBOnChangeCallback? Optional callback function(tableName, key, data) invoked on any row change.
--- @field discoveryQuietPeriod number? Optional seconds of quiet time with no new peers before considering discovery complete (default: 1.0).
--- @field discoveryMaxTime number? Optional maximum seconds to wait for peer discovery before considering it complete (default: 3.0).
--- @field onDiscoveryComplete LibP2PDB.DBOnDiscoveryCompleteCallback? Optional callback function(isInitial) invoked when peers discovery completes.

--- @class LibP2PDB.Serializer Serializer interface for encoding/decoding data.
--- @field Serialize fun(self: LibP2PDB.Serializer, value: any): string Serializes a value to a string.
--- @field Deserialize fun(self: LibP2PDB.Serializer, str: string): boolean, any? Deserializes a string back to a value.

--- @class LibP2PDB.Compressor Compressor interface for compressing/decompressing data.
--- @field Compress fun(self: LibP2PDB.Compressor, str: string): string Compresses a string.
--- @field Decompress fun(self: LibP2PDB.Compressor, str: string): string? Decompresses a string.

--- @class LibP2PDB.Encoder Encoder interface for encoding/decoding data.
--- @field EncodeChannel fun(self: LibP2PDB.Encoder, str: string): string Encodes a string for safe transmission over chat channels.
--- @field DecodeChannel fun(self: LibP2PDB.Encoder, str: string): string? Decodes a string received from chat channels.
--- @field EncodePrint fun(self: LibP2PDB.Encoder, str: string): string Encodes a string for safe printing in chat windows.
--- @field DecodePrint fun(self: LibP2PDB.Encoder, str: string): string? Decodes a string received from chat windows.

--- @alias LibP2PDB.DBOnErrorCallback fun(errMsg: string, stack: string?) Callback function invoked on errors.
--- @alias LibP2PDB.DBOnChangeCallback fun(tableName: string, key: LibP2PDB.TableKey, data: LibP2PDB.TableRowData?) Callback function invoked on any row change.
--- @alias LibP2PDB.DBOnDiscoveryCompleteCallback fun(isInitial: boolean) Callback function invoked when peer discovery completes.

--- Create a new peer-to-peer synchronized database instance.
--- Each database is identified by a unique prefix and operates independently.
--- Database configuration cannot be changed after creation.
--- Use GetDatabase to retrieve existing databases by prefix.
--- @param desc LibP2PDB.DBDesc Description of the database to create.
--- @return LibP2PDB.DBHandle dbh The newly created database handle.
function LibP2PDB:NewDatabase(desc)
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.prefix, 16), "desc.prefix must be a non-empty string (max 16 chars)")
    assert(IsFunctionOrNil(desc.onError), "desc.onError must be a function if provided")
    assert(IsNonEmptyTableOrNil(desc.channels), "desc.channels must be a non-empty array of string if provided")
    assert(IsInterfaceOrNil(desc.serializer, "Serialize", "Deserialize"), "desc.serializer must be a serializer interface if provided")
    assert(IsInterfaceOrNil(desc.compressor, "Compress", "Decompress"), "desc.compressor must be a compressor interface if provided")
    assert(IsInterfaceOrNil(desc.encoder, "EncodeChannel", "DecodeChannel", "EncodePrint", "DecodePrint"), "desc.encoder must be an encoder interface if provided")
    assert(IsFunctionOrNil(desc.onChange), "desc.onChange must be a function if provided")
    assert(IsNumberOrNil(desc.discoveryQuietPeriod, 0), "desc.discoveryQuietPeriod must be a positive number if provided")
    assert(IsNumberOrNil(desc.discoveryMaxTime, 0), "desc.discoveryMaxTime must be a positive number if provided")
    assert(IsFunctionOrNil(desc.onDiscoveryComplete), "desc.onDiscoveryComplete must be a function if provided")

    if desc.channels then
        for _, channel in ipairs(desc.channels) do
            assert(IsNonEmptyString(channel), "each channel in desc.channels must be a non-empty string")
        end
    end

    -- Ensure prefix is unique
    assert(Private.prefixes[desc.prefix] == nil, "a database with prefix '" .. desc.prefix .. "' already exists")

    -- Create the new database
    --- @type LibP2PDB.DBInstance
    local dbi = {
        -- Identity
        prefix = desc.prefix,
        clock = 0,
        -- Configuration
        channels = desc.channels,
        discoveryQuietPeriod = desc.discoveryQuietPeriod or 1.0,
        discoveryMaxTime = desc.discoveryMaxTime or 3.0,
        serializer = desc.serializer,
        compressor = desc.compressor,
        encoder = desc.encoder,
        -- Networking
        peers = {},
        buckets = {},
        -- Data
        tables = {},
        -- Callbacks
        onError = desc.onError,
        onChange = desc.onChange,
        onDiscoveryComplete = desc.onDiscoveryComplete,
        -- Access control
        --writePolicy = nil,
    }

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

    -- Setup default encoder if none provided
    if not dbi.encoder then
        if LibDeflate then
            --- @class LibP2PDB.LibDeflateEncoder : LibP2PDB.Encoder
            --- @field channelCodec table Custom codec instance
            dbi.encoder = {
                -- Create custom codec that rejects \000 (NULL), \010 (LF), and \013 (CR).
                -- This is because SAY/YELL channels cannot handle LF and CR characters in
                -- addition to NULL and will just truncate the message if they are present.
                channelCodec = LibDeflate:CreateCodec("\000\010\013", "\001", ""),
                EncodeChannel = function(self, str)
                    return self.channelCodec:Encode(str)
                end,
                DecodeChannel = function(self, str)
                    return self.channelCodec:Decode(str)
                end,
                EncodePrint = function(self, str)
                    return LibDeflate:EncodeForPrint(str)
                end,
                DecodePrint = function(self, str)
                    return LibDeflate:DecodeForPrint(str)
                end,
            }
        else
            error("encoder required but none found; provide custom encoder via desc.encoder")
        end
    end

    -- Validate encoding works
    do
        local testString = "This is a test string for encoding.\000\010\013"
        local encodedChannel = dbi.encoder:EncodeChannel(testString)
        local decodedChannel = dbi.encoder:DecodeChannel(encodedChannel)
        assert(decodedChannel == testString, "encoder provided in desc.encoder is invalid for channel encoding")

        local encodedPrint = dbi.encoder:EncodePrint(testString)
        local decodedPrint = dbi.encoder:DecodePrint(encodedPrint)
        assert(decodedPrint == testString, "encoder provided in desc.encoder is invalid for print encoding")
    end

    -- Register comm prefix
    AceComm.RegisterComm(Private, desc.prefix)
    assert(C_ChatInfo.IsAddonMessagePrefixRegistered(desc.prefix), "failed to register addon message prefix '" .. desc.prefix .. "'")

    -- Register the new database
    --- @type LibP2PDB.DBHandle
    local dbh = {}
    Private.prefixes[desc.prefix] = dbh
    Private.databases[dbh] = dbi
    return dbh
end

--- Retrieve a database by its prefix.
--- @param prefix string Unique communication prefix for the database (max 16 chars).
--- @return LibP2PDB.DBHandle? dbh The database handle if found, or nil if not found.
function LibP2PDB:GetDatabase(prefix)
    assert(IsNonEmptyString(prefix, 16), "prefix must be a non-empty string (max 16 chars)")
    return Private.prefixes[prefix]
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Table Definition
------------------------------------------------------------------------------------------------------------------------

--- @class LibP2PDB.TableDesc Description for creating a new table in the database.
--- @field name string Name of the table to create.
--- @field keyType LibP2PDB.TableKeyType Data type of the primary key.
--- @field schema LibP2PDB.TableSchema? Optional table schema defining field names and their allowed data types.
--- @field onValidate LibP2PDB.TableOnValidateCallback? Optional callback function(key, data) for custom row validation. Must return true if valid, false otherwise. Data is a copy and has not yet been applied when this is called.
--- @field onChange LibP2PDB.TableOnChangeCallback? Optional callback function(key, data) on row data changes. Data is nil for deletions. Data is a copy, and has already been applied when this is called.

--- @alias LibP2PDB.TableKeyType "string"|"number" Data type of the primary key.
--- @alias LibP2PDB.TableKey string|number Primary key value.
--- @alias LibP2PDB.TableRowData table<string|number, boolean|string|number|nil> Row data containing fields.
--- @alias LibP2PDB.TableSchema table<string|number, string|string[]> Table schema definition.
--- @alias LibP2PDB.TableSchemaSorted [string|number, string|string[]] Table schema as a sorted array of field name and allowed types pairs.
--- @alias LibP2PDB.TableOnValidateCallback fun(key: LibP2PDB.TableKey, data: LibP2PDB.TableRowData):boolean Callback function for custom row validation.
--- @alias LibP2PDB.TableOnChangeCallback fun(key: LibP2PDB.TableKey, data: LibP2PDB.TableRowData?) Callback function invoked on row data changes.

--- Create a new table in the database with an optional schema.
--- If no schema is provided, the table accepts any fields.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param desc LibP2PDB.TableDesc Description of the table to define.
function LibP2PDB:NewTable(db, desc)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyTable(desc), "desc must be a non-empty table")
    assert(IsNonEmptyString(desc.name), "desc.name must be a non-empty string")
    assert(IsNonEmptyString(desc.keyType), "desc.keyType must be a non-empty string")
    assert(desc.keyType == "string" or desc.keyType == "number", "desc.keyType must be 'string' or 'number'")
    assert(IsNonEmptyTableOrNil(desc.schema), "desc.schema must be a non-empty table if provided")
    for fieldKey, allowedTypes in pairs(desc.schema or {}) do
        assert(IsNonEmptyString(fieldKey) or IsNumber(fieldKey), "each field key in desc.schema must be a non-empty string or number")
        if IsNonEmptyTable(allowedTypes) then
            --- @cast allowedTypes string[]
            for _, allowedType in ipairs(allowedTypes) do
                assert(IsNonEmptyString(allowedType), "each type in desc.schema field types must be a non-empty string")
                assert(IsPrimitiveType(allowedType), "field types in desc.schema must be 'string', 'number', 'boolean', or 'nil'")
            end
        elseif IsNonEmptyString(allowedTypes) then
            --- @cast allowedTypes string
            assert(IsPrimitiveType(allowedTypes), "field type in desc.schema must be 'string', 'number', 'boolean', or 'nil'")
        else
            error("each field value in desc.schema must be a non-empty string or non-empty table of strings")
        end
    end
    assert(IsFunctionOrNil(desc.onValidate), "desc.onValidate must be a function if provided")
    assert(IsFunctionOrNil(desc.onChange), "desc.onChange must be a function if provided")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Ensure table name is unique
    assert(dbi.tables[desc.name] == nil, "table '" .. desc.name .. "' already exists in the database")

    -- Create the table instance
    --- @type LibP2PDB.TableInstance
    dbi.tables[desc.name] = {
        keyType = desc.keyType,
        schema = desc.schema,
        onValidate = desc.onValidate,
        onChange = desc.onChange,
        subscribers = setmetatable({}, { __mode = "k" }),
        rows = {},
    }
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: CRUD Operations
------------------------------------------------------------------------------------------------------------------------

--- Insert a new key into a table.
--- Validates the key type and row schema against the table definition.
--- If a schema is defined, extra fields in the row are ignored.
--- Fails if the key already exists (use SetKey to overwrite).
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to insert into.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
--- @param data table Data for the new row.
--- @return boolean success Returns true on success, false otherwise.
function LibP2PDB:InsertKey(db, tableName, key, data)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")
    assert(IsTable(data), "data must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Ensure the key does not already exist
    local existingRow = ti.rows[key]
    if existingRow and not existingRow.version.tombstone then
        error("key '" .. tostring(key) .. "' already exists in table '" .. tableName .. "'")
    end

    -- Set the row
    return Private:SetKey(dbi, tableName, ti, key, data)
end

--- Create or replace a key in a table.
--- Validates the key type and row schema against the table definition.
--- If a schema is defined, extra fields in the row are ignored.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to set into.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
--- @param data table Row data containing fields defined in the table schema.
--- @return boolean success Returns true on success, false otherwise.
function LibP2PDB:SetKey(db, tableName, key, data)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")
    assert(IsTable(data), "row must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Set the row
    return Private:SetKey(dbi, tableName, ti, key, data)
end

--- @alias LibP2PDB.TableUpdateFunction fun(data: LibP2PDB.TableRowData?): LibP2PDB.TableRowData Function invoked to produce updated row data.

--- Create or update a key in a table.
--- Validates the key type against the table definition.
--- The update function is called with the current row data and must return the updated row data.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to update.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
--- @param updateFn LibP2PDB.TableUpdateFunction Function invoked to produce updated row data. Current row data is passed as a copy.
--- @return boolean success Returns true on success, false otherwise.
function LibP2PDB:UpdateKey(db, tableName, key, updateFn)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")
    assert(IsFunction(updateFn), "updateFn must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Call the update function to get the updated row data
    local existingRow = ti.rows[key]
    local success, updatedRow = SafeCall(dbi, updateFn, existingRow and ShallowCopy(existingRow.data) or nil)
    if not success then
        return false
    end
    assert(IsTable(updatedRow), "updateFn must return a table")

    -- Set the row
    return Private:SetKey(dbi, tableName, ti, key, updatedRow)
end

--- Delete a key from a table.
--- Validates the key type against the table definition.
--- Marks the row as a tombstone for gossip synchronization, whether the row existed or not.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to delete from.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
--- @return boolean success Returns true on success, false otherwise.
function LibP2PDB:DeleteKey(db, tableName, key)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Set the row
    return Private:SetKey(dbi, tableName, ti, key, nil)
end

--- Determine if a key exists in a table.
--- A key exists if it is present and not marked as a tombstone.
--- Validates the key type against the table definition.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to check.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
function LibP2PDB:HasKey(db, tableName, key)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Lookup the row and return existence
    local row = ti.rows[key]
    if row == nil or row.version.tombstone then
        return false
    end
    return true
end

--- Retrieve the data for a specific key in a table.
--- A key exists if it is present and not marked as a tombstone.
--- Validates the key type against the table definition.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to get from.
--- @param key LibP2PDB.TableKey Primary key value for the row (must match table's keyType).
--- @return LibP2PDB.TableRowData? rowData The row data if found, or nil if not found.
function LibP2PDB:GetKey(db, tableName, key)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsNonEmptyString(key) or IsNumber(key), "key must be a string or number")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table and key type
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Lookup the row
    local row = ti.rows[key]
    if row == nil or row.version.tombstone then
        return nil
    end

    -- Return a copy of the row data
    return ShallowCopy(row.data)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Subscriptions
------------------------------------------------------------------------------------------------------------------------

--- Subscribe to changes in a specific table.
--- The callback is invoked with the key and new data (nil for deletions) whenever a row changes.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to subscribe to
--- @param callback LibP2PDB.TableOnChangeCallback Function(key, data) to invoke on changes
function LibP2PDB:Subscribe(db, tableName, callback)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsFunction(callback), "callback must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")

    -- Register subscriber (safe even if already present)
    ti.subscribers[callback] = true
end

--- Unsubscribe to changes in a specific table.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to unsubscribe from
--- @param callback LibP2PDB.TableOnChangeCallback Function(key, data) to remove from subscriptions
function LibP2PDB:Unsubscribe(db, tableName, callback)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "table name must be a non-empty string")
    assert(IsFunction(callback), "callback must be a function")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table
    local t = dbi.tables[tableName]
    assert(t, "table '" .. tableName .. "' is not defined in the database")

    -- Remove subscriber (safe even if not present)
    t.subscribers[callback] = nil
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Persistence
------------------------------------------------------------------------------------------------------------------------

--- @class LibP2PDB.DBState Database state.
--- @field clock number Lamport clock of the exported database.
--- @field tables table<string, LibP2PDB.TableState>? Registry of tables, or nil if no tables.

--- @class LibP2PDB.TableState Table state.
--- @field rows table<LibP2PDB.TableKey, LibP2PDB.TableRow> Registry of table rows.

--- Export the database state to a table.
--- Empty tables are omitted from the exported state.
--- @param db LibP2PDB.DBHandle Database handle.
--- @return LibP2PDB.DBState state The exported database state.
function LibP2PDB:Export(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Export database state
    return Private:ExportDatabase(dbi)
end

--- Import a database state from a table.
--- Merges the imported state with existing data based on version metadata.
--- Validates incoming data against table definitions, skipping invalid entries.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param state LibP2PDB.DBState The database state to import.
function LibP2PDB:Import(db, state)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsTable(state), "state must be a table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Import database state
    Private:ImportDatabase(dbi, state)
end

--- Serialize the database state to a compact binary string format.
--- If a table has a schema, fields are serialized in alphabetical order without names.
--- If no schema is defined, all fields are serialized in arbitrary order with names.
--- Empty tables are omitted from the serialized output.
--- @param db LibP2PDB.DBHandle Database handle.
--- @return string serialized The serialized database state.
function LibP2PDB:Serialize(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Serialize database state
    return Private:SerializeDatabase(dbi)
end

--- Deserialize a database state from a compact binary string format.
--- Merges the deserialized data with existing data based on version metadata.
--- Validates incoming data against table definitions, skipping invalid entries.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param str string The serialized database string to deserialize.
function LibP2PDB:Deserialize(db, str)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(str), "str must be a non-empty string")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Deserialize the serialized database
    local state = Private:DeserializeDatabase(dbi, str)
    if not state then
        ReportError(dbi, "failed to deserialize database state")
        return
    end

    -- Import the database state
    --- @cast state LibP2PDB.DBState
    Private:ImportDatabase(dbi, state)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Sync / Gossip Controls
------------------------------------------------------------------------------------------------------------------------

--- Discover peers on the database's communication prefix.
--- If onDiscoveryComplete callback is defined, it will be invoked when discovery completes.
--- @param db LibP2PDB.DBHandle Database handle.
function LibP2PDB:DiscoverPeers(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Send the discover peers message
    local obj = {
        type = CommMessageType.PeerDiscoveryRequest,
        peer = Private.peerId,
    }
    Private:Broadcast(dbi, obj, dbi.channels, CommPriority.Low)

    -- Record the time of the peer discovery request
    if dbi.onDiscoveryComplete then
        dbi.discoveryStartTime = GetTime()
        dbi.lastDiscoveryResponseTime = dbi.discoveryStartTime
    end
end

--- Request a full snapshot from a peer or broadcast to all peers.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param target string? Optional target peer to request the snapshot from; if nil, broadcasts to all peers
function LibP2PDB:RequestSnapshot(db, target)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(target == nil or IsNonEmptyString(target), "target must be a non-empty string if provided")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Send the request message
    local obj = {
        type = CommMessageType.SnapshotRequest,
        peer = Private.peerId,
    }
    if target then
        Private:Send(dbi, obj, "WHISPER", target, CommPriority.Normal)
    else
        -- Request snapshot from discovered peers
        for _, peerData in pairs(dbi.peers) do
            if peerData.clock > dbi.clock then
                Private:Send(dbi, obj, "WHISPER", peerData.name, CommPriority.Normal)
            end
        end
    end
end

--- Immediately initiate a gossip sync by sending the current digest to all peers.
--- @param db LibP2PDB.DBHandle Database handle.
function LibP2PDB:SyncNow(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Build digest
    local digest = Private:BuildDigest(dbi)

    -- Broadcast the digest
    local obj = {
        type = CommMessageType.Digest,
        peer = Private.peerId,
        data = digest,
    }
    Private:Broadcast(dbi, obj, dbi.channels, CommPriority.Normal)
end

------------------------------------------------------------------------------------------------------------------------
-- Public API: Utility / Metadata
------------------------------------------------------------------------------------------------------------------------

--- Return the local peer's unique ID
--- @return string peerId The local peer ID
function LibP2PDB:GetPeerId()
    return Private.peerId
end

--- Return a remote peer's unique ID from its GUID
--- @param guid string Full GUID of the remote peer
--- @return string? peerId The remote peer ID if valid, or nil if not
function LibP2PDB:GetPeerIdFromGUID(guid)
    assert(IsNonEmptyString(guid), "guid must be a non-empty string")
    if strsub(guid, 1, 7) ~= "Player-" then
        return nil
    end
    return strsub(guid, 8) -- skip "Player-" prefix
end

--- List all defined tables in the database.
--- @param db LibP2PDB.DBHandle Database handle.
--- @return table<string> tables Array of table names defined in the database
function LibP2PDB:ListTables(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Collect table names
    local tableNames = {}
    for tableName in pairs(dbi.tables) do
        tinsert(tableNames, tableName)
    end
    return tableNames
end

--- List all keys of a specific table in the database.
--- @param db LibP2PDB.DBHandle Database handle.
--- @param tableName string Name of the table to list keys from
--- @return table<LibP2PDB.TableKey> keys Array of keys in the specified table
function LibP2PDB:ListKeys(db, tableName)
    assert(IsEmptyTable(db), "db must be an empty table")
    assert(IsNonEmptyString(tableName), "tableName must be a non-empty string")

    -- Validate db instance
    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Validate table
    local ti = dbi.tables[tableName]
    assert(ti, "table '" .. tableName .. "' is not defined in the database")

    -- Collect keys
    local keys = {}
    for key, row in pairs(ti.rows) do
        if row and not row.version.tombstone then
            tinsert(keys, key)
        end
    end
    return keys
end

--- List all discovered peers for this database.
--- This list is not persisted and is reset on logout/reload.
--- @param db LibP2PDB.DBHandle Database handle.
--- @return table<string, table> peers Table of peerId -> peer data
function LibP2PDB:ListPeers(db)
    assert(IsEmptyTable(db), "db must be an empty table")

    local dbi = Private.databases[db]
    assert(dbi, "db is not a recognized database handle")

    -- Collect peers
    local peers = {}
    for peerId, data in pairs(dbi.peers) do
        peers[peerId] = DeepCopy(data)
    end
    return peers
end

--- Sanitize a string for printing in WoW chat channels.
--- Replaces non-printable characters with their numeric byte values prefixed by a backslash.
--- @param s string Input string
--- @return string sanitized The sanitized string safe for printing
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
-- Private API
------------------------------------------------------------------------------------------------------------------------

--- @class LibP2PDB.DBInstance Database instance.
--- @field prefix string Unique communication prefix.
--- @field clock number Lamport clock for versioning.
--- @field channels string[]? List of custom channels for broadcasts.
--- @field discoveryQuietPeriod number Seconds of quiet time for discovery completion.
--- @field discoveryMaxTime number Maximum time for discovery completion.
--- @field serializer LibP2PDB.Serializer Serializer interface.
--- @field compressor LibP2PDB.Compressor Compressor interface.
--- @field encoder LibP2PDB.Encoder Encoder interface.
--- @field peers table<string, LibP2PDB.PeerInfo> Known peers for this session.
--- @field buckets table Communication event buckets for burst control.
--- @field tables table<string, LibP2PDB.TableInstance> Defined tables in the database.
--- @field onError LibP2PDB.DBOnErrorCallback? Callback for error events.
--- @field onChange LibP2PDB.DBOnChangeCallback? Callback for row changes.
--- @field onDiscoveryComplete LibP2PDB.DBOnDiscoveryCompleteCallback? Callback for discovery completion.
--- @field discoveryStartTime number? Time when discovery started.
--- @field lastDiscoveryResponseTime number? Time when last discovery response was received.
--- @field isInitialDiscoveryComplete boolean? Flag indicating if initial discovery is complete.

--- @class LibP2PDB.PeerInfo Peer information.
--- @field name string Name of the peer.
--- @field clock number Lamport clock of the peer's database.
--- @field lastSeen number Timestamp of the last time the peer was seen.

--- @class LibP2PDB.TableInstance Table instance.
--- @field keyType LibP2PDB.TableKeyType Primary key type for the table.
--- @field schema LibP2PDB.TableSchema? Optional schema definition for the table.
--- @field onValidate LibP2PDB.TableOnValidateCallback? Optional validation callback for rows.
--- @field onChange LibP2PDB.TableOnChangeCallback? Optional change callback for rows.
--- @field subscribers table<LibP2PDB.TableOnChangeCallback, boolean> Weak table of subscriber callbacks.
--- @field rows table<LibP2PDB.TableKey, LibP2PDB.TableRow> Registry of rows in the table.

--- @class LibP2PDB.TableRow Table row definition.
--- @field data LibP2PDB.TableRowData? Data for the row, or nil if the row is a tombstone (deleted).
--- @field version LibP2PDB.TableRowVersion Version metadata for the row.

--- @class LibP2PDB.TableRowVersion Table row version metadata.
--- @field clock number Lamport clock value.
--- @field peer string Peer ID that last modified the row.
--- @field tombstone boolean? Optional flag indicating if the row is a tombstone (deleted).

--- Retrieve the schema definition for a specific table instance.
--- @param ti LibP2PDB.TableInstance Table instance.
--- @param sorted boolean? Optional flag to return the schema with sorted field names (default: false).
--- @return LibP2PDB.TableSchema|LibP2PDB.TableSchemaSorted|nil schema The table schema, or nil if no schema is defined. If sorted is true, returns an array of {fieldName, fieldType(s)} pairs sorted by fieldName.
function Private:GetTableSchema(ti, sorted)
    if not ti.schema then
        return nil
    end
    if sorted then
        --- @type LibP2PDB.TableSchemaSorted
        local schemaSorted = {}
        local fieldNames = {}
        for fieldName in pairs(ti.schema) do
            tinsert(fieldNames, fieldName)
        end
        tsort(fieldNames)
        for _, fieldName in ipairs(fieldNames) do
            tinsert(schemaSorted, { fieldName, ti.schema[fieldName] })
        end
        return schemaSorted
    else
        return ti.schema
    end
end

--- Set a row in a table, overwriting any existing row.
--- Validates the row schema against the table definition.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param tableName string Name of the table.
--- @param ti LibP2PDB.TableInstance Table instance.
--- @param key LibP2PDB.TableKey Primary key value for the row.
--- @param data table? Row data containing fields defined in the table schema (or nil for tombstone).
--- @return boolean success Returns true on success, false otherwise.
function Private:SetKey(dbi, tableName, ti, key, data)
    -- Prepare the row data
    local rowData = Private:PrepareRowData(tableName, ti, data)

    -- Run custom validation if provided
    if rowData and ti.onValidate then
        local success, result = SafeCall(dbi, ti.onValidate, key, rowData)
        if not success then
            return false -- validation threw an error
        end
        assert(IsBoolean(result), "onValidate must return a boolean")
        if not result then
            return false -- validation failed
        end
    end

    -- Determine if the row will change
    local changes = false
    local existingRow = ti.rows[key]
    if rowData then
        if not existingRow or existingRow.version.tombstone or not ShallowEqual(existingRow.data, rowData) then
            changes = true -- new row or data changes
        end
    else
        if not existingRow or not existingRow.version.tombstone then
            changes = true -- new tombstone row or existing row is not a tombstone
        end
    end

    -- Apply changes if any
    if changes then
        -- Versioning (Lamport clock)
        dbi.clock = dbi.clock + 1

        -- Determine peer value for version metadata
        local peerValue = (key == Private.peerId) and "=" or Private.peerId

        -- Store the row
        ti.rows[key] = {
            data = rowData,
            version = {
                clock = dbi.clock,
                peer = peerValue,
                tombstone = (rowData == nil) and true or nil,
            },
        }

        -- Invoke row changed callbacks
        Private:InvokeChangeCallbacks(dbi, tableName, ti, key, rowData)
    end

    return true
end

--- Prepare data for a row.
--- If a schema is defined, validates field types and copies only defined fields.
--- If no schema is defined, copies all primitive fields with string or number keys.
--- @param tableName string Name of the table.
--- @param ti LibP2PDB.TableInstance Table instance.
--- @param data table? Row data to prepare (or nil for tombstone).
--- @return LibP2PDB.TableRowData? rowData The processed row data.
function Private:PrepareRowData(tableName, ti, data)
    if data == nil then
        return nil -- Tombstone
    end

    local rowData = {}
    if ti.schema then
        -- Schema defined: validate and copy only defined fields
        for k, allowedTypes in pairs(ti.schema) do
            local v = data[k]
            local t = type(v)
            if IsTable(allowedTypes) then
                --- @cast allowedTypes string[]
                assert(Contains(allowedTypes, t), "expected field '" .. k .. "' of type '" .. strjoin(", ", unpack(allowedTypes)) .. "' in table '" .. tableName .. "', but was '" .. t .. "'")
            elseif IsString(allowedTypes) then
                --- @cast allowedTypes string
                assert(t == allowedTypes, "expected field '" .. k .. "' of type '" .. allowedTypes .. "' in table '" .. tableName .. "', but was '" .. t .. "'")
            else
                error("invalid schema definition for field '" .. k .. "' in table '" .. tableName .. "'")
            end
            rowData[k] = v
        end
    else
        -- No schema: copy all primitive fields with string or number keys
        for k, v in pairs(data) do
            if (IsString(k) or IsNumber(k)) and IsPrimitive(v) then
                rowData[k] = v
            end
        end
    end
    return rowData
end

--- Prepare version metadata for a row.
--- @param version table LibP2PDB.TableRowVersion Input version metadata.
--- @return LibP2PDB.TableRowVersion rowVersion The sanitized version metadata.
function Private:PrepareRowVersion(version)
    assert(IsNonEmptyTable(version), "version must be a non-empty table")
    assert(IsNumber(version.clock, 0), "clock must be a positive number")
    assert(IsNonEmptyString(version.peer), "peer must be a non-empty string")
    assert(IsBooleanOrNil(version.tombstone), "tombstone must be a boolean or nil")
    local rowVersion = {
        clock = version.clock,
        peer = version.peer,
    }
    if version.tombstone then
        rowVersion.tombstone = true
    end
    return rowVersion
end

--- Invoke change callbacks for a row change.
--- Skips callbacks during import operations.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param tableName string Name of the table.
--- @param ti LibP2PDB.TableInstance Table instance.
--- @param key LibP2PDB.TableKey Primary key value for the row.
--- @param rowData LibP2PDB.TableRowData? New row data (or nil for tombstone).
function Private:InvokeChangeCallbacks(dbi, tableName, ti, key, rowData)
    -- Skip callbacks during import
    if Private.isImporting then
        return
    end

    -- Invoke database global change callback
    if dbi.onChange then
        SafeCall(dbi, dbi.onChange, tableName, key, rowData)
    end

    -- Invoke database table change callback
    if ti.onChange then
        SafeCall(dbi, ti.onChange, key, rowData)
    end

    -- Invoke database table subscribers
    for callback in pairs(ti.subscribers) do
        SafeCall(dbi, callback, key, rowData)
    end
end

--- Export the database state to a table.
--- Empty tables are omitted from the exported state.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @return LibP2PDB.DBState state The exported database state.
function Private:ExportDatabase(dbi)
    local state = {
        clock = dbi.clock,
    }

    local tables = {}
    for tableName, tableData in pairs(dbi.tables) do
        local rows = {}
        for key, row in pairs(tableData.rows) do
            rows[key] = {
                data = ShallowCopy(row.data),
                version = ShallowCopy(row.version),
            }
        end

        -- Include table only if it has rows
        if next(rows) ~= nil then
            tables[tableName] = {
                rows = rows
            }
        end
    end

    -- Include tables only if there are any
    if next(tables) ~= nil then
        state.tables = tables
    end

    return state
end

--- Import a database state into the database instance.
--- Merges the imported state with existing data based on version metadata.
--- Validates incoming data against table definitions, skipping invalid entries.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param state LibP2PDB.DBState The database state to import.
function Private:ImportDatabase(dbi, state)
    if not IsNumber(state.clock, 0) then
        return -- invalid clock
    end

    -- Verify if there's any valid tables with rows to import
    local hasRowsToImport = false
    for tableName, tableData in pairs(state.tables or {}) do
        -- Is valid table structure
        if IsNonEmptyString(tableName) and dbi.tables[tableName] and IsNonEmptyTable(tableData) and IsNonEmptyTable(tableData.rows) then
            -- Check for at least one valid row
            for key, row in pairs(tableData.rows) do
                -- Is valid row structure
                if (IsNonEmptyString(key) or IsNumber(key)) and IsTable(row) and IsTableOrNil(row.data) and
                    IsTable(row.version) and IsNumber(row.version.clock, 0) and IsNonEmptyString(row.version.peer) and IsBooleanOrNil(row.version.tombstone) then
                    hasRowsToImport = true
                    break
                end
            end
        end
        if hasRowsToImport then
            break
        end
    end
    if not hasRowsToImport then
        return -- no valid rows to import
    end

    -- Begin import
    Private.isImporting = true

    -- Merge Lamport clock
    dbi.clock = max(dbi.clock, state.clock)

    -- Import database
    for tableName, tableData in pairs(state.tables or {}) do
        local ti = dbi.tables[tableName]
        if ti then -- only import defined tables
            for key, row in pairs(tableData.rows or {}) do
                -- Ignore invalid rows during import
                SafeCall(dbi, Private.ImportRow, Private, dbi, tableName, ti, key, row)
            end
        end
    end

    -- End import
    Private.isImporting = false
end

--- Import a single row into a table instance.
--- Merges the row with existing data based on version metadata.
--- Validates incoming data against table definitions, skipping invalid entries.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param tableName string Name of the table.
--- @param ti LibP2PDB.TableInstance Table instance.
--- @param key LibP2PDB.TableKey Primary key value for the row.
--- @param row LibP2PDB.TableRow Incoming row data with version metadata.
function Private:ImportRow(dbi, tableName, ti, key, row)
    -- Validate key
    assert(type(key) == ti.keyType, "expected key of type '" .. ti.keyType .. "' for table '" .. tableName .. "', but was '" .. type(key) .. "'")

    -- Validate row
    assert(IsNonEmptyTable(row), "row must be a non-empty table")

    -- Determine if the incoming row is newer
    local rowVersion = Private:PrepareRowVersion(row.version)
    local existingRow = ti.rows[key]
    if existingRow and not Private:IsIncomingNewer(existingRow.version, rowVersion) then
        return -- existing row is newer, do not import
    end

    -- Prepare the row data and version
    local rowData = Private:PrepareRowData(tableName, ti, not rowVersion.tombstone and row.data or nil)

    -- Run custom validation if provided
    if rowData and ti.onValidate then
        local success, result = SafeCall(dbi, ti.onValidate, key, rowData)
        if not success then
            return -- validation threw an error
        end
        assert(IsBoolean(result), "onValidate must return a boolean")
        if not result then
            return -- validation failed
        end
    end

    -- Determine if the row will change
    local changes = false
    local existingRow = ti.rows[key]
    if rowData then
        if not existingRow or existingRow.version.tombstone or not ShallowEqual(existingRow.data, rowData) then
            changes = true -- new row or data changes
        end
    else
        if not existingRow or not existingRow.version.tombstone then
            changes = true -- new tombstone row or existing row is not a tombstone
        end
    end

    -- Apply changes if any
    if changes then
        -- Store the row
        ti.rows[key] = {
            data = rowData,
            version = rowVersion,
        }

        -- Invoke row changed callbacks
        Private:InvokeChangeCallbacks(dbi, tableName, ti, key, rowData)
    end
end

--- Determine if incoming version metadata is newer than existing version metadata.
--- @param existing LibP2PDB.TableRowVersion Existing version metadata.
--- @param incoming LibP2PDB.TableRowVersion Incoming version metadata.
--- @return boolean isNewer Returns true if incoming is newer, false otherwise.
function Private:IsIncomingNewer(existing, incoming)
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

--- Serialize the database state to a compact binary string format.
--- If a table has a schema, fields are serialized in alphabetical order without names.
--- If no schema is defined, all fields are serialized in arbitrary order with names.
--- Empty tables are omitted from the serialized output.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @return string serialized The serialized database state.
function Private:SerializeDatabase(dbi)
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
            --- @type LibP2PDB.TableSchemaSorted?
            local schemaSorted = Private:GetTableSchema(tableData, true)

            -- Serialize rows
            for key, row in pairs(rows) do
                -- Add row data
                local data = row.data
                local version = row.version
                local rowArray, rowIdx
                if schemaSorted then
                    -- Schema defined: serialize fields in alphabetical order without names
                    local dataArray = {}
                    local dataIdx = 0
                    if data then
                        for i = 1, #schemaSorted do
                            local fieldName = schemaSorted[i][1]
                            local fieldValue = data[fieldName]
                            dataIdx = dataIdx + 1
                            dataArray[dataIdx] = fieldValue == nil and NIL_MARKER or fieldValue
                        end
                    else
                        for i = 1, #schemaSorted do
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

    -- Serialize the root array to a compact binary string format
    return dbi.serializer:Serialize(rootArray)
end

--- Deserialize an entire database from a compact binary string format.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param str string The serialized database string to deserialize.
--- @return LibP2PDB.DBState? state The deserialized database state on success, or an error message on failure.
function Private:DeserializeDatabase(dbi, str)
    local success, rootArray = dbi.serializer:Deserialize(str)
    if not success or not rootArray then
        ReportError(dbi, "failed to deserialize binary string")
        return
    end
    if not IsNonEmptyTable(rootArray) then
        ReportError(dbi, "root array must be a non-empty table")
        return
    end

    -- Process the array structure into a state that can be imported later
    local clock = rootArray[1]
    if not IsNumber(clock, 0) then
        ReportError(dbi, "clock must be a positive number")
        return
    end

    --- @type LibP2PDB.DBState
    local state = {
        clock = clock,
    }

    -- Parse tables (format: clock, tableArray1, tableArray2, ...)
    local rootLen = #rootArray
    local tables = {}
    for i = 2, rootLen do
        local tableArray = rootArray[i]
        local tableLen = tableArray and #tableArray or 0

        if not IsTable(tableArray) or tableLen < 1 then
            ReportError(dbi, "table array must be a non-empty table")
            return
        end

        local tableName = tableArray[1]
        if not IsNonEmptyString(tableName) then
            ReportError(dbi, "table name must be a non-empty string")
            return
        end

        local ti = dbi.tables[tableName]
        if not ti then
            ReportError(dbi, "table '%s' is not defined in the database", tostring(tableName))
            return
        end

        -- Get sorted schema once for this table
        --- @type LibP2PDB.TableSchemaSorted?
        local schemaSorted = Private:GetTableSchema(ti, true)

        -- Parse rows (format: tableName, rowArray1, rowArray2, ...)
        local rows = {}
        local keyType = ti.keyType
        for rowIndex = 2, tableLen do
            local rowArray = tableArray[rowIndex]
            local rowLen = rowArray and #rowArray or 0
            local row = Private:DeserializeRow(dbi, keyType, tableName, rowArray, rowLen, schemaSorted)
            if row then
                local key = rowArray[1]
                rows[key] = row
            end
        end

        -- Include table only if it has rows
        if next(rows) ~= nil then
            tables[tableName] = {
                rows = rows,
            }
        end
    end

    -- Include tables only if there are any
    if next(tables) ~= nil then
        state.tables = tables
    end

    return state
end

--- Deserialize a single row from an array structure.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param keyType LibP2PDB.TableKeyType Expected key type for the table.
--- @param tableName string Name of the table (for error messages).
--- @param rowArray table The array structure representing the row.
--- @param rowLen number Length of the row array.
--- @param schemaSorted LibP2PDB.TableSchemaSorted? Optional sorted schema for the table.
--- @return LibP2PDB.TableRow? row The deserialized row on success, or an error message on failure.
function Private:DeserializeRow(dbi, keyType, tableName, rowArray, rowLen, schemaSorted)
    if not IsTable(rowArray) or rowLen < 3 then
        ReportError(dbi, "skipping row with invalid structure in table '%s'", tableName)
        return
    end

    -- Validate key type
    local key = rowArray[1]
    if type(key) ~= keyType then
        ReportError(dbi, "skipping row with invalid key type '%s' in table '%s'", tostring(key), tableName)
        return
    end

    local dataArray = rowArray[2]
    local data = {}
    if IsTable(dataArray) then
        if schemaSorted then
            -- Schema defined: map array indices back to field names
            local dataLen = #dataArray
            for fieldIndex = 1, dataLen do
                local fieldValue = dataArray[fieldIndex]
                local fieldInfo = schemaSorted[fieldIndex]

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
    if not IsNumber(versionClock, 0) then
        ReportError(dbi, "skipping row with invalid version clock '%s' in table '%s'", tostring(versionClock), tableName)
        return
    end

    local versionPeer = rowArray[4]
    if not IsNonEmptyString(versionPeer) then
        ReportError(dbi, "skipping row with invalid version peer '%s' in table '%s'", tostring(versionPeer), tableName)
        return
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
        data = not versionTombstone and data or nil,
        version = version,
    }
end

--- Build a digest of the current database state for gossip sync.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @return table digest The built database digest.
function Private:BuildDigest(dbi)
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

--- Send a message to a specific target peer.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param data any The message data to send.
--- @param channel string The channel to send the message on.
--- @param target string? Target peer name, only required for WHISPER channel.
--- @param priority LibP2PDB.CommPriority The priority of the message.
function Private:Send(dbi, data, channel, target, priority)
    local serialized = dbi.serializer:Serialize(data)
    if not serialized then
        Debug("failed to serialize data for prefix '%s'", tostring(dbi.prefix))
        return
    end

    local compressed = dbi.compressor:Compress(serialized)
    if not compressed then
        Debug("failed to compress data for prefix '%s'", tostring(dbi.prefix))
        return
    end

    local encoded = dbi.encoder:EncodeChannel(compressed)
    if not encoded then
        Debug("failed to encode data for prefix '%s'", tostring(dbi.prefix))
        return
    end

    if target then
        Debug("sending %d bytes on prefix '%s' channel '%s' target '%s'", #encoded, tostring(dbi.prefix), tostring(channel), tostring(target))
    else
        Debug("sending %d bytes on prefix '%s' channel '%s'", #encoded, tostring(dbi.prefix), tostring(channel))
    end

    --- @cast priority "ALERT"|"BULK"|"NORMAL"
    AceComm.SendCommMessage(self, dbi.prefix, encoded, channel, target, priority)
end

--- Broadcast a message to all peers on multiple channels.
--- @param dbi LibP2PDB.DBInstance Database instance.
--- @param data any The message data to broadcast.
--- @param channels table<string>? Optional list of additional channels to broadcast on.
--- @param priority LibP2PDB.CommPriority The priority of the message.
function Private:Broadcast(dbi, data, channels, priority)
    local serialized = dbi.serializer:Serialize(data)
    if not serialized then
        Debug("failed to serialize message for prefix '%s'", tostring(dbi.prefix))
        return
    end

    local compressed = dbi.compressor:Compress(serialized)
    if not compressed then
        Debug("failed to compress message for prefix '%s'", tostring(dbi.prefix))
        return
    end

    local encoded = dbi.encoder:EncodeChannel(compressed)
    if not encoded then
        Debug("failed to encode message for prefix '%s'", tostring(dbi.prefix))
        return
    end

    Debug("broadcasting %d bytes on prefix '%s'", #encoded, tostring(dbi.prefix))

    if IsInGuild() then
        --- @cast priority "ALERT"|"BULK"|"NORMAL"
        AceComm.SendCommMessage(self, dbi.prefix, encoded, "GUILD", nil, priority)
    end

    if IsInRaid() then
        --- @cast priority "ALERT"|"BULK"|"NORMAL"
        AceComm.SendCommMessage(self, dbi.prefix, encoded, "RAID", nil, priority)
    elseif IsInGroup() then
        --- @cast priority "ALERT"|"BULK"|"NORMAL"
        AceComm.SendCommMessage(self, dbi.prefix, encoded, "PARTY", nil, priority)
    end

    if not IsInInstance() then
        --- @cast priority "ALERT"|"BULK"|"NORMAL"
        AceComm.SendCommMessage(self, dbi.prefix, encoded, "YELL", nil, priority)
    end

    for _, channel in ipairs(channels or {}) do
        --- @cast priority "ALERT"|"BULK"|"NORMAL"
        AceComm.SendCommMessage(self, dbi.prefix, encoded, channel, nil, priority)
    end
end

--- @class LibP2PDB.Message
--- @field type LibP2PDB.CommMessageType
--- @field peer string
--- @field data any
--- @field dbi LibP2PDB.DBInstance
--- @field channel string
--- @field sender string

--- Handler for received communication messages.
--- @param prefix string The communication prefix.
--- @param encoded string The encoded message data.
--- @param channel string The channel the message was received on.
--- @param sender string The sender of the message.
function Private:OnCommReceived(prefix, encoded, channel, sender)
    -- Ignore messages from self
    if sender == Private.playerName then
        return
    end

    -- Get the database instance for this prefix
    local db = LibP2PDB:GetDatabase(prefix)
    if not db then
        Debug("received message for unknown prefix '%s' from channel '%s' sender '%s'", tostring(prefix), tostring(channel), tostring(sender))
        return
    end

    local dbi = Private.databases[db]
    if not dbi then
        Debug("received message for unregistered database prefix '%s' from channel '%s' sender '%s'", tostring(prefix), tostring(channel), tostring(sender))
        return
    end

    -- Deserialize message
    local compressed = dbi.encoder:DecodeChannel(encoded)
    if not compressed then
        Debug("failed to decode message from prefix '%s' channel '%s' sender '%s'", tostring(prefix), tostring(channel), tostring(sender))
        return
    end

    local serialized = dbi.compressor:Decompress(compressed)
    if not serialized then
        Debug("failed to decompress message from prefix '%s' channel '%s' sender '%s'", tostring(prefix), tostring(channel), tostring(sender))
        return
    end

    local success, obj = dbi.serializer:Deserialize(serialized)
    if not success or not obj then
        Debug("failed to deserialize message from prefix '%s' channel '%s' sender '%s': %s", tostring(prefix), tostring(channel), tostring(sender), Dump(obj))
        return
    end

    -- Validate message structure
    if not IsTable(obj) then
        Debug("received invalid message structure from '%s' on channel '%s'", tostring(sender), tostring(channel))
        return
    end

    if not IsNumber(obj.type) then
        Debug("received message with missing or invalid type from '%s' on channel '%s'", tostring(sender), tostring(channel))
        return
    end

    if not IsNonEmptyString(obj.peer) then
        Debug("received message with missing or invalid peer from '%s' on channel '%s'", tostring(sender), tostring(channel))
        return
    end

    -- Build message object
    --- @type LibP2PDB.Message
    local message = {
        type = obj.type,
        peer = obj.peer,
        data = obj.data,
        dbi = dbi,
        channel = channel,
        sender = sender,
    }

    -- Get or create bucket
    local bucket = dbi.buckets[message.type]
    if not bucket then
        bucket = {}
        dbi.buckets[message.type] = bucket
    end

    -- If we already have a timer running for this peer, ignore it
    if bucket[message.peer] then
        return
    end

    -- Create a timer to process this message after 1 second
    bucket[message.peer] = C_Timer.NewTimer(1.0, function()
        -- Process the message
        Private:DispatchMessage(message)

        -- Clean up
        bucket[message.peer] = nil

        -- Clean up bucket if empty
        if not next(bucket) then
            dbi.buckets[message.type] = nil
        end
    end)
end

--- Dispatch a received message to the appropriate handler.
--- @param message LibP2PDB.Message
function Private:DispatchMessage(message)
    if message.type == CommMessageType.PeerDiscoveryRequest then
        Private:PeerDiscoveryRequestHandler(message)
    elseif message.type == CommMessageType.PeerDiscoveryResponse then
        Private:PeerDiscoveryResponseHandler(message)
    elseif message.type == CommMessageType.SnapshotRequest then
        Private:RequestSnapshotMessageHandler(message)
    elseif message.type == CommMessageType.SnapshotResponse then
        Private:SnapshotMessageHandler(message)
    elseif message.type == CommMessageType.Digest then
        Private:DigestMessageHandler(message)
    elseif message.type == CommMessageType.RequestRows then
        Private:RequestRowsMessageHandler(message)
    elseif message.type == CommMessageType.Rows then
        Private:RowsMessageHandler(message)
    else
        Debug("received unknown message type %d from '%s' on channel '%s'", message.type, tostring(message.sender), tostring(message.channel))
    end
end

--- Handler for peer discovery request messages.
--- @param message LibP2PDB.Message
function Private:PeerDiscoveryRequestHandler(message)
    -- Send peer discovery response
    local obj = {
        type = CommMessageType.PeerDiscoveryResponse,
        peer = Private.peerId,
        data = message.dbi.clock,
    }
    Private:Send(message.dbi, obj, "WHISPER", message.sender, CommPriority.Low)
end

--- Handler for peer discovery response messages.
--- @param message LibP2PDB.Message
function Private:PeerDiscoveryResponseHandler(message)
    local dbi = message.dbi

    -- Update last discovery time
    if dbi.onDiscoveryComplete then
        dbi.lastDiscoveryResponseTime = GetTime()
    end

    -- Lookup peer info
    local peerInfo = dbi.peers[message.peer]
    if not peerInfo then
        -- New peer
        dbi.peers[message.peer] = {
            name = message.sender,
            clock = message.data,
            lastSeen = GetServerTime(),
        }
    else
        -- Update peer info
        peerInfo.clock = message.data
        peerInfo.lastSeen = GetServerTime()
    end
end

--- Handler for snapshot request messages.
--- @param message LibP2PDB.Message
function Private:RequestSnapshotMessageHandler(message)
    local obj = {
        type = CommMessageType.SnapshotResponse,
        peer = Private.peerId,
        data = Private:ExportDatabase(message.dbi),
    }
    Private:Send(message.dbi, obj, "WHISPER", message.sender, CommPriority.Low)
end

--- Handler for snapshot response messages.
--- @param message LibP2PDB.Message
function Private:SnapshotMessageHandler(message)
    Private:ImportDatabase(message.dbi, message.data)
end

--- Handler for digest messages.
--- @param message LibP2PDB.Message
function Private:DigestMessageHandler(message)
    -- Compare digest and build missing table
    local missingTables = {}
    for tableName, incomingTable in pairs(message.data.tables or {}) do
        local ti = message.dbi.tables[tableName]
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
                    if Private:IsIncomingNewer(localRow.version, incomingVersion) then
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
        peer = Private.peerId,
        data = missingTables,
    }
    Private:Send(message.dbi, obj, "WHISPER", message.sender, CommPriority.Normal)
end

--- Handler for request rows messages.
--- @param message LibP2PDB.Message
function Private:RequestRowsMessageHandler(message)
    -- Build rows to send
    local rowsToSend = {}
    for tableName, requestedRows in pairs(message.data or {}) do
        local ti = message.dbi.tables[tableName]
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
        peer = Private.peerId,
        data = rowsToSend,
    }
    Private:Send(message.dbi, obj, "WHISPER", message.sender, CommPriority.Normal)
end

--- Handler for rows messages.
--- @param message LibP2PDB.Message
function Private:RowsMessageHandler(message)
    -- Import received rows
    for incomingTableName, incomingTableData in pairs(message.data or {}) do
        local ti = message.dbi.tables[incomingTableName]
        if ti then
            for incomingKey, incomingRow in pairs(incomingTableData or {}) do
                local importResult, importError = Private:ImportRow(message.dbi, incomingTableName, ti, incomingKey, incomingRow)
                if not importResult then
                    Debug("failed to import row with key '%s' in table '%s' from '%s' on channel '%s': %s", tostring(incomingKey), incomingTableName, tostring(message.sender), tostring(message.channel), tostring(importError))
                end
            end
        end
    end
end

--- OnUpdate handler called periodically to handle time-based events.
--- @param dbi LibP2PDB.DBInstance Database instance.
function Private:OnUpdate(dbi)
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

------------------------------------------------------------------------------------------------------------------------
-- Testing
------------------------------------------------------------------------------------------------------------------------

if DEBUG then
    local Assert = {
        IsNil = function(value, msg) assert(value == nil, msg or "value is not nil") end,
        IsNotNil = function(value, msg) assert(value ~= nil, msg or "value is nil") end,
        IsTrue = function(value, msg) assert(value == true, msg or "value is not true") end,
        IsFalse = function(value, msg) assert(value == false, msg or "value is not false") end,
        IsNumber = function(value, msg) assert(IsNumber(value), msg or "value is not a number") end,
        IsString = function(value, msg) assert(IsString(value), msg or "value is not a string") end,
        IsEmptyString = function(value, msg) assert(IsEmptyString(value), msg or "value is not an empty string") end,
        IsNonEmptyString = function(value, msg) assert(IsNonEmptyString(value), msg or "value is not a non-empty string") end,
        IsTable = function(value, msg) assert(IsTable(value), msg or "value is not a table") end,
        IsEmptyTable = function(value, msg) assert(IsEmptyTable(value), msg or "value is not an empty table") end,
        IsNonEmptyTable = function(value, msg) assert(IsNonEmptyTable(value), msg or "value is not a non-empty table") end,
        IsFunction = function(value, msg) assert(IsFunction(value), msg or "value is not a function") end,
        IsInterface = function(value, interface, msg)
            assert(IsTable(value), msg or "value is not a table")
            for _, fnName in ipairs(interface) do
                assert(IsFunction(value[fnName]), msg or format("value is missing function '%s'", tostring(fnName)))
            end
        end,
        AreEqual = function(actual, expected, msg) assert(DeepEqual(actual, expected) == true, msg or format("values are not equal, expected '%s' but got '%s'", Dump(expected), Dump(actual))) end,
        AreNotEqual = function(actual, expected, msg) assert(DeepEqual(actual, expected) == false, msg or format("values are equal, both are '%s'", Dump(actual))) end,
        Contains = function(haystack, needle, msg)
            if type(haystack) == "string" then
                assert(strfind(haystack, needle, 1, true) ~= nil, msg or format("string does not contain '%s'", tostring(needle)))
            elseif type(haystack) == "table" then
                for _, v in pairs(haystack) do
                    if DeepEqual(v, needle) then
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
                    if DeepEqual(v, needle) then
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
        ExpectErrors = function(func)
            DISABLE_ERROR_REPORTING = true
            func()
            DISABLE_ERROR_REPORTING = false
            assert(LAST_ERROR ~= nil, "expected an error but none was reported")
            LAST_ERROR = nil
        end,
    }

    --- @diagnostic disable: param-type-mismatch, assign-type-mismatch, missing-fields
    local UnitTests = {
        NewDatabase = function()
            do -- check new database creation with minimal description
                Assert.IsNil(LibP2PDB:GetDatabase("LibP2PDBTests1"))
                local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
                Assert.IsEmptyTable(db)
                Assert.AreEqual(LibP2PDB:GetDatabase("LibP2PDBTests1"), db)

                -- verify internal database instance
                local dbi = Private.databases[db]
                Assert.IsNonEmptyTable(dbi)
                Assert.AreEqual(dbi.prefix, "LibP2PDBTests1")
                Assert.AreEqual(dbi.clock, 0)
                Assert.IsNil(dbi.channels)
                Assert.AreEqual(dbi.discoveryQuietPeriod, 1)
                Assert.AreEqual(dbi.discoveryMaxTime, 3)
                Assert.IsInterface(dbi.serializer, { "Serialize", "Deserialize" })
                Assert.IsInterface(dbi.compressor, { "Compress", "Decompress" })
                Assert.IsInterface(dbi.encoder, { "EncodeChannel", "DecodeChannel", "EncodePrint", "DecodePrint" })
                Assert.IsEmptyTable(dbi.peers)
                Assert.IsEmptyTable(dbi.buckets)
                Assert.IsEmptyTable(dbi.tables)
                Assert.IsNil(dbi.onError)
                Assert.IsNil(dbi.onChange)
                Assert.IsNil(dbi.onDiscoveryComplete)
                Assert.IsNil(dbi.discoveryStartTime)
                Assert.IsNil(dbi.lastDiscoveryResponseTime)
                Assert.IsNil(dbi.isInitialDiscoveryComplete)
            end
            do -- check new database creation with full description
                Assert.IsNil(LibP2PDB:GetDatabase("LibP2PDBTests2"))
                local db = LibP2PDB:NewDatabase({
                    prefix = "LibP2PDBTests2",
                    onError = function(dbi, msg) end,
                    channels = { "CUSTOM" },
                    serializer = AceSerializer,
                    compressor = {
                        Compress = function(self, str) return str end,
                        Decompress = function(self, str) return str end
                    },
                    encoder = {
                        EncodeChannel = function(self, str) return str end,
                        DecodeChannel = function(self, str) return str end,
                        EncodePrint = function(self, str) return str end,
                        DecodePrint = function(self, str) return str end
                    },
                    onChange = function(table, key, row) end,
                    discoveryQuietPeriod = 5,
                    discoveryMaxTime = 30,
                    onDiscoveryComplete = function(isInitial) end,
                })
                Assert.IsEmptyTable(db)
                Assert.AreEqual(LibP2PDB:GetDatabase("LibP2PDBTests2"), db)

                -- verify internal database instance
                local dbi = Private.databases[db]
                Assert.IsNonEmptyTable(dbi)
                Assert.AreEqual(dbi.prefix, "LibP2PDBTests2")
                Assert.AreEqual(dbi.clock, 0)
                Assert.AreEqual(dbi.channels, { "CUSTOM" })
                Assert.AreEqual(dbi.discoveryQuietPeriod, 5)
                Assert.AreEqual(dbi.discoveryMaxTime, 30)
                Assert.IsInterface(dbi.serializer, { "Serialize", "Deserialize" })
                Assert.IsInterface(dbi.compressor, { "Compress", "Decompress" })
                Assert.IsInterface(dbi.encoder, { "EncodeChannel", "DecodeChannel", "EncodePrint", "DecodePrint" })
                Assert.IsEmptyTable(dbi.peers)
                Assert.IsEmptyTable(dbi.buckets)
                Assert.IsEmptyTable(dbi.tables)
                Assert.IsFunction(dbi.onError)
                Assert.IsFunction(dbi.onChange)
                Assert.IsFunction(dbi.onDiscoveryComplete)
                Assert.IsNil(dbi.discoveryStartTime)
                Assert.IsNil(dbi.lastDiscoveryResponseTime)
                Assert.IsNil(dbi.isInitialDiscoveryComplete)
            end
        end,

        NewDatabase_DescIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase(nil) end)
            Assert.Throws(function() LibP2PDB:NewDatabase(true) end)
            Assert.Throws(function() LibP2PDB:NewDatabase(false) end)
            Assert.Throws(function() LibP2PDB:NewDatabase("") end)
            Assert.Throws(function() LibP2PDB:NewDatabase("invalid") end)
            Assert.Throws(function() LibP2PDB:NewDatabase(123) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({}) end)
        end,

        NewDatabase_DescPrefixIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = nil }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "this_prefix_is_longer_than_16_characters" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = {} }) end)
        end,

        NewDatabase_DescOnErrorIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onError = {} }) end)
        end,

        NewDatabase_DescChannelsIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = {} }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = { true } }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = { false } }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = { "" } }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = { 123 } }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", channels = { {} } }) end)
        end,

        NewDatabase_DescOnChangeIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = {} }) end)
        end,

        NewDatabase_DescDiscoveryQuietPeriodIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = -1 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryQuietPeriod = {} }) end)
        end,

        NewDatabase_DescDiscoveryMaxTimeIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = -1 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", discoveryMaxTime = {} }) end)
        end,

        NewDatabase_DescOnDiscoveryCompleteIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = true }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = false }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = "" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onDiscoveryComplete = {} }) end)
        end,

        NewDatabase_PrefixAlreadyExists_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.IsEmptyTable(db)
            Assert.Throws(function() LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" }) end)
        end,

        NewDatabase_DefaultSerializerNotFound_Throws = function()
            -- can't test this properly since we do not want to unload libraries
        end,

        NewDatabase_CustomSerializerFails_Throws = function()
            Assert.Throws(function()
                LibP2PDB:NewDatabase({
                    prefix = "LibP2PDBTests",
                    serializer = {
                        Serialize = function(self, value) return tostring(value) end,
                        Deserialize = function(self, str) return true, {} end,
                    },
                })
            end)
        end,

        NewDatabase_DefaultCompressorNotFound_Throws = function()
            -- can't test this properly since we do not want to unload libraries
        end,

        NewDatabase_CustomCompressorFails_Throws = function()
            Assert.Throws(function()
                LibP2PDB:NewDatabase({
                    prefix = "LibP2PDBTests",
                    compressor = {
                        Compress = function(self, str) return str end,
                        Decompress = function(self, str) return nil end,
                    },
                })
            end)
        end,

        NewDatabase_DefaultEncoderNotFound_Throws = function()
            -- can't test this properly since we do not want to unload libraries
        end,

        NewDatabase_CustomEncoderFails_Throws = function()
            Assert.Throws(function()
                LibP2PDB:NewDatabase({
                    prefix = "LibP2PDBTests",
                    encoder = {
                        EncodeChannel = function(self, str) return str end,
                        DecodeChannel = function(self, str) return nil end,
                        EncodePrint = function(self, str) return str end,
                        DecodePrint = function(self, str) return nil end,
                    },
                })
            end)
        end,

        NewDatabase_RegistersCommPrefix = function()
            local db = LibP2PDB:NewDatabase({ prefix = "_LibP2PDBTests_" })
            Assert.IsEmptyTable(db)
            Assert.IsTrue(C_ChatInfo.IsAddonMessagePrefixRegistered("_LibP2PDBTests_"))
        end,

        GetDatabase = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            Assert.IsEmptyTable(db1)

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            Assert.IsEmptyTable(db2)

            local fetched1 = LibP2PDB:GetDatabase("LibP2PDBTests1")
            Assert.AreEqual(fetched1, db1)

            local fetched2 = LibP2PDB:GetDatabase("LibP2PDBTests2")
            Assert.AreEqual(fetched2, db2)

            local fetchedNil = LibP2PDB:GetDatabase("NonExistent")
            Assert.IsNil(fetchedNil)
        end,

        GetDatabase_PrefixIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:GetDatabase(nil) end)
            Assert.Throws(function() LibP2PDB:GetDatabase(true) end)
            Assert.Throws(function() LibP2PDB:GetDatabase(false) end)
            Assert.Throws(function() LibP2PDB:GetDatabase("") end)
            Assert.Throws(function() LibP2PDB:GetDatabase("this_prefix_is_longer_than_16_characters") end)
            Assert.Throws(function() LibP2PDB:GetDatabase(123) end)
            Assert.Throws(function() LibP2PDB:GetDatabase({}) end)
        end,

        NewTable = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.IsEmptyTable(db)
            local dbi = Private.databases[db]
            Assert.IsNonEmptyTable(dbi)

            do -- check new table creation with minimal description
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })

                local ti = dbi.tables["Users1"]
                Assert.IsNonEmptyTable(ti)
                Assert.AreEqual(ti.keyType, "string")
                Assert.IsNil(ti.schema)
                Assert.IsNil(ti.onValidate)
                Assert.IsNil(ti.onChange)
                Assert.IsTable(ti.subscribers)
                Assert.IsEmptyTable(ti.rows)
            end
            do -- check new table creation with full description
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = "number",
                    },
                    onValidate = function(key, row) return true end,
                    onChange = function(key, row) end,
                })

                local ti = dbi.tables["Users2"]
                Assert.IsNonEmptyTable(ti)
                Assert.AreEqual(ti.keyType, "number")
                Assert.AreEqual(ti.schema, { name = "string", age = "number" })
                Assert.IsFunction(ti.onValidate)
                Assert.IsFunction(ti.onChange)
                Assert.IsTable(ti.subscribers)
                Assert.IsEmptyTable(ti.rows)
            end
            do -- check schema allowed types
                local schemaDef = {
                    boolField = "boolean",
                    strField = "string",
                    numField = "number",
                    nilField = "nil", -- not really useful?
                    multiple = { "boolean", "string", "number", "nil" },
                }
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "string",
                    schema = schemaDef,
                })

                local ti = dbi.tables["Users3"]
                Assert.IsNonEmptyTable(ti)
                Assert.AreEqual(ti.schema, schemaDef)
            end
        end,

        NewTable_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:NewTable(nil, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(true, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(false, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable("", { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(123, { name = "Users", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable({}, { name = "Users", keyType = "string" }) end)
        end,

        NewTable_DescIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, nil) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, true) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, false) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, "") end)
            Assert.Throws(function() LibP2PDB:NewTable(db, "invalid") end)
            Assert.Throws(function() LibP2PDB:NewTable(db, 123) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, {}) end)
        end,

        NewTable_DescTableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = nil, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = true, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = false, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "", keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = 123, keyType = "string" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = {}, keyType = "string" }) end)
        end,

        NewTable_DescKeyTypeIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = nil }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = true }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = false }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "boolean" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "nil" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "table" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "function" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "userdata" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "thread" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = {} }) end)
        end,

        NewTable_DescSchemaIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = true }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = false }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = "" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = {} }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = true } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = false } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = "" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = "table" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = "function" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = "userdata" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = "thread" } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = 123 } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = {} } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { true } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { false } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { "" } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { "table" } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { "function" } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { "userdata" } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { "thread" } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { 123 } } }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { a = { {} } } }) end)
        end,

        NewTable_DescOnValidateIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = true }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = false }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = "" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onValidate = {} }) end)
        end,

        NewTable_DescOnChangeIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = true }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = false }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = "" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = "invalid" }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = 123 }) end)
            Assert.Throws(function() LibP2PDB:NewTable(db, { name = "Users", keyType = "string", onChange = {} }) end)
        end,

        NewTable_NameAlreadyExists_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            local tableDesc = { name = "Users", keyType = "string" }
            LibP2PDB:NewTable(db, tableDesc)
            Assert.Throws(function() LibP2PDB:NewTable(db, tableDesc) end)
        end,

        InsertKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user1", { name = "Bob", age = 25, city = "NY" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Bob", age = 25, city = "NY" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user2", { name = "Alice", age = 30, town = "LA" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user2"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users1", "user3", { name = "Eve", age = -1 }))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users1", "user3"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user4", {}))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user4"), {})
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 1, { name = "Bob", age = 25 }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Bob", age = 25 })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 2, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 2), { name = "Alice" })
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users2", 3, { name = "Eve", age = -1 }))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users2", 3))
            end
            do -- check inserting over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end,
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Bob" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Eve" })

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Eve" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Bob" })
            end
        end,

        InsertKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:InsertKey(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(true, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(false, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey("", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey("invalid", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey({}, "Users", 1, { name = "A" }) end)
        end,

        InsertKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:InsertKey(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, true, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, false, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "invalid", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, {}, 1, { name = "A" }) end)
        end,

        InsertKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", 123, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users1", {}, { name = "A" }) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", "invalid", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:InsertKey(db, "Users2", {}, { name = "A" }) end)
            end
        end,

        InsertKey_DataIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, nil) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, true) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, false) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, "") end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, "invalid") end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, 123) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, {}) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, { age = 25 }) end)
        end,

        InsertKey_InvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            -- check inserting a new key invokes all callbacks
            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting the same key again does not invoke any callbacks
            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }) end)
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting over a deleted key invokes all callbacks
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(dbCount, 3)    -- 1 for delete, 1 for insert
            Assert.AreEqual(tableCount, 3) -- 1 for delete, 1 for insert
            Assert.AreEqual(subCount, 3)   -- 1 for delete, 1 for insert
        end,

        SetKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users1", "user1", { name = "Bob", age = 25, city = "NY" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Bob", age = 25, city = "NY" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users1", "user1", { name = "Alice", age = 30, town = "LA" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsFalse(LibP2PDB:SetKey(db, "Users1", "user1", { name = "Eve", age = -1 }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users1", "user1", {}))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), {})
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users2", 1, { name = "Bob", age = 25 }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Bob", age = 25 })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users2", 1, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Alice" })
                Assert.IsFalse(LibP2PDB:SetKey(db, "Users2", 1, { name = "Eve", age = -1 }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Alice" })
            end
            do -- check setting over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 1, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Bob" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 2, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 3, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Eve" })

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 1, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 2, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Eve" })
                Assert.IsTrue(LibP2PDB:SetKey(db, "Users3", 3, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Bob" })
            end
        end,

        SetKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:SetKey(nil, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(true, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(false, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey("", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey("invalid", "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(123, "Users", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey({}, "Users", 1, { name = "A" }) end)
        end,

        SetKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:SetKey(db, nil, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, true, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, false, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "invalid", 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, 123, 1, { name = "A" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, {}, 1, { name = "A" }) end)
        end,

        SetKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", 123, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users1", {}, { name = "A" }) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", "invalid", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:SetKey(db, "Users2", {}, { name = "A" }) end)
            end
        end,

        SetKey_DataIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, nil) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, true) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, false) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, "") end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, "invalid") end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, 123) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, {}) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:SetKey(db, "Users", 1, { age = 25 }) end)
        end,

        SetKey_InvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            -- check inserting a new key invokes all callbacks
            Assert.IsTrue(LibP2PDB:SetKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting the same key again with same data does not invoke any callbacks
            Assert.IsTrue(LibP2PDB:SetKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting over a deleted key invokes all callbacks
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.IsTrue(LibP2PDB:SetKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(dbCount, 3)    -- 1 for delete, 1 for insert
            Assert.AreEqual(tableCount, 3) -- 1 for delete, 1 for insert
            Assert.AreEqual(subCount, 3)   -- 1 for delete, 1 for insert
        end,

        UpdateKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users1", "user1", function(data) return { name = "Bob", age = 25, city = "NY" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Bob", age = 25, city = "NY" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users1", "user1", function(data) return { name = "Alice", age = 30, town = "LA" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsFalse(LibP2PDB:UpdateKey(db, "Users1", "user1", function(data) return { name = "Eve", age = -1 } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users1", "user1", function(data) return {} end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), {})
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users2", 1, function(data) return { name = "Bob", age = 25 } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Bob", age = 25 })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users2", 1, function(data) return { name = "Alice" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Alice" })
                Assert.IsFalse(LibP2PDB:UpdateKey(db, "Users2", 1, function(data) return { name = "Eve", age = -1 } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Alice" })
            end
            do -- check updating over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 1, function(data) return { name = "Bob" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Bob" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 2, function(data) return { name = "Alice" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 3, function(data) return { name = "Eve" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Eve" })

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 1, function(data) return { name = "Alice" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 2, function(data) return { name = "Eve" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Eve" })
                Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users3", 3, function(data) return { name = "Bob" } end))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Bob" })
            end
        end,

        UpdateKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:UpdateKey(nil, "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(true, "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(false, "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey("", "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey("invalid", "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(123, "Users", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey({}, "Users", 1, function(data) return data end) end)
        end,

        UpdateKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:UpdateKey(db, nil, 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, true, 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, false, 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "invalid", 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, 123, 1, function(data) return data end) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, {}, 1, function(data) return data end) end)
        end,

        UpdateKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", 123, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users1", {}, { name = "A" }) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", nil, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", true, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", false, { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", "", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", "invalid", { name = "A" }) end)
                Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users2", {}, { name = "A" }) end)
            end
        end,

        UpdateKey_UpdateFunctionIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, nil) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, true) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, false) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, "") end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, "invalid") end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, 123) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, {}) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, { name = "Bob" }) end)
            Assert.Throws(function() LibP2PDB:UpdateKey(db, "Users", 1, { age = 25 }) end)
        end,

        UpdateKey_InvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            -- check inserting a new key invokes all callbacks
            Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users", 1, function() return { name = "Bob", age = 25 } end))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting the same key again with same data does not invoke any callbacks
            Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users", 1, function() return { name = "Bob", age = 25 } end))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            -- check inserting over a deleted key invokes all callbacks
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.IsTrue(LibP2PDB:UpdateKey(db, "Users", 1, function() return { name = "Bob", age = 25 } end))
            Assert.AreEqual(dbCount, 3)    -- 1 for delete, 1 for insert
            Assert.AreEqual(tableCount, 3) -- 1 for delete, 1 for insert
            Assert.AreEqual(subCount, 3)   -- 1 for delete, 1 for insert
        end,

        UpdateKey_UpdateFunctionData_IsNotModified = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:UpdateKey(db, "Users", 1, function(row)
                row.additionalField = "abc"
                return row
            end)
            local fetchedRow = LibP2PDB:GetKey(db, "Users", 1)
            Assert.AreEqual(fetchedRow, { name = "Bob", age = 25 })
        end,

        DeleteKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user1", { name = "Bob", age = 25, city = "NY" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users1", "user1"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user2", { name = "Alice", age = 30, town = "LA" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users1", "user2"))
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users1", "user3", { name = "Eve", age = -1 }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users1", "user3"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user4", {}))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users1", "user4"))
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 1, { name = "Bob", age = 25 }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users2", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 2, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users2", 2))
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users2", 3, { name = "Eve", age = -1 }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users2", 3))
            end
            do -- check inserting over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end,
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Bob" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Eve" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Eve" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Bob" }))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
            end
        end,

        DeleteKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:DeleteKey(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(true, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(false, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey("invalid", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey({}, "Users", 1) end)
        end,

        DeleteKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:DeleteKey(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, true, 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, false, 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, "", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, "invalid", 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:DeleteKey(db, {}, 1) end)
        end,

        DeleteKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", nil) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", true) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", false) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", "") end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", 123) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users1", {}) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", nil) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", true) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", false) end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", "") end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", "invalid") end)
                Assert.Throws(function() LibP2PDB:DeleteKey(db, "Users2", {}) end)
            end
        end,

        DeleteKey_InvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            -- check deleting a key invokes all callbacks
            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.AreEqual(dbCount, 2)    -- 1 for insert, 1 for delete
            Assert.AreEqual(tableCount, 2) -- 1 for insert, 1 for delete
            Assert.AreEqual(subCount, 2)   -- 1 for insert, 1 for delete

            -- check deleting the same key again does not invoke any callbacks
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.AreEqual(dbCount, 2)
            Assert.AreEqual(tableCount, 2)
            Assert.AreEqual(subCount, 2)

            -- check deleting a non-existent key invokes all callbacks
            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 2))
            Assert.AreEqual(dbCount, 3)
            Assert.AreEqual(tableCount, 3)
            Assert.AreEqual(subCount, 3)
        end,

        HasKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user1", { name = "Bob", age = 25, city = "NY" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users1", "user1"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user2", { name = "Alice", age = 30, town = "LA" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users1", "user2"))
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users1", "user3", { name = "Eve", age = -1 }))
                Assert.IsFalse(LibP2PDB:HasKey(db, "Users1", "user3"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user4", {}))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users1", "user4"))
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 1, { name = "Bob", age = 25 }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users2", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 2, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users2", 2))
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users2", 3, { name = "Eve", age = -1 }))
                Assert.IsFalse(LibP2PDB:HasKey(db, "Users2", 3))
            end
            do -- check inserting over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end,
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Bob" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Eve" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsFalse(LibP2PDB:HasKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsFalse(LibP2PDB:HasKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsFalse(LibP2PDB:HasKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Alice" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Eve" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Bob" }))
                Assert.IsTrue(LibP2PDB:HasKey(db, "Users3", 3))
            end
        end,

        HasKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:HasKey(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(true, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(false, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey("invalid", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey({}, "Users", 1) end)
        end,

        HasKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:HasKey(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, true, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, false, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, "", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, "invalid", 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:HasKey(db, {}, 1) end)
        end,

        HasKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", nil) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", true) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", false) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", "") end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", 123) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users1", {}) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", nil) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", true) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", false) end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", "") end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", "invalid") end)
                Assert.Throws(function() LibP2PDB:HasKey(db, "Users2", {}) end)
            end
        end,

        HasKey_DoesNotInvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.IsTrue(LibP2PDB:HasKey(db, "Users", 1))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }) end)
            Assert.IsTrue(LibP2PDB:HasKey(db, "Users", 1))
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.IsFalse(LibP2PDB:HasKey(db, "Users", 1))
            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.IsTrue(LibP2PDB:HasKey(db, "Users", 1))
            Assert.AreEqual(dbCount, 3)    -- 1 for delete, 1 for insert
            Assert.AreEqual(tableCount, 3) -- 1 for delete, 1 for insert
            Assert.AreEqual(subCount, 3)   -- 1 for delete, 1 for insert
        end,

        GetKey = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- test with string key type and no schema
                LibP2PDB:NewTable(db, {
                    name = "Users1",
                    keyType = "string",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user1", { name = "Bob", age = 25, city = "NY" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user1"), { name = "Bob", age = 25, city = "NY" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user2", { name = "Alice", age = 30, town = "LA" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user2"), { name = "Alice", age = 30, town = "LA" })
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users1", "user3", { name = "Eve", age = -1 }))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users1", "user3"))
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users1", "user4", {}))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users1", "user4"), {})
            end
            do -- test with number key type and schema
                LibP2PDB:NewTable(db, {
                    name = "Users2",
                    keyType = "number",
                    schema = {
                        name = "string",
                        age = {
                            "number",
                            "nil"
                        }
                    },
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 1, { name = "Bob", age = 25 }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 1), { name = "Bob", age = 25 })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users2", 2, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users2", 2), { name = "Alice" })
                Assert.IsFalse(LibP2PDB:InsertKey(db, "Users2", 3, { name = "Eve", age = -1 }))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users2", 3))
            end
            do -- check inserting over deleted keys
                LibP2PDB:NewTable(db, {
                    name = "Users3",
                    keyType = "number",
                    onValidate = function(key, data)
                        return not data or not data.age or data.age >= 0
                    end,
                })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Bob" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Eve" })

                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 1))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 1))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 2))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 2))
                Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users3", 3))
                Assert.IsNil(LibP2PDB:GetKey(db, "Users3", 3))

                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 1, { name = "Alice" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 1), { name = "Alice" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 2, { name = "Eve" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 2), { name = "Eve" })
                Assert.IsTrue(LibP2PDB:InsertKey(db, "Users3", 3, { name = "Bob" }))
                Assert.AreEqual(LibP2PDB:GetKey(db, "Users3", 3), { name = "Bob" })
            end
        end,

        GetKey_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:GetKey(nil, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(true, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(false, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey("", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey("invalid", "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(123, "Users", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey({}, "Users", 1) end)
        end,

        GetKey_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:GetKey(db, nil, 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, true, 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, false, 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, "", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, "invalid", 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, 123, 1) end)
            Assert.Throws(function() LibP2PDB:GetKey(db, {}, 1) end)
        end,

        GetKey_KeyIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            do -- check string key type
                LibP2PDB:NewTable(db, { name = "Users1", keyType = "string" })
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", nil) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", true) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", false) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", "") end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", 123) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users1", {}) end)
            end
            do -- check number key type
                LibP2PDB:NewTable(db, { name = "Users2", keyType = "number" })
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", nil) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", true) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", false) end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", "") end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", "invalid") end)
                Assert.Throws(function() LibP2PDB:GetKey(db, "Users2", {}) end)
            end
        end,

        GetKey_DoesNotInvokeChangeCallbacks = function()
            local dbCount, tableCount, subCount = 0, 0, 0
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests", onChange = function() dbCount = dbCount + 1 end })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", onChange = function() tableCount = tableCount + 1 end })
            LibP2PDB:Subscribe(db, "Users", function() subCount = subCount + 1 end)
            Assert.AreEqual(dbCount, 0)
            Assert.AreEqual(tableCount, 0)
            Assert.AreEqual(subCount, 0)

            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(LibP2PDB:GetKey(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            Assert.Throws(function() LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }) end)
            Assert.AreEqual(LibP2PDB:GetKey(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.AreEqual(dbCount, 1)
            Assert.AreEqual(tableCount, 1)
            Assert.AreEqual(subCount, 1)

            Assert.IsTrue(LibP2PDB:DeleteKey(db, "Users", 1))
            Assert.IsNil(LibP2PDB:GetKey(db, "Users", 1))
            Assert.IsTrue(LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 }))
            Assert.AreEqual(LibP2PDB:GetKey(db, "Users", 1), { name = "Bob", age = 25 })
            Assert.AreEqual(dbCount, 3)    -- 1 for delete, 1 for insert
            Assert.AreEqual(tableCount, 3) -- 1 for delete, 1 for insert
            Assert.AreEqual(subCount, 3)   -- 1 for delete, 1 for insert
        end,

        Version = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string" } })
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob" })

            local version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 1)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsNil(version.tombstone)

            LibP2PDB:UpdateKey(db, "Users", 1, function(row)
                row.name = "Robert"
                return row
            end)
            version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 2)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsNil(version.tombstone)

            LibP2PDB:DeleteKey(db, "Users", 1)
            version = Private.databases[db].tables["Users"].rows[1].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 3)
            Assert.AreEqual(version.peer, Private.peerId)
            Assert.IsTrue(version.tombstone)
        end,

        Version_WhenPeerEqualsKey_PeerValueIsEqualChar = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string" } })

            LibP2PDB:InsertKey(db, "Users", Private.peerId, { name = "Bob" })
            local version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 1)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:SetKey(db, "Users", Private.peerId, { name = "Robert" })
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 2)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:UpdateKey(db, "Users", Private.peerId, function(row)
                row.name = "Alice"
                return row
            end)
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 3)
            Assert.AreEqual(version.peer, "=")
            Assert.IsNil(version.tombstone)

            LibP2PDB:DeleteKey(db, "Users", Private.peerId)
            version = Private.databases[db].tables["Users"].rows[Private.peerId].version
            Assert.IsTable(version)
            Assert.AreEqual(version.clock, 4)
            Assert.AreEqual(version.peer, "=")
            Assert.IsTrue(version.tombstone)
        end,

        Subscribe = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            local callbackInvoked = 0
            local callback = function(key, row)
                callbackInvoked = callbackInvoked + 1
                Assert.AreEqual(key, 1)
                Assert.AreEqual(row, { name = "Bob", age = 25 })
            end
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.AreEqual(callbackInvoked, 1)
        end,

        Subscribe_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Subscribe(nil, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(123, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe("", "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe({}, "Users", function() end) end)
        end,

        Subscribe_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:Subscribe(db, nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, 123, function() end) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, {}, function() end) end)
        end,

        Subscribe_CallbackIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", 123) end)
            Assert.Throws(function() LibP2PDB:Subscribe(db, "Users", "invalid") end)
        end,

        Unsubscribe = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            local callbackInvoked = 0
            local callback = function(key, row)
                callbackInvoked = callbackInvoked + 1
            end
            LibP2PDB:Subscribe(db, "Users", callback)
            LibP2PDB:Unsubscribe(db, "Users", callback)
            LibP2PDB:Unsubscribe(db, "Users", callback)
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 })
            Assert.AreEqual(callbackInvoked, 0)
        end,

        Unsubscribe_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Unsubscribe(nil, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(123, "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe("", "Users", function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe({}, "Users", function() end) end)
        end,

        Unsubscribe_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, nil, function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, 123, function() end) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, {}, function() end) end)
        end,

        Unsubscribe_CallbackIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string" })
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", nil) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", 123) end)
            Assert.Throws(function() LibP2PDB:Unsubscribe(db, "Users", "invalid") end)
        end,

        Export = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db, "Users", 2, { name = "Alice", age = 30 })

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
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db1, "Users", 2, { name = "Alice", age = 30 })
            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            local exported = LibP2PDB:Export(db1)
            LibP2PDB:Import(db2, exported)

            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", 1), LibP2PDB:GetKey(db1, "Users", 1))
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", 2), LibP2PDB:GetKey(db1, "Users", 2))
        end,

        Import_DBIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            local exported = LibP2PDB:Export(db)
            Assert.Throws(function() LibP2PDB:Import(nil, exported) end)
            Assert.Throws(function() LibP2PDB:Import(123, exported) end)
            Assert.Throws(function() LibP2PDB:Import("", exported) end)
            Assert.Throws(function() LibP2PDB:Import({}, exported) end)
        end,

        Import_ExportedIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:Import(db, nil) end)
            Assert.Throws(function() LibP2PDB:Import(db, 123) end)
            Assert.Throws(function() LibP2PDB:Import(db, "invalid") end)
        end,

        Import_SkipInvalidRows = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", 1, { name = "Bob", age = 25 })

            local exported = LibP2PDB:Export(db1)

            -- Corrupt the exported state by adding a row with invalid schema
            exported.tables["Users"].rows[2] = { data = { name = "Alice" }, version = { clock = 2, peer = Private.peerId } }

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            Assert.ExpectErrors(function() LibP2PDB:Import(db2, exported) end)
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", 1), LibP2PDB:GetKey(db1, "Users", 1))
            Assert.IsNil(LibP2PDB:GetKey(db2, "Users", 2))
        end,

        Serialize = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db, "Users", "1", { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db, "Users", "2", { name = "Alice", age = 30 })
            LibP2PDB:DeleteKey(db, "Users", "2")
            LibP2PDB:InsertKey(db, "Users", Private.peerId, { name = "Eve", age = 35 })

            local serialized = LibP2PDB:Serialize(db)
            Assert.IsNonEmptyString(serialized)
        end,

        Serialize_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Serialize(nil) end)
            Assert.Throws(function() LibP2PDB:Serialize(123) end)
            Assert.Throws(function() LibP2PDB:Serialize("") end)
            Assert.Throws(function() LibP2PDB:Serialize({}) end)
        end,

        Deserialize = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number", gender = { "string", "nil" } } })
            LibP2PDB:InsertKey(db1, "Users", "1", { name = "Bob", age = 25, gender = "male" })
            LibP2PDB:InsertKey(db1, "Users", "2", { name = "Alice", age = 30, gender = "female" })
            LibP2PDB:DeleteKey(db1, "Users", "2")
            LibP2PDB:InsertKey(db1, "Users", Private.peerId, { name = "Eve", age = 35, gender = nil })
            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number", gender = { "string", "nil" } } })

            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "1"), { name = "Bob", age = 25, gender = "male" })
            Assert.IsNil(LibP2PDB:GetKey(db2, "Users", "2"))
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", Private.peerId), { name = "Eve", age = 35, gender = nil })
        end,

        Deserialize_InvalidClock_Fails = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Test with invalid binary data
            Assert.ExpectErrors(function() LibP2PDB:Deserialize(db2, "invalid_binary_data") end)
        end,

        Deserialize_InvalidTableStructure_Fails = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Test with malformed serialized data
            Assert.ExpectErrors(function() LibP2PDB:Deserialize(db2, "\1\2\3") end)
        end,

        Deserialize_UndefinedTable_Fails = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Products", keyType = "string", schema = { name = "string" } })
            LibP2PDB:InsertKey(db1, "Products", "1", { name = "Widget" })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Serialize from db1 (has Products table), deserialize to db2 (only has Users table)
            local serialized = LibP2PDB:Serialize(db1)
            Assert.ExpectErrors(function() LibP2PDB:Deserialize(db2, serialized) end)
        end,

        Deserialize_InvalidKeyType_SkipsRow = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "stringkey", { name = "Alice", age = 30 })
            LibP2PDB:InsertKey(db1, "Users", "5", { name = "Bob", age = 25 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })

            -- Serialize from db1 (string keys), deserialize to db2 (expects number keys)
            local serialized = LibP2PDB:Serialize(db1)
            Assert.ExpectErrors(function() LibP2PDB:Deserialize(db2, serialized) end)

            -- String key should be skipped (check internal rows to avoid Get assertion)
            Assert.IsNil(Private.databases[db2].tables["Users"].rows["stringkey"])

            -- Numeric string key should also be skipped (check internal rows)
            Assert.IsNil(Private.databases[db2].tables["Users"].rows[5])
        end,

        Deserialize_InvalidRowStructure_SkipsRow = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionClock_SkipsRow = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidVersionPeer_SkipsRow = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_InvalidFieldType_SkipsRow = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "key1", { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db1, "Users", "key2", { name = "Alice", age = 30 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize should work - schema validation happens during Set, not deserialize
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Both rows should be imported
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key1"), { name = "Bob", age = 25 })
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key2"), { name = "Alice", age = 30 })
        end,

        Deserialize_TombstoneWithoutData_Imports = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "deleted", { name = "Bob", age = 25 })
            LibP2PDB:DeleteKey(db1, "Users", "deleted")

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db2, "Users", "deleted", { name = "OldData", age = 99 })

            -- Serialize tombstone from db1, deserialize to db2
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Row should be deleted
            Assert.IsFalse(LibP2PDB:HasKey(db2, "Users", "deleted"))
        end,

        Deserialize_InvalidBinaryData_Fails = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            Assert.ExpectErrors(function() LibP2PDB:Deserialize(db, "corrupted_data") end)
        end,

        Deserialize_EmptyData_ImportsCorrectly = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db1, "Users", "key", { name = "Bob", age = 25 })

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Users", keyType = "string", schema = { name = "string", age = "number" } })

            -- Normal serialize/deserialize
            local serialized = LibP2PDB:Serialize(db1)
            LibP2PDB:Deserialize(db2, serialized)

            -- Row should be imported
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Users", "key"), { name = "Bob", age = 25 })
        end,

        Deserialize_OptionalFields = function()
            local db1 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests1" })
            LibP2PDB:NewTable(db1, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })
            LibP2PDB:InsertKey(db1, "Config", "theme", { key = "theme", value = "dark" })
            LibP2PDB:InsertKey(db1, "Config", "sound", { key = "sound", value = nil })

            local serialized = LibP2PDB:Serialize(db1)

            local db2 = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests2" })
            LibP2PDB:NewTable(db2, { name = "Config", keyType = "string", schema = { key = "string", value = { "string", "nil" } } })

            LibP2PDB:Deserialize(db2, serialized)
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Config", "theme"), { key = "theme", value = "dark" })
            Assert.AreEqual(LibP2PDB:GetKey(db2, "Config", "sound"), { key = "sound", value = nil })
        end,

        Deserialize_DBIsInvalid_Throws = function()
            Assert.Throws(function() LibP2PDB:Deserialize(nil, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize(123, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize("", "") end)
            Assert.Throws(function() LibP2PDB:Deserialize({}, "") end)
        end,

        Deserialize_SerializedIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:Deserialize(db, nil) end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, 123) end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, "") end)
            Assert.Throws(function() LibP2PDB:Deserialize(db, {}) end)
        end,

        ListTables = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
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
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            LibP2PDB:NewTable(db, { name = "Users", keyType = "number", schema = { name = "string", age = "number" } })
            LibP2PDB:InsertKey(db, "Users", 1, { name = "Bob", age = 25 })
            LibP2PDB:InsertKey(db, "Users", 2, { name = "Alice", age = 30 })
            LibP2PDB:InsertKey(db, "Users", 3, { name = "Eve", age = 35 })
            LibP2PDB:DeleteKey(db, "Users", 2)
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

        ListKeys_TableNameIsInvalid_Throws = function()
            local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
            Assert.Throws(function() LibP2PDB:ListKeys(db, nil) end)
            Assert.Throws(function() LibP2PDB:ListKeys(db, 123) end)
            Assert.Throws(function() LibP2PDB:ListKeys(db, {}) end)
        end,
    }
    --- @diagnostic enable: param-type-mismatch, assign-type-mismatch, missing-fields

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
        local db = LibP2PDB:NewDatabase({ prefix = "LibP2PDBTests" })
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
            LibP2PDB:InsertKey(db, "Players", i, {
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
        -- Store current prefixes and databases
        local originalPrefixes = Private.prefixes
        local originalDatabases = Private.databases

        -- Reset prefixes and databases for isolated test environment
        Private.prefixes = setmetatable({}, { __mode = "v" })
        Private.databases = setmetatable({}, { __mode = "k" })

        -- Run the test
        testFn()

        -- Restore previous prefixes and databases
        Private.prefixes = originalPrefixes
        Private.databases = originalDatabases
    end

    local function RunTests()
        Debug("Running LibP2PDB tests...")
        local startTime = debugprofilestop()
        for _, v in pairs(UnitTests) do
            RunTest(v)
        end
        local endTime = debugprofilestop()
        Debug("All tests %s.", C(Color.Green, "successful"))
        Debug("Ran all tests in %.2f ms.", endTime - startTime)
    end

    local function RunPerformanceTests()
        Debug("Running LibP2PDB performance tests...")
        local startTime = debugprofilestop()
        for _, v in pairs(PerformanceTests) do
            RunTest(v)
        end
        local endTime = debugprofilestop()
        Debug("Ran all performance tests in %.2f ms.", endTime - startTime)
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
