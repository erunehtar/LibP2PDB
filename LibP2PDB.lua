local MAJOR, MINOR = "LibP2PDB", 1
assert(LibStub, MAJOR .. " requires LibStub")

local LibP2PDB = LibStub:NewLibrary(MAJOR, MINOR)
if not LibP2PDB then return end -- no upgrade needed

local ChatThrottleLib = LibStub("ChatThrottleLib")
assert(ChatThrottleLib, MAJOR .. " requires ChatThrottleLib")

------------------------------------------------------------
-- Public API: Database Instance Creation
------------------------------------------------------------

-- Create a new distributed database instance
function LibP2PDB:New(options)
    -- options.clusterId (string)
    -- options.namespace (string)
    -- options.channels (table, optional)
    -- options.onRowChange(table, key, row, meta) (function, optional)
end

------------------------------------------------------------
-- Public API: Table Definition (Schema)
------------------------------------------------------------

-- Define a typed table inside the DB
function LibP2PDB:DefineTable(db, table, definition)
    -- definition.keyType (string)
    -- definition.schema (table)
    -- definition.onValidate(key, row) (function, optional)
end

------------------------------------------------------------
-- Public API: CRUD Operations
------------------------------------------------------------

-- Insert a new row
function LibP2PDB:Insert(db, table, key, row)
end

-- Read a row
function LibP2PDB:Get(db, table, key)
end

-- Update an existing row using a function(oldRow) -> newRow
function LibP2PDB:Update(db, table, key, updateFn)
end

-- Create or replace a row
function LibP2PDB:Set(db, table, key, row)
end

-- Delete a row (tombstone)
function LibP2PDB:Delete(db, table, key)
end

------------------------------------------------------------
-- Public API: Subscriptions
------------------------------------------------------------

-- Subscribe to changes in a table
function LibP2PDB:Subscribe(db, table, callback)
end

-- Unsubscribe from table changes
function LibP2PDB:Unsubscribe(db, table, callback)
end

------------------------------------------------------------
-- Public API: Persistence (SavedVariables)
------------------------------------------------------------

-- Export the entire DB state as a serializable table
function LibP2PDB:Export(db)
end

-- Load DB state from SavedVariables
function LibP2PDB:Import(db, savedTable)
end

------------------------------------------------------------
-- Public API: Sync / Gossip Controls
------------------------------------------------------------

-- Request a full snapshot from a specific peer
function LibP2PDB:RequestSnapshot(db, target)
end

-- Force a digest broadcast
function LibP2PDB:ForceDigest(db)
end

-- Force a full sync cycle
function LibP2PDB:ForceSync(db)
end

------------------------------------------------------------
-- Public API: Utility / Metadata
------------------------------------------------------------

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

------------------------------------------------------------
-- Public API: Access Control
------------------------------------------------------------

-- Set a write policy function(table, key, row, meta) -> true/false
function LibP2PDB:SetWritePolicy(db, policyFn)
end
