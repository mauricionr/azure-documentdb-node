var DocumentDBClient = require('documentdb').DocumentClient
  , config = require('../Shared/config')
  , databaseId = config.names.database
  
var host = config.connection.endpoint;
var masterKey = config.connection.authKey;

// Establish a new instance of the DocumentDBClient to be used throughout this demo
var client = new DocumentDBClient(host, { masterKey: masterKey });

//---------------------------------------------------------------------------------------------------
//
// 1. findDatabaseById  - Attempt to find a database by Id, if found then just complete the sample
// 2. createDatabase    - If the database was not found, try create it
// 3. listDatabases     - Once the database was created, list all the databases on the account
// 4. readDatbase       - Read a database by its _self
// 5. readDatabase      - Read a database by its id (using new ID Based Routing)
// 6. deleteDatabase    - Delete a database given its id
//
//---------------------------------------------------------------------------------------------------

function readDatabaseById(databaseId, callback) {
    client.readDatabase('dbs/' + databaseId, function (err, db) {
        if (err) {
            handleError(err);
        }
        
        callback(db);
    });
}

function readDatabase(database, callback) {
    client.readDatabase(database._self, function (err, db) {
        if (err) {
            handleError(err);
        }

        callback(db);
    });
}

function listDatabases(callback) {
    return client.readDatabases().toArray(function (err, dbs) {
        if (err) {
            handleError(err);
        }

        callback(dbs);
    });
}

function createDatabase(databaseId, callback) {
    var dbdef = {id : databaseId};

    client.createDatabase(dbdef, function (err, createdDatabase) {
        if (err) {
            handleError (err);
        }

        callback(createdDatabase);
    });
}

function deleteDatabase(databaseId, callback) {
    var dbLink = 'dbs/' + databaseId;

    client.deleteDatabase(dbLink, function (err) {
        if (err) {
            handleError(err);
        } else {
            console.log('Database with id \'' + databaseId + '\' deleted.');
            callback();
        }
    });
}

function findDatabaseById(databaseId, callback) {
    var querySpec = {
        query: 'SELECT * FROM root r WHERE  r.id = @id',
        parameters: [
            {
                name: '@id',
                value: databaseId
            }
        ]
    };

    client.queryDatabases(querySpec).toArray(function (err, results) {
        if (err) {
            handleError(err);
        }
        
        if (results.length === 0) {
            // no error occured, but there were no results returned 
            // indicating no database exists matching the query            
            // so, explictly return null
            callback(null, null);
        } else {
            // we found a database, so return it
            callback(null, results[0]);
        }
    });
};

function handleError(error) {}