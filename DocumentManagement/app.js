var DocumentDBClient = require('documentdb').DocumentClient
  , config = require('../Shared/config')
  , fs = require('fs')
  , async = require('async')
  , databaseId = config.names.database
  , collectionId = config.names.collection
  , dbLink
  , collLink;

var host = config.connection.endpoint;
var masterKey = config.connection.authKey;

var documentDefinitions = function () {
    var data = fs.readFileSync('../Shared/Data/Families.json');   
    return JSON.parse(data).Families;
};

// Establish a new instance of the DocumentDBClient to be used throughout this demo
var client = new DocumentDBClient( host, { masterKey: masterKey });

//NOTE: 
//when using the new IDBased Routing URIs, instead of the _self, as we 're doing in this sample
//ensure that the URI does not end with a trailing '/' character
//so dbs/databaseId instead of dbs/databaseId/

//-----------------------------------------------------------------------------------------
// 1. createDocuments   - Insert some documents in to collection
// 2. listDocuments     - Read the document feed for a collection
// 3. readDocument      - Read a single document by its id
// 4. queryDocuments     - Query for documents by some property
// 5. replaceDocument   - Update some properties and replace the document
// 6. deleteDocument    - Given a document id, delete it
//------------------------------------------------------------------------------------------

function insertDocuments(collLink, callback) {
    var createdList = [];
    async.each(
        documentDefinitions(), 
        
        function iterator(documentDefinition, cb) {
            client.createDocument(collLink, documentDefinition, function (err, document) {
                if (err) {
                    console.log(err);

                } else {
                    console.log('created ' + document.id);
                    createdList.push(document);
                    cb();
                }
            });
        },

        function (err) {
            console.log('iterating done ' + createdList.length);
            callback(createdList);
        }
    );
}

function listDocuments(collLink, callback) {
    return client.readDocuments(collLink).toArray(function (err, docs) {
        if (err) {
            handleError(err);
        } else {
            console.log(docs.length + ' Documents found');
            callback(docs);
        }
    });
}

function readDocument(docLink, callback) {
    client.readDocument(docLink, function (err, doc) {
        if (err) {
            handleError(err);
        
        } else {
            console.log('Document \'' + docLink + '\' found');
            callback(doc);
        }
    });
}

function deleteDatabase(dbLink) {
    client.deleteDatabase(dbLink, function (err) {
        if (err) {
            handleError(err);
        }
    });
}

function getOrCreateCollection(dbLink, id, callback) {
    //we're using queryCollections here and not readCollection
    //readCollection will throw an exception if resource is not found
    //queryCollections will not, it will return empty resultset. 
    
    //the collection we create here just uses default IndexPolicy, default OfferType. 
    //for more on IndexPolicy refer to the IndexManagement samples
    //for more on OfferTye refer to CollectionManagement samples

    var querySpec = {
        query: 'SELECT * FROM root r WHERE r.id=@id',
        parameters: [
            {
                name: '@id',
                value: id
            }
        ]
    };
    
    client.queryCollections(dbLink, querySpec).toArray(function (err, results) {
        if (err) {
            handleError(err);
            
        //collection not found, create it
        } else if (results.length === 0) {
            var collDef = { id: id };
            
            client.createCollection(dbLink, collDef, function (err, created) {
                if (err) {
                    handleError(err);
                
                } else {                    
                    callback(created);
                }
            });
        
        //collection found, return it
        } else {
            callback(results[0]);
        }
    });
}

function getOrCreateDatabase(id, callback) {
    //we're using queryDatabases here and not readDatabase
    //readDatabase will throw an exception if resource is not found
    //queryDatabases will not, it will return empty resultset. 
    
    var querySpec = {
        query: 'SELECT * FROM root r WHERE r.id=@id',
        parameters: [
            {
                name: '@id',
                value: id
            }
        ]
    };
    
    client.queryDatabases(querySpec).toArray(function (err, results) {
        if (err) {
            handleError(err);

        //database not found, create it
        } else if (results.length === 0) {
            var databaseDef = { id: id };
            
            client.createDatabase(databaseDef, function (err, created) {
                if (err) {
                    handleError(err);
                
                } else {                    
                    callback(created);
                }
            });
        
        //database found, return it
        } else {
            callback(results[0]);
        }
    });
}

function handleError(error) {
    console.log('\nAn error with code \'' + error.code + '\' has occurred:');
    console.log('\t' + JSON.parse(error.body).message);
    
    finish();
}

function finish() {
    deleteDatabase(dbLink);
    console.log('\nEnd of demo.');
}