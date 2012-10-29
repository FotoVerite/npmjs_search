var registry = new (require('cushion').Connection)('localhost', 15984);
var elasticSearch = require('elasticsearchclient');
var npmjsSearch = exports;

npmjsSearch.registry_data = {};
npmjsSearch.host = 'localhost';
npmjsSearch.port = 9200;
npmjsSearch.serverOptions = {
  host: npmjsSearch.host,
  port: npmjsSearch.port
};
npmjsSearch.esl = null;

npmjsSearch.setElasticSearchHost = function(options) {
    npmjsSearch.host = options.host || npmjsSearch.host;
    npmjsSearch.port = options.port || npmjsSearch.port;
};

//*Connectors

npmjsSearch.connectESL = function() {
  if(npmjsSearch.esl === null) {
    npmjsSearch.esl = new elasticSearch(npmjsSearch.serverOptions);
  }
};

npmjsSearch.queryDatabase = function(callback) {
  console.log('Querying Information: please wait one moment');
  registry.request({
    'method': 'GET',
    'path': 'registry/_all_docs?include_docs=true',
    'callback': function(error, response, headers) {
      if(error) {
        console.log(error);
      }
      else{
        callback(response);
      }
    }
  });
};

npmjsSearch.indexAll = function(data) {
  var commands = [];
  data.rows.forEach( function(row) {
    commands.push({ "index" : { "_index" :'registry', "_type" : "npm_module", "_id": row.doc.name} });
    var hash = {};
    hash.rev = row.value.rev;
    hash.name = row.doc.name;
    if(row.doc.users === undefined){
      hash.stars = 0;
    }
    else {
      hash.stars = row.doc.users.length;
    }
    if(typeof row.doc.author === "object") {
      hash.author = row.doc.author.name;
    }
    else {
      hash.author = row.doc.author;
    }
    hash.description = row.doc.description;
    hash.readme = row.doc.readme;
    hash.keywords = row.doc.keywords;
    commands.push(hash);
  });
  npmjsSearch.esl.bulk(commands, {})
            .on('data', function(data) { console.log(data); })
            .exec();
};

npmjsSearch.couchdbReindex = function(index, type) {
  npmjsSearch.connectESL();
  npmjsSearch.implode('_river');
  npmjsSearch.implode(index);
  npmjsSearch.createRegistry(index, type);
};

npmjsSearch.implode = function(index) {
  npmjsSearch.connectESL();
  npmjsSearch.esl.createCall({path: index, method:"DELETE"}, info.serverOptions)
  .on('data', function(data) { console.log(data); })
  .exec();
};

npmjsSearch.createRegistry = function(index, type) {
  npmjsSearch.connectESL();
  npmjsSearch.esl.createIndex(index, {}).on('data', function(data) { console.log(data);
    npmjsSearch.setRegistryMaps(index, type, npmjsSearch.createRiver);
  }).exec();
};

npmjsSearch.createRiver = function(index_name, type) {
  var path = "_river/" + index_name + "_river/_meta";
  var options = {
    "type" : "couchdb",
    "couchdb" : {
        "host" : "localhost",
        "port" : 15984,
        "db" : "registry",
        "filter" : null,
        "ignore_attachments":true,
        "script": "if(ctx.doc.versions){ctx.doc.versions = null};"
      },
     "index" : {
        "index" : index_name,
        "type" : type,
        "bulk_size" : "100",
        "bulk_timeout" : "10ms"
    }
  };
  npmjsSearch.esl.createCall({data: options, path: path, method:"PUT"}, info.serverOptions)
  .on('data', function(data) { console.log(data); })
  .exec();
};

npmjsSearch.setRegistryMaps = function(index, type, cb) {
  var mappings = {};
  mappings[type] = {};
  mappings[type].properties = {};
  var maps = mappings[type].properties;
  maps.name = {"type" : "multi_field",
                "fields": {
                  "name": {"type": "string", "index": "not_analyzed", "boost": 5} ,
                  "autocomplete" : {"type" : "string", "index" : "analyzed"}
                }
              };
  maps.author = {"type" : "multi_field",
                "fields": {
                  "author": {"type": "string", "index": "not_analyzed", "boost": 2} ,
                  "autocomplete" : {"type" : "string", "index" : "analyzed"}
                }
              };
  maps.description = {"type" : "string"};
  maps.readme = {"type" : "string", "boost": 0.2};
  maps.keywords = {"type" : "string", "index_name" : "keywords"};
  npmjsSearch.esl.putMapping(index, type, mappings).on('data', function(data) {
      console.log(data);
      cb(index, type);
   })
.exec();
};

//* Search
npmjsSearch.searchRegistry = function(query, cb){
    npmjsSearch.connectESL();
    npmjsSearch.esl.search('npm', 'module', query).on(
      'data',
      function(data) {
        return cb(data);
    }).exec();
};

npmjsSearch.autoComplete = function(query, cb) {
  npmjsSearch.searchRegistry(
    {
      "fields": ['name', 'description'],
      query: {
        "field" : { "*.autocomplete" : (query +"*") }
      }
    },
    cb
  );
};