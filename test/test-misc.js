var assert = require('assert');
var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
//log4js.addAppender(log4js.fileAppender('./' + logger_name + '.log'), logger_name);
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');
var cassandra = require('cassandra-node-client').create(10000, '127.0.0.1', logger)
var sys = require('sys')
var _ = require('underscore')._

var keyspace = "CassandraNodeBridgeTest"
var key1 = "test_key1";
var columns1 = [
  { 
    name: "k1sc1",
    columns: [
      {name: "k1sc1c1", value: "k1sc1c1 value"},
    ]
  },
  {
    name: "k1sc2",
    columns: [
      {name: "k1sc2c1", value: "k1sc2c1 value"},
      {name: "k1sc2c2", value: "k1sc2c2 value"},
    ]    
  }
];

var key2 = "test_key2";
var columns2 = [
  { 
    name: "k2sc1",
    columns: [
      {name: "k2sc1c1", value: "k2sc1c1 value"},
    ]
  },
  {
    name: "k2sc2",
    columns: [
      {name: "k2sc2c1", value: "k2sc2c1 value"},
      {name: "k2sc2c2", value: "k2sc2c2 value"},
    ]    
  }
];
var insert_ts;

var TestSuite = require('async_testing').TestSuite;

var suite = new TestSuite("Miscellaneous tests");
suite.setup(setup);
suite.teardown(teardown);
suite.addTests({
  "Test remove row": test_remove_row,
  "Test remove super column": test_remove_super_column,
  "Test remove column": test_remove_column,
  "Test get_count on super columns": test_get_super_column_count,
  "Test get_count on subcolumns": test_get_subcolumn_count,
  "Test multiget_slice": test_multiget_slice
});

suite.runTests();

function setup(test) {
  function mutations_for_columns(columns) {
    var mutations = [];
    columns.forEach(function(c) {
      var mut = {name: c.name};
      mut.columns = [];
      c.columns.forEach(function(sc) { 
        sc.timestamp = insert_ts;
        mut.columns.push(sc); 
      })
      mutations.push(mut);
    })    
    return mutations;
  }
  insert_ts = Date.now()
  var mut_map = {};
  mut_map[key1] = {}
  mut_map[key2] = {}
  mut_map[key1]["TestSuperColumnFamily_UTF8_UTF8"] = mutations_for_columns(columns1);
  mut_map[key2]["TestSuperColumnFamily_UTF8_UTF8"] = mutations_for_columns(columns2);
  cassandra.batch_mutate(keyspace, mut_map, ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error adding columns: " + err)
      return;
    }
    test();
  });
}

function teardown(clean_up_done) {
  function mutations_for_columns(columns) {
    var mutations = []
    columns.forEach(function(col) {
      var subcolumn_names = _.map(col.columns, function(c) {return c.name})
      mutations.push({timestamp:insert_ts, super_column: col.name,
                      predicate:{column_names:subcolumn_names}})
    })
    return mutations;  
  }
  var mut_map = {}
  mut_map[key1] = {}
  mut_map[key2] = {}
  mut_map[key1]["TestSuperColumnFamily_UTF8_UTF8"] = mutations_for_columns(columns1)
  mut_map[key2]["TestSuperColumnFamily_UTF8_UTF8"] = mutations_for_columns(columns2)

  cassandra.batch_mutate(keyspace, mut_map, ConsistencyLevel.ONE, function(err) {
    if (err) {
      assert.ok(false, "Error trying to clean up:" + err);
    }
    clean_up_done();
  });
}  

function test_remove_row(assert, finished, test) {
  var column_path = column_parent = { 
    column_family: "TestSuperColumnFamily_UTF8_UTF8"
  }
  
  _test_remove(assert, finished, column_path, column_parent)
}

function test_remove_super_column(assert, finished, test) {
  var column_path = column_parent = {
    column_family: "TestSuperColumnFamily_UTF8_UTF8", 
    super_column: columns1[0].name
  }
  _test_remove(assert, finished, column_path, column_parent)
}

function test_remove_column(assert, finished, test) {
  var column_path = {
    column_family: "TestSuperColumnFamily_UTF8_UTF8", 
    super_column: columns1[0].name,
    column: columns1[0].columns[0].name,
  }
  var column_parent = { 
    column_family: "TestSuperColumnFamily_UTF8_UTF8", 
    super_column: columns1[0].name
  }
  _test_remove(assert, finished, column_path, column_parent)
}

function _test_remove(assert, finished, column_path, column_parent) {

  cassandra.remove(keyspace, key1, column_path, Date.now(), 
    ConsistencyLevel.ONE, function(err, result) {

    if (err) {
      assert.ok(false, "Error removing key: " + err)              
      return;
    }
    look_for_it();

  });
  
  function look_for_it() {
    cassandra.get_slice(keyspace, key1, column_parent, 
      {slice_range: {start:'',finish:'',reversed: false,count:100}},
      ConsistencyLevel.ONE, function(err, result) {
      if (err) {
        assert.ok(false, "Error getting slice: " + err)              
        return;
      }
      assert.equal(0, result.length, "Was expecting no results, but got " + result.length);
      logger.info("get_slice returned no results as expected.")
      finished();
    });
  }  
}

function test_get_super_column_count(assert, finished, test) {
  var column_parent =  {column_family:"TestSuperColumnFamily_UTF8_UTF8"};
  cassandra.get_count(keyspace, key1, column_parent, 
    ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error getting super column count: " + err);
      return;
    }
    assert.equal(2, result);
    logger.info("get_count returned correct super column count.")
    finished();
  });
}

function test_get_subcolumn_count(assert, finished, test) {
  var column_parent = { column_family: "TestSuperColumnFamily_UTF8_UTF8", super_column: "k1sc1" }
  cassandra.get_count(keyspace, key1, column_parent, 
    ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error getting subcolumn count: " + err);
      return;
    }
    assert.equal(1, result);
    logger.info("get_count returned correct subcolumn count for sc1.")
    test_sc2();
  });
  
  function test_sc2() {
    var column_parent = { column_family: "TestSuperColumnFamily_UTF8_UTF8", super_column: "k1sc2" }
    cassandra.get_count(keyspace, key1, column_parent,
      ConsistencyLevel.ONE, function(err, result) {
      if (err) {
        assert.ok(false, "Error getting subcolumn count: " + err)              
        return;
      }
      assert.equal(2, result);
      logger.info("get_count return correct subcolumn count for sc2.");
      finished();
    });
  }
}

function test_multiget_slice(assert, finished, test) {
  var column_parent =  {column_family:"TestSuperColumnFamily_UTF8_UTF8"};
  cassandra.multiget_slice(keyspace, [key1, key2], column_parent, 
    {slice_range:{start:'', finish:'', reversed:false, count:5}},
    ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error requesting multiget_slice: " + err);
      return;
    }
    assert.ok(result[key1], "Result did not contain " + key1);
    assert.equal(2, result[key1].length);
    assert.equal(columns1[0].name, result[key1][0].name);
    assert.equal(columns1[1].name, result[key1][1].name);
    assert.ok(result[key2], "Result did not contain " + key2);
    assert.equal(2, result[key2].length);
    assert.equal(columns2[0].name, result[key2][0].name);
    assert.equal(columns2[1].name, result[key2][1].name);
    logger.info("multiget_slice returned correct results.");
    finished();
  });
}

