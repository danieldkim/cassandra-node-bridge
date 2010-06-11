var assert = require('assert');
var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
//log4js.addAppender(log4js.fileAppender('./' + logger_name + '.log'), logger_name);
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');
var cassandra = require('cassandra-node-client').create(10000, '127.0.0.1', logger)
var ConsistencyLevel = require('cassandra-node-client').ConsistencyLevel;

var sys = require('sys')
var _ = require('underscore')._
var test_KS = "CassandraNodeBridgeTest"

var TestSuite = require('async_testing').TestSuite;

var insertTestsSuite = new TestSuite();

insertTestsSuite.teardown(function(teardown_done) {
  var test_col_ts = this.test_col_ts;
  if (!test_col_ts) {
    teardown_done();
    return;
  }
  var mut_map = {test_key: {TestColumnFamily_UTF8:[{timestamp:test_col_ts, predicate:{column_names:["test_col"]}}]}}
  cassandra.batch_mutate(test_KS, mut_map, ConsistencyLevel.ONE, function(err) {
    if (err) assert.ok(false, "Error trying to clean up after test_insert(): " + mess);
    teardown_done();
  })     
});

insertTestsSuite.addTests({
  "Test insert": test_insert, 
});

var columnOrderSuite = new TestSuite();

columnOrderSuite.teardown(function(teardown_done) {
  if (!this.insert_succeeded) {
    teardown_done();
    return;
  }
  var key = this.key;
  var columns_insert_order = this.columns_insert_order;
  var insert_ts = this.insert_ts;
  var column_parent = this.column_parent;
  var mut_map = {}
  mut_map[key] = {}
  var mutations;
  var column_names = [];
  if (columns_insert_order[0].columns) { // super columns
    mutations = []
    columns_insert_order.forEach(function(col) {
      var subcolumn_names = _.map(col.columns, function(c) {return c.name})
      column_names.push(col.name)
      mutations.push({timestamp:insert_ts, super_column: col.name,
                      predicate:{column_names:subcolumn_names}})
    })
  } else {
    var column_names = _.map(columns_insert_order, function(c) {return c.name})
    mutations = [{timestamp:insert_ts, predicate:{column_names:column_names}}]
  }
  mut_map[key][column_parent.column_family] = mutations      
  cassandra.batch_mutate(test_KS, mut_map, ConsistencyLevel.ONE, function(err) {
    if (err) assert.ok(false, "Error trying to clean up after _test_column_order():" + mess);
    teardown_done();
  });
});

columnOrderSuite.addTests({
  "Test column order with TimeUUID column comparisons":  
    test_column_order_TimeUUID, 
  "Test column order with UTF8 column comparisons":  
    test_column_order_UTF8, 
  "Test column order with UTF8 column comparisons, UTF8 subcolumn comparisons":  
    test_column_order_UTF8_UTF8, 
  "Test column order with UTF8 column comparisons, TimeUUID subcolumn comparisons":  
    test_column_order_UTF8_TimeUUID, 
  "Test column order with TimeUUID column comparisons, UTF8 subcolumn comparisons":  
    test_column_order_TimeUUID_UTF8, 
  "Test column order with TimeUUID column comparisons, TimeUUID subcolumn comparisons":  
    test_column_order_TimeUUID_TimeUUID,
  "Test column order with Long column comparisons":  
    test_column_order_TimeUUID, 
  "Test column order with UTF8 column comparisons, Long subcolumn comparisons": 
    test_column_order_UTF8_Long, 
});

require('async_testing').runSuites({
  "Insert tests":  insertTestsSuite,
  "Column order tests":  columnOrderSuite
});

function test_insert(assert, finished, test) {
  cassandra.insert(test_KS, "test_key", 
    {column_family:"TestColumnFamily_UTF8",column:"test_col"}, 
    "test_val", "auto", ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error inserting: " + mess); 
      return;
    }
    cassandra.get(test_KS, "test_key",
    {column_family: "TestColumnFamily_UTF8", column: "test_col"},
    ConsistencyLevel.ONE, function(err, col) {
      if (err) {
        assert.ok(false, "Error getting column: " + mess); 
        return;
      }
      test.test_col_ts = col.timestamp;
      logger.debug("test_col_ts:" + test.test_col_ts)
      assert.equal("test_col", col.name, "Unexpected column name")
      assert.equal("test_val", col.value, "Unexpected column value")
      logger.info("test_insert() successful.")
      finished();
    });

  });
}

function test_column_order_UTF8(assert, finished, test) {  
  var c1 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', value: "c1"},
      c2 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', value: "c2"};
  _test_column_order("test_key", {column_family:"TestColumnFamily_UTF8"}, 
    [c1, c2], [c2, c1], null, assert, finished, test)   
}

function test_column_order_TimeUUID(assert, finished, test) {
  var c1 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', value: "c1"},
      c2 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', value: "c2"};
  _test_column_order("test_key", {column_family:"TestColumnFamily_TimeUUID"}, 
    [c2, c1], [c1, c2], null, assert, finished, test)       
}

function test_column_order_UTF8_UTF8(assert, finished, test) {
  var c1 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', 
            columns: [{name: 'de64c338-3c8a-11df-8951-b1971952eb7e', value: 'c1c1'},
                      {name: '2ae37812-3c8b-11df-88d7-22322cd185d3', value: 'c1c2'}]},
      c2 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', 
            columns: [{name: 'efb4e622-3bc2-11df-8cc1-4a0b67bf84f7', value: 'c2c1'},
                      {name: '91c12e72-3d07-11df-9eb1-30610603492e', value: 'c2c2'}]};
  var subcolumns_expected_orders = {}
  subcolumns_expected_orders[c1.name] = c1.columns.reverse()
  subcolumns_expected_orders[c2.name] = c2.columns.reverse()
  _test_column_order("test_key", {column_family:"TestSuperColumnFamily_UTF8_UTF8"}, 
    [c1, c2], [c2, c1], subcolumns_expected_orders, assert, finished, test)   
}

function test_column_order_UTF8_TimeUUID(assert, finished, test) {
  var c1 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', 
            columns: [{name: '2ae37812-3c8b-11df-88d7-22322cd185d3', value: 'c1c1'},
                      {name: 'de64c338-3c8a-11df-8951-b1971952eb7e', value: 'c1c2'}]},
      c2 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', 
            columns: [{name: '91c12e72-3d07-11df-9eb1-30610603492e', value: 'c2c1'},
                      {name: 'efb4e622-3bc2-11df-8cc1-4a0b67bf84f7', value: 'c2c2'}]};
  var subcolumns_expected_orders = {}
  subcolumns_expected_orders[c1.name] = c1.columns.reverse()
  subcolumns_expected_orders[c2.name] = c2.columns.reverse()
  _test_column_order("test_key", {column_family:"TestSuperColumnFamily_UTF8_TimeUUID"}, 
    [c1, c2], [c2, c1], subcolumns_expected_orders, assert, finished, test)   
}

function test_column_order_TimeUUID_UTF8(assert, finished, test) {
  var c1 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', 
            columns: [{name: 'efb4e622-3bc2-11df-8cc1-4a0b67bf84f7', value: 'c2c1'},
                      {name: '91c12e72-3d07-11df-9eb1-30610603492e', value: 'c2c2'}]},
      c2 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', 
            columns: [{name: 'de64c338-3c8a-11df-8951-b1971952eb7e', value: 'c1c1'},
                      {name: '2ae37812-3c8b-11df-88d7-22322cd185d3', value: 'c1c2'}]};
  var subcolumns_expected_orders = {}
  subcolumns_expected_orders[c1.name] = c1.columns.reverse()
  subcolumns_expected_orders[c2.name] = c2.columns.reverse()
  _test_column_order("test_key", {column_family:"TestSuperColumnFamily_TimeUUID_UTF8"}, 
    [c1, c2], [c2, c1], subcolumns_expected_orders, assert, finished, test)   
}

function test_column_order_TimeUUID_TimeUUID(assert, finished, test) {
  var c1 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', 
            columns: [{name: '91c12e72-3d07-11df-9eb1-30610603492e', value: 'c2c1'},
                      {name: 'efb4e622-3bc2-11df-8cc1-4a0b67bf84f7', value: 'c2c2'}]},
      c2 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', 
            columns: [{name: '2ae37812-3c8b-11df-88d7-22322cd185d3', value: 'c1c1'},
                      {name: 'de64c338-3c8a-11df-8951-b1971952eb7e', value: 'c1c2'}]};
  var subcolumns_expected_orders = {}
  subcolumns_expected_orders[c1.name] = c1.columns.reverse()
  subcolumns_expected_orders[c2.name] = c2.columns.reverse()
  _test_column_order("test_key", {column_family:"TestSuperColumnFamily_TimeUUID_TimeUUID"}, 
    [c1, c2], [c2, c1], subcolumns_expected_orders, assert, finished, test)   
}

function test_column_order_Long(assert, finished, test) {
  var c1 = {name: 1, value: "c1"},
      c2 = {name: 2, value: "c2"};
  _test_column_order("test_key", {column_family:"TestColumnFamily_Long"}, 
    [c2, c1], [c1, c2], null, assert, finished, test)       
}

function test_column_order_UTF8_Long(assert, finished, test) {
  var c1 = {name: 'efb4e334-3bc2-11df-9292-eb7b240ef4c6', 
            columns: [{name: 2, value: 'c1c1'},
                      {name: 1, value: 'c1c2'}]},
      c2 = {name: '0e1daf22-3c72-11df-9629-cb5c46c044d3', 
            columns: [{name: 4, value: 'c2c1'},
                      {name: 3, value: 'c2c2'}]};
  var subcolumns_expected_orders = {}
  subcolumns_expected_orders[c1.name] = c1.columns.reverse()
  subcolumns_expected_orders[c2.name] = c2.columns.reverse()
  _test_column_order("test_key", {column_family:"TestSuperColumnFamily_UTF8_Long"}, 
    [c1, c2], [c2, c1], subcolumns_expected_orders, assert, finished, test)   
}

function _test_column_order(key, column_parent, columns_insert_order, columns_expected_order, subcolumns_expected_orders, assert, finished, test) {
  
  test.key = key;
  test.column_parent = column_parent;
  test.insert_succeeded = false;
  test.insert_ts = insert_ts = Date.now();
  columns_insert_order.forEach(function(c) {
    if (c.columns) {
      c.columns.forEach(function(sc) { sc.timestamp = insert_ts; })
    } else {
      c.timestamp = insert_ts;
    }
  })
  test.columns_insert_order = columns_insert_order;

  var mutations;
  if (column_parent.super_column) {
    mutations = [{name: column_parent.super_column, columns: columns_insert_order}];
  } else {
    mutations = columns_insert_order;
  }
  var mut_map = {}
  mut_map[key] = {}
  mut_map[key][column_parent.column_family] = mutations
  cassandra.batch_mutate(test_KS, mut_map, ConsistencyLevel.ONE, function(err, result) {
    if (err) {
      assert.ok(false, "Error adding columns: " + mess);
      return; 
    }
    assert.equal('number', typeof result, "batch_mutate did not return a number (timestamp)");
    var count = columns_insert_order.length
    cassandra.get_slice(test_KS, key, column_parent, {
      slice_range:{ 
        start:columns_expected_order[0].name, finish:'', 
        reversed:false, count:columns_insert_order.length
      }
    }, ConsistencyLevel.ONE, function(err, result) {
      if (err) {
        assert.ok(false, "Error getting test columns: " + mess);
        return;
      }
      test.insert_succeeded = true;
      assert.equal(columns_expected_order.length, result.length, 
                   "Did not get back expected number of columns.")
      for (var i = 0; i < result.length; i++ ) {
        assert.equal(columns_expected_order[i].name, result[i].name, 
                     "Results not sorted properly, was expecting " + columns_expected_order[i].name)
        if (!subcolumns_expected_orders) continue;
        var subcolumns_expected_order = subcolumns_expected_orders[result[i].name]
        for (var j = 0; j < subcolumns_expected_order.length; j++) {
          assert.equal(subcolumns_expected_order[j].name, result[i].columns[j].name, 
                       "Subcolumn results not sorted properly, was expecting " + subcolumns_expected_order[j].name)
        }
      }
      logger.info("_test_column_order: get_slice() returned uuid's in expected order.");
      finished();
    });
  });
  
}
