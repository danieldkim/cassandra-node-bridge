var no_op_logger  = { isDebugEnabled: function() {return false} };
['debug', 'info', 'error', 'warn', 'fatal'].forEach(function(f) { no_op_logger[f] = function() {} }); 

function Request() {};
require('sys').inherits(Request, require('events').EventEmitter);

exports.ConsistencyLevel = ConsistencyLevel = {
  ZERO: 0, ONE: 1, QUORUM: 2, DCQUORUM:3, DCQUORUMSYNC: 4  
}

exports.create = function (port, host, logger) {
  host = host || 'localhost'
  logger = logger || no_op_logger
  function call_proxy(method, arg_hash, event_emitter) {
    var connection = require('net').createConnection(port, host)
    var result_data  = ''
    if (arg_hash) { 
      arg_hash['consistency_level'] = arg_hash['consistency_level'] || ConsistencyLevel.ONE
    }
    for (k in arg_hash) {
      if (typeof arg_hash[k] == "object") {
        arg_hash[k] = JSON.stringify(arg_hash[k])
      }
    }
    
    connection.addListener("timeout", function() {
      event_emitter.emit("error", "Connection to Cassandra server timed out.");
    })

    connection.addListener("close", function(had_error) {
      if (had_error) event_emitter.emit("error", "Error connecting to Cassandra server.");
    })
      
    connection.addListener("connect", function() {
      var method_call = method + "?" + require('querystring').stringify(arg_hash) 
      connection.write(method_call)
    })

    connection.addListener("data", function(data) {
      result_data += data
    })

    connection.addListener("end", function() {
      var error, error_mess, result;
      try {
        var spl = result_data.split('\r\n')
        var status = spl[0]
        if (logger.isDebugEnabled()) logger.debug("[cassandra node client] - status:" + status);
        var body = spl[1]
        if (logger.isDebugEnabled()) logger.debug("[cassandra node client] - body:" + body);
        if (status.match(/^200\b/)) {
          if (body) {
            result = eval('(' + body + ')')
          }
        } else if (status.match(/^500\b/)) {
          error = true;
          error_mess = body;
        } else {
          error = true;
          error_mess = "Could not parse valid status code from response."
        }
      } catch (e) {
        error = true;
        error_mess = e.message;
      } finally {
        connection.end()
      }
      if (error) {
        event_emitter.emit("error", error_mess)
      } else {
        event_emitter.emit("success", result);
      }
    })

  }
  
  return {
    create_request: function(method, argument_hash, event_listeners, send_now) {
      var request = new Request();
      request.send = function() {
        call_proxy(method, argument_hash, this);
      }
      if (event_listeners) {
        for (event in event_listeners) {
          request.addListener(event, event_listeners[event])
        }
        if (typeof send_now == 'undefined') send_now = true;
        if (send_now) request.send();
      }
      return request;
    },
     
/*    get_uuids: function(count, success_func, error_func) {
      call_proxy("get_uuids", {count:count}, success_func, error_func)
    },
    
    get: function(keyspace, key, column_path, consistency_level, success_func, error_func) {
      call_proxy("get", 
                 {keyspace:keyspace, key: key, column_path: column_path, 
                  consistency_level: consistency_level},
                 success_func, error_func)
    },
    
    get_slice : function(keyspace, key, column_parent, predicate, consistency_level, success_func, error_func) {
      call_proxy("get_slice", 
                  {keyspace:keyspace, key: key, column_parent: column_parent, 
                   predicate: predicate, consistency_level: consistency_level},
                 success_func, error_func)
    },    
    
    multiget_slice: function(keyspace, keys, column_parent, predicate, consistency_level, success_func, error_func) {
      call_proxy("multiget_slice", 
                  {keyspace:keyspace, keys: key, column_parent: column_parent, 
                   predicate: predicate, consistency_level: consistency_level},
                  success_func, error_func)
    },

    get_range_slices: function(keyspace, column_parent, predicate, range, consistency_level, success_func, error_func) {
       call_proxy("get_range_slices", 
                   {keyspace:keyspace, column_parent: column_parent, 
                    predicate: predicate, range: range, 
                    consistency_level: consistency_level},
                  success_func, error_func)
     },

    insert: function(keyspace, key, column_path, value, timestamp, consistency_level, success_func, error_func) {
       call_proxy("insert", 
                   {keyspace:keyspace, key: key, column_path: column_path, 
                    value: value, timestamp: timestamp, 
                    consistency_level: consistency_level},
                  success_func, error_func)
     },

    batch_mutate: function(keyspace, mutation_map, consistency_level, success_func, error_func) {
       call_proxy("batch_mutate", 
                   {keyspace:keyspace, mutation_map: mutation_map, 
                    consistency_level: consistency_level},
                  success_func, error_func)
     },
*/    
  }
}
