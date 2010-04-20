var sys = require('sys');

var logger;

exports.set_logger = function(a_logger) {
  logger = a_logger;
}

exports.run_sync_test = function run_sync_test(test_name, test) {
  try {
    test();
    logger.info('[' + test_name + "] - PASSED.")
  } catch (e) {
    if (e.stack) {
      logger.info('[' + test_name + "] - FAILED:\n" + e.stack);
    } else {
      throw e;
    }
  }
}

exports.run_async_tests_sequentially = function run_async_tests_sequentially(tests, setup) {  

  var statuses = []
  for ( var i = 1; i <= tests.length; i++) {
    statuses[i] = "waiting"
  }
  statuses[0] = "done"

  function create_done_func(status_idx, test_name) {
    return function(succeeded) {
      statuses[status_idx] = "done"
      logger.info((succeeded ? "--- PASSED ---> " : " --- FAILED! --->") + test_name)
    }
  }
  
  var interval_id = setInterval(
    function() {
      // logger.debug("statuses: " + sys.inspect(statuses))
      if ( statuses[statuses.length-1] == "done") {
        clearInterval(interval_id);
        logger.debug("All tests done.")
        return;
      }
      for ( var i = 1; i <= tests.length; i++) {
        if (statuses[i-1] == "done" && statuses[i] == "waiting" ) {
          statuses[i] = "running"
          test_name = tests[i-1][0]
          test_func = tests[i-1][1]
          try {
            logger.info("Starting " + test_name + "...");
            if (setup) {
              var done_func = create_done_func(i, test_name);
              setup(function() {
                test_func(done_func);                
              })
            } else {
              test_func(create_done_func(i, test_name));              
            }
          } catch (e) {
            statuses[i] = "done";
            logger.error(test_name + " had exception: " + (e.stack ? e.stack : e));
          }
        }
      }
    }, 1);
}

exports.clean_up_wrapper_factory = function clean_up_wrapper_factory(clean_up_func, test_done) {
  return {
    clean_up_after: function(wrapped) {
      return function() {
        var e_holder;
        var test_succeeded = true;
        try {
          wrapped.apply(this, arguments)
        } catch(e) {
          test_succeeded = false;
          logger.error("clean_up_after caught exception from " + wrapped.name + 
                       ": " + (e.stack ? e.stack : e))
        } finally {
          try { 
            clean_up_func(function() {
              test_done(test_succeeded);
            })
          } catch (e) {
            logger.error("clean_up_after caught exception while cleaning up for " +
                          wrapped.name + ": " + (e.stack ? e.stack : e))
          }
        }
      }
    },
    clean_up_on_exception_for: function(wrapped) {
      return function() {
        try {
          wrapped.apply(this, arguments)
        } catch(e) {
          logger.error("clean_up_on_exception_for caught exception from " + 
                        wrapped.name + ": " + (e.stack ? e.stack : e))
          try { 
            clean_up_func(function() {
              test_done(false);
            })
          } catch (clnp_e) {
            logger.error("clean_up_on_exception_for caught exception while cleaning up for " + 
                          wrapped.name + ": " + (clnp_e.stack ? clnp_e.stack : clnp_e))
          }
        } 
      }
    }
  }
}

