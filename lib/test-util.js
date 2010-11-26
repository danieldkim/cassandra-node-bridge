var async_testing = require('async_testing')

exports.run = function(filename, suite) {
  if (_.include(process.ARGV, '--debug'))
    setTimeout(function() {
      async_testing.runSuite(suite);
    }, 15000);
  else
    return async_testing.run(filename, process.ARGV);  
}
