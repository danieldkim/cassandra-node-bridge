# Cassandra-Node Bridge

Cassandra-Node Bridge provides a link between 
[Apache Cassandra](http://cassandra.apache.org/) and  
[Node.js](http://nodejs.org/).  Access cassandra from Node.js through an
asynchronous Javascript interface.  

Since there is no Javascript binding for 
[Thrift](http://incubator.apache.org/thrift/), which is what Cassandra uses
for client access, and I didn't feel like writing a low-level thrift client in
Javascript, CNB uses a proxy implemented on 
[EventMachine](http://rubyeventmachine.com/) which uses the Ruby thrift client.

## Requirements

* Ruby 1.8.7
* EventMachine 0.12.10
* Thrift Ruby Software Library (the "thrift" Ruby gem) 0.2.0 
* [JSON implementation for Ruby](http://flori.github.com/json/) 1.2.0
* [simple_uuid](http://github.com/ryanking/simple_uuid)  0.0.2
* Apache Cassandra 0.6.1
* Node.js >0.1.91

## Usage

### Running the proxy - cassandra_proxy.rb

It is recommended that you run the proxy on a 1.8.7 ruby interpreter. 1.8.6 has
a memory leak in the String class.  1.9 uses native threads  which have more 
overhead than the green threads in 1.8.7.  Since the proxy isn't doing much 
more than shuffling data back and forth between the client and the cassandra 
server most of the execution time will be spent waiting on I/O, so green
threads work best.

Also, note that since Ruby's green threads are not native threads, you'll
want to run multiple instances of cassandra_proxy if you want to take
advantage of multiple cores.

The --help option shows what options are available:

<pre>
$ ruby cassandra_proxy.rb --help
Usage: ruby cassandra_proxy.rb [options]
    -h, --host HOST                  Cassandra server host
    -p, --port PORT                  Cassandra server port
    -t, --threadpool-size SIZE       size of threadpool (maximum concurrency)
    -v, --verbose                    verbose (print request/response data)
</pre>

Note the default thread size of 20 -- which is simply EventMachine's default 
thread size -- is VERY LOW.  You should set this to a much higher number in
a production setting.

### The Javascript client - cassandra-node-client.js

To use the javascript client, just include cassandra-node-client.js in your 
NODE_PATH, require it, and initialize a client and the consistency level 
constants like so:

    var cassandra = require('cassandra-node-client').create(10000, '127.0.0.1', logger)
    var ConsistencyLevel = require('cassandra-node-client').ConsistencyLevel
    
The first two parameters are the port and host that the proxy is running on.
The third parameter is an optional logger object from
[log4js-node](http://github.com/csausdev/log4js-node).
  
The client provides an asynchronous Javascript version of the 
[Cassandra API](http://wiki.apache.org/cassandra/API06).

#### Duck typing

CNB does uses plain objects to represent the Cassandra API Structures.  To
simplify things a bit, in places where Cassandra uses aggregation to work
around Thrift's lack of inheritance, structures are *collapsed* and 
[duck typing](http://en.wikipedia.org/wiki/Duck_typing) is used to 
differentiate between different kinds of structures.

For example, where a **ColumnOrSuperColumn** would appear in the API CNB 
expects/returns an object that looks like either a **Column** *or* a 
**SuperColumn**.  Going further, instead of a **Mutation**, CNB expects/returns
an object that looks like a **Deletion**, **Column**, or **SuperColumn**.

#### Requests -- hash-based argument passing and event listeners

To make a request with CNB, create a request object for the relevant API method
with the relevant arguments.  CNB accepts method arguments in a hash rather
than using positional parameters:

    var request = cassandra.create_request("get_slice", {
      keyspace: "MyKeyspace", key: "my_key", 
      column_parent: {column_family:"MyColumnFamily"}, 
      predicate: {column_names:["col1", "col2"]}, 
      consistency_level: ConsistencyLevel.QUORUM
    })

After creating the request, add listeners for the "success" and "error" events:

    request.addListener("success", function(result) {
      result.forEach(function(col) {
        sys.puts("Got column " + column.name + ":" + column.value);
      })
    })
    request.addListener("error", function(mess) {
      sys.puts("Error from cassandra-node-client: " + mess)
    })
    
The success callback receives the result of the request as its only argument.  
The error callback receives a string error message.

Finally, send the the request:

    request.send();

#### Consistency levels

Every cassandra API method takes an optional <code>consistency_level</code> 
parameter.  To use the consistency level constants, initialize them like so:

     var ConsistencyLevel = require('cassandra-node-client').ConsistencyLevel
    
Then you can use <code>ConsistencyLevel.ONE</code>, 
<code>ConsistencyLevel.QUORUM</code>, etc.  The argument defaults to 
<code>ConsistencyLevel.ONE</code> for all methods.

#### UUID's and Longs

CNB supports the use of **TimeUUIDType** and **LongType** for column comparisons.  

If **TimeUUIDType** is specified in the 
[storage configuration](http://wiki.apache.org/cassandra/StorageConfiguration)
for a column, CNB will automatically deserialize the string representation of a
UUID it receives before storing it in Cassandra.  Conversely, it will 
automatically serialize the UUID's it pulls out of cassandra before sending
them to the client.  This all happens behind the scenes; all interaction with
the client uses the string representation.  As a convenience, CNB also provides
a <code>get\_uuids</code> method which will generate time-based UUID's for you
to use in your application.

If **LongType** is specified for a column, Javascript numbers can be used for
the column name.  CNB takes care of marshalling the number into a Java long
(network byte order) before storing it in Cassandra, and unmarshalling it
after pulling it out and before sending it to the client.

#### "auto" timestamps

Timestamps can be specified as "auto".  An auto-generated timestamp will be set
to the number of microseconds since the epoch at the time the request is 
processed by the proxy.  All timestamps specified as "auto" within a single 
request will resolve to the same value.

If you're using auto-generated timestamps **anywhere** in your application, you
should use them consistently **everywhere**.  The auto-generated timestamps have
a **microsecond** level precision.  If you mix timestamps that you generate in
Javascript with a millisecond precision, you'll get some unexpected results.
For example, if you issue a <code>remove</code> call with an auto-generated
timestamp, then subsequently issue a batch insert with a Javascript-generated
timestamp, you will find that your inserts do not persist. 

#### Supported Client Methods

The following [Cassandra API](http://wiki.apache.org/cassandra/API06) methods
are supported by CNB:

* get (a "not_found" event is emitted if the column cannot be found.)
* get_slice
* multiget_slice
* get_count
* get\_range\_slices
* insert
* batch_mutate
* remove

Additionally, a <code>get\_uuids</code> method can be used to generate UUID's.
<code>get\_uuids</code> takes an optional **count** argument (defaults to 1)
specifying how many uuids to generate and returns them in a list.
 
#### Examples

**get_slice**:

    var request = cassandra.create_request("get_slice", {
      keyspace: "MyKeyspace", key: "my_key", 
      column_parent: {column_family:"MyColumnFamily"}, 
      predicate: {column_names:["col1", "col2"]}, 
      consistency_level: ConsistencyLevel.ONE
    })
    request.addListener("success", function(result) {
      result.forEach(function(col) {
        sys.puts("Got column " + column.name + ":" + column.value);
      })      
    })

**get_uuids**:

    var request = cassandra.create_request("get_uuids")
    request.addListener("success", function(result) {
      sys.puts("Got uuid: " + result[0]);
    })


**batch_mutate**:

    var request = cassandra.create_request("batch_mutate", {
      keyspace: "MyKeyspace", 
      mutation_map: {
        "my_key": {
          "MyColumnFamily": [ 
            {
              name: "super_col1", columns: [
                {name: "col1", value: "col1 value", timestamp: "auto"},
                {name: "col2", value: "col2 value", timestamp: "auto"}
              ]
            },
            {
              name: "super_col2", columns: [
                {name: "col3", value: "col3 value", timestamp: "auto"},
                {name: "col4", value: "col4 value", timestamp: "auto"}
              ]
            },            
          ]
        }
      }      
    })
    request.addListener("success", function(result) {
      sys.puts("Update succeeded. auto-generated timestamp: " + result);
    })

    
For more code examples check out the code in test/test-column-order.js and test/test-misc.js.

## Author

Daniel Kim  
danieldkimster@gmail.com  
http://github.com/danieldkim

## License

Copyright (c) 2010 Daniel Kim

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE.
