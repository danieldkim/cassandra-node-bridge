$LOAD_PATH << "./lib"
require 'rubygems'
require 'eventmachine'
require 'cassandra'
require 'cassandra_constants'
require 'cassandra_types'
require 'cgi'
require 'json'
require 'optparse'
require 'simple_uuid'

include CassandraThrift

MAX_LONG_VALUE = 9223372036854775807
WE_ARE_BIG_ENDIAN = [1].pack("s") == [1].pack("n")

module CassandraProxyHelper
  def process_timestamp(ts, time_for_auto=Time.now)
    if ts == 'auto'
      time_for_auto.to_i * 1_000_000 + time_for_auto.usec
    else
      ts.to_i
    end
  end

  def to_java_long(val)
    num = val.to_i
    raise "Max long value exceeded." if num > MAX_LONG_VALUE
    long  = [num].pack("Q")
    WE_ARE_BIG_ENDIAN ? long : long.reverse
  end
  
  def from_java_long(val)
    WE_ARE_BIG_ENDIAN ? val.unpack("Q").first : val.reverse.unpack("Q").first
  end
  
  def massage_for_cassandra!(*args)
    marshal_longs! *args
    deserialize_uuids! *args
    return self
  end

  def massage_for_json!(*args)
    unmarshal_longs! *args
    serialize_uuids! *args
    return self
  end
  
  module_function :process_timestamp
end

class ColumnPath
  include CassandraProxyHelper
  
  def self.from_hash(cp)
    args = {
      :column_family => cp['column_family'], :super_column => cp['super_column'],
      :column => cp['column']
    }
    ColumnPath.new args
  end
    
  def marshal_longs!(keyspace_conf)
    if keyspace_conf[column_family]['CompareWith'].match /Long/
      if super_column
        self.super_column = to_java_long(super_column)
      elsif column
        self.column = to_java_long(column)
      end
    end
    
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (comp_subcolumns_with and comp_subcolumns_with.match /Long/ ) 
      self.column = to_java_long(column)
    end
  end
  
  def deserialize_uuids!(keyspace_conf)
    if keyspace_conf[column_family]['CompareWith'].match /UUID/
      if super_column
        self.super_column = UUID.new(super_column).to_s
      elsif column
        self.column = UUID.new(column).to_s
      end
    end
    
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (comp_subcolumns_with and comp_subcolumns_with.match /UUID/ ) 
      self.column = UUID.new(column).to_s
    end
    
    return self
  end
end

class ColumnParent
  include CassandraProxyHelper
  
  def self.from_hash(cf)
    ColumnParent.new :column_family => cf['column_family'], 
                     :super_column => cf['super_column']
  end

  def marshal_longs!(keyspace_conf)
    return self unless super_column
    if keyspace_conf[column_family]['CompareWith'].match /Long/
      self.super_column = to_java_long(super_column)
    end
    return self
  end
  
  def deserialize_uuids!(keyspace_conf)
    return self unless super_column
    if keyspace_conf[column_family]['CompareWith'].match /UUID/
      self.super_column = UUID.new(super_column).to_s
    end
    return self
  end
end

class SlicePredicate
  include CassandraProxyHelper
  
  def self.from_hash(sp)
    args = {}
    if sp['column_names']
      args[:column_names] = sp['column_names']
    elsif sp['slice_range']
      args[:slice_range] = SliceRange.from_hash(sp['slice_range'])
    else 
      raise "Slice predicate needs either column_names or slice_range."
    end
    SlicePredicate.new args
  end
  
  def marshal_longs!(keyspace_conf, column_family, super_column)
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (super_column and comp_subcolumns_with and 
        comp_subcolumns_with.match /Long/ ) or
       (not super_column and
        keyspace_conf[column_family]['CompareWith'].match /Long/)
    
      if self.column_names
        self.column_names = self.column_names.map do |col| 
                              to_java_long(col) 
                            end
      end
  
      if slice_range
        self.slice_range.start = to_java_long(slice_range.start)
        self.slice_range.finish = to_java_long(slice_range.finish)  
      end
  
    end
    return self
  end  
  
  def deserialize_uuids!(keyspace_conf, column_family, super_column)
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (super_column and comp_subcolumns_with and 
        comp_subcolumns_with.match /UUID/ ) or
       (not super_column and
        keyspace_conf[column_family]['CompareWith'].match /UUID/)
    
      if self.column_names
        self.column_names = self.column_names.map do |col| 
                              UUID.new(col).to_s 
                            end
      end
  
      if slice_range
        if slice_range.start and slice_range.start.length > 0
          self.slice_range.start = UUID.new(slice_range.start).to_s
        end  
        if slice_range.finish and slice_range.finish.length > 0
          self.slice_range.finish = UUID.new(slice_range.finish).to_s  
        end
      end
  
    end
    return self
  end  
end

class SliceRange
  def self.from_hash(sr)
    SliceRange.new :start => sr['start'], :finish => sr['finish'], 
                   :reversed => sr['reversed'], :count => sr['count']    
  end
end

class KeyRange
  def self.from_hash(kr)
    KeyRange.new :start_key => kr['start_key'], :end_key => kr['end_key'],
                 :start_token => kr['start_token'], :end_token => kr['end_token'],
                 :count => kr['count']    
  end
end

class Column
  include CassandraProxyHelper

  def self.from_hash(c, time_for_auto=Time.now) 
    timestamp = CassandraProxyHelper.process_timestamp c['timestamp'], time_for_auto
    Column.new :name => c['name'], :value => c['value'].to_s, :timestamp => timestamp
  end
  
  def marshal_longs!(keyspace_conf, column_family, super_column)
    long_columns = keyspace_conf[column_family]['CompareWith'].match /Long/
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    long_subcolumns = comp_subcolumns_with.match /Long/ if comp_subcolumns_with  
    if (not super_column and long_columns) or
      (super_column and long_subcolumns)
      self.name = to_java_long(name)
    end    
    return self
  end
      
  def deserialize_uuids!(keyspace_conf, column_family, super_column)
    uuid_columns = keyspace_conf[column_family]['CompareWith'].match /UUID/
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    uuid_subcolumns = comp_subcolumns_with.match /UUID/ if comp_subcolumns_with  
    if (not super_column and uuid_columns) or
      (super_column and uuid_subcolumns)
      self.name = UUID.new(name).to_s
    end
    return self
  end

  def unmarshal_longs!(keyspace_conf, column_family, super_column)
    long_columns = keyspace_conf[column_family]['CompareWith'].match /Long/
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    long_subcolumns = comp_subcolumns_with.match /Long/ if comp_subcolumns_with  
    if (not super_column and long_columns) or
      (super_column and long_subcolumns)
      self.name = from_java_long(name)
    end     
    return self
  end
    
  def serialize_uuids!(keyspace_conf, column_family, super_column)
    uuid_columns = keyspace_conf[column_family]['CompareWith'].match /UUID/
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    uuid_subcolumns = comp_subcolumns_with.match /UUID/ if comp_subcolumns_with  
    if (not super_column and uuid_columns) or
      (super_column and uuid_subcolumns)
      self.name = UUID.new(name).to_guid
    end    
    return self
  end
  
end

class SuperColumn
  include CassandraProxyHelper

  def marshal_longs!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /Long/
      self.name = to_java_long(name)
    end
    self.columns.each do |column| 
      column.marshal_longs!(keyspace_conf, column_family, true) 
    end
    return self
  end
      
  def deserialize_uuids!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /UUID/
      self.name = UUID.new(name).to_s
    end
    self.columns.each do |column| 
      column.deserialize_uuids!(keyspace_conf, column_family, true) 
    end
    return self
  end

  def unmarshal_longs!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /Long/
      self.name = from_java_long(name)
    end
    self.columns.each do |column| 
      column.unmarshal_longs!(keyspace_conf, column_family, true) 
    end
    return self
  end
    
  def serialize_uuids!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /UUID/
      self.name = UUID.new(name).to_guid
    end
    self.columns.each do |column| 
      column.serialize_uuids!(keyspace_conf, column_family, true) 
    end
    return self
  end
end

class ColumnOrSuperColumn
  include CassandraProxyHelper

  def marshal_longs!(keyspace_conf, column_family, super_column_name=nil)
    if column 
      column.marshal_longs! keyspace_conf, column_family, super_column_name
    elsif super_column
      super_column.marshal_longs! keyspace_conf, column_family
    else
      throw "Must have column or super column"
    end
    return self
  end
      
  def deserialize_uuids!(keyspace_conf, column_family, super_column_name=nil)
    if column 
      column.deserialize_uuids! keyspace_conf, column_family, super_column_name
    elsif super_column
      super_column.deserialize_uuids! keyspace_conf, column_family
    else
      throw "Must have column or super column"
    end
    return self
  end

  def unmarshal_longs!(keyspace_conf, column_family, super_column_name=nil)
    if column 
      column.unmarshal_longs! keyspace_conf, column_family, super_column_name
    elsif super_column
      super_column.unmarshal_longs! keyspace_conf, column_family
    else
      throw "Must have column or super column"
    end
    return self
  end
    
  def serialize_uuids!(keyspace_conf, column_family, super_column_name=nil)
    if column 
      column.serialize_uuids! keyspace_conf, column_family, super_column_name
    elsif super_column
      super_column.serialize_uuids! keyspace_conf, column_family
    else
      throw "Must have column or super column"
    end
    return self
  end
  
end

class Deletion
  include CassandraProxyHelper

  def marshal_longs!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /Long/
      if super_column
        self.super_column = to_java_long(super_column)
      else 
        predicate.marshal_longs! keyspace_conf, column_family, super_column
      end
    end
    
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (comp_subcolumns_with and comp_subcolumns_with.match /Long/ )
      predicate.marshal_longs! keyspace_conf, column_family, super_column
    end
    
    return self
  end
 
  def deserialize_uuids!(keyspace_conf, column_family)
    if keyspace_conf[column_family]['CompareWith'].match /UUID/
      if super_column
        self.super_column = UUID.new(super_column).to_s
      else 
        predicate.deserialize_uuids! keyspace_conf, column_family, super_column
      end
    end
    
    comp_subcolumns_with = keyspace_conf[column_family]['CompareSubcolumnsWith']
    if (comp_subcolumns_with and comp_subcolumns_with.match /UUID/ )
      predicate.deserialize_uuids! keyspace_conf, column_family, super_column
    end
    
    return self
  end
end

class Mutation
  include CassandraProxyHelper
  
  def self.from_hash(mut, time_for_auto=Time.now)
    if mut['name']
      if mut['value']
        c = Column.from_hash mut, time_for_auto
        cosc = ColumnOrSuperColumn.new :column => c
      elsif mut['columns']
        cols = mut['columns'].map do |col| Column.from_hash col end
        sc = SuperColumn.new :name => mut['name'], :columns => cols
        cosc = ColumnOrSuperColumn.new :super_column => sc
      else
        raise "Malformed mutation - 'name' field, but no 'value' or 'columns' field."
      end
      Mutation.new :column_or_supercolumn => cosc
    elsif mut['timestamp'] or mut['predicate']
      args = {}
      args[:timestamp] = CassandraProxyHelper.process_timestamp(mut['timestamp'], time_for_auto) if mut['timestamp']
      args[:super_column] = mut['super_column'] if mut['super_column']
      args[:predicate] = SlicePredicate.from_hash(mut['predicate']) if mut['predicate']
      Mutation.new :deletion => Deletion.new(args)
    else
      raise "Malformed mutation - no 'name', 'timestamp', or 'predicate' field."
    end
  end
  
  def marshal_longs!(keyspace_conf, column_family)
    if column_or_supercolumn
      column_or_supercolumn.marshal_longs! keyspace_conf, column_family
    elsif deletion
      deletion.marshal_longs! keyspace_conf, column_family
    end
    return self
  end
  
  def deserialize_uuids!(keyspace_conf, column_family)
    if column_or_supercolumn
      column_or_supercolumn.deserialize_uuids! keyspace_conf, column_family
    elsif deletion
      deletion.deserialize_uuids! keyspace_conf, column_family
    end
    return self
  end
end


# add a from_json method to each of these classes that just parses some json and
# calls from_hash on the result 
[ ColumnPath, ColumnParent, SlicePredicate, SliceRange, KeyRange, Mutation].each do |c|
  c.module_eval %q{ 
    def self.from_json(json) 
      from_hash JSON.parse(json) 
    end 
  }
end

module Hashifier
  # recursively turns all contained cassandra objects into hashes
  def hashify(o)
    if o.instance_of? Hash
      o.each { |k,v| o[k] = hashify v }
      return o
    elsif o.instance_of? Array
      return o.map { |e| hashify e }
    elsif o.respond_to? "to_hash"
      return o.to_hash
    elsif o.respond_to? "cass_thrift_fields"
      h = {}
      o.cass_thrift_fields.each_value { |v| 
        field_val = hashify o.send(v[:name])
        h[v[:name]] = field_val if field_val
      }
      return h
    else
      return o
    end
  end
  
  module_function :hashify
end

# for each of these classes, add cass_thrift_fields method which exposes 
# private FIELDS data to Hashifier
[ KeySlice, SuperColumn, Column ].each do |c|
  c.module_eval %q{ def cass_thrift_fields() FIELDS end }
end

# override to_hash to collapse into either a column or super column
class ColumnOrSuperColumn
  def to_hash
    if column
      Hashifier.hashify column
    else
      Hashifier.hashify super_column
    end
  end
end

class CassandraProxy < EventMachine::Connection
    
  def receive_data(data)
    op = proc {
      puts "received data: #{data}" if @@verbose
      begin
        method, query_str = data.split /[?$]/
        if query_str
          query_str.chomp!
          args = CGI::parse query_str 
        end
        self.class.send(method.chomp, args)
      rescue => e
        # send error to callback as result
        "#{e}\n#{e.backtrace.join("\n")}" + 
          (e.class == InvalidRequestException ? "\n#{e.why}" : "")
      end
    }
    cb = proc { |result|
      begin
        if result.class == String
          puts "returning error: #{result}" if @@verbose
          send_data "500 Internal Server Error\r\n#{result}"
        else
          if result
            json_result = Hashifier.hashify(result).to_json 
          end
          puts "returning result: #{json_result}" if @@verbose
          send_data "200 OK\r\n"
          send_data json_result 
        end
      rescue => e
        puts "returning error: #{e}" if @@verbose
        send_data "500 Internal Server Error\r\n#{e}\n#{e.backtrace.join("\n")}\r\n"
      ensure
        close_connection_after_writing
      end
    }
    EM.defer(op, cb)
  end

  def self.check_args(args, required_keys) 
    required_keys.each do |key| 
      raise "#{key} argument required." unless args[key].first
      if key == "keyspace" 
        raise "Invalid keyspace: #{args[keyspace]}" unless @@keyspaces[args[key].first]
      end
    end
  end
  
  def self.check_column_family(keyspace, column_family) 
    unless @@keyspaces[keyspace] and @@keyspaces[keyspace][column_family]
      raise "Invalid column family: #{keyspace}.#{column_family}" 
    end
  end
  
  def self.get_uuids(args)
    count = 1
    count = args['count'].first.to_i if args and args['count']
    uuids = []
    count.times do uuids << UUID.new.to_guid end
    uuids 
  end
  
  def self.get(args)
    check_args(args, ['keyspace', 'key', 'column_path', 'consistency_level'])
    keyspace = args['keyspace'].first 
    key = args['key'].first
    column_path = ColumnPath.from_json(args['column_path'].first)
    check_column_family(keyspace, column_path.column_family)    
    column_path.massage_for_cassandra! @@keyspaces[keyspace]
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      cosc = client.get(keyspace, key, column_path, consistency_level)
      cosc.massage_for_json! @@keyspaces[keyspace], column_path.column_family, column_path.super_column
    end
  end
  
  def self.get_slice(args)
    check_args(args, ['keyspace', 'key', 'column_parent', 'predicate', 'consistency_level'])
    keyspace = args['keyspace'].first 
    key = args['key'].first
    column_parent = ColumnParent.from_json(args['column_parent'].first)
    check_column_family(keyspace, column_parent.column_family)
    column_parent.massage_for_cassandra! @@keyspaces[keyspace]
    predicate = SlicePredicate.from_json(args['predicate'].first)
    predicate.massage_for_cassandra! @@keyspaces[keyspace], column_parent.column_family, 
                                 column_parent.super_column
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      results = client.get_slice keyspace, key, column_parent, predicate, consistency_level
      results.each do |cosc| 
        cosc.massage_for_json! @@keyspaces[keyspace], column_parent.column_family, column_parent.super_column 
      end
      results
    end
  end
  
  def self.multiget_slice(args)
    check_args(args, ['keyspace', 'keys', 'column_parent', 'predicate', 'consistency_level'])
    keyspace = args['keyspace'].first 
    keys = JSON.parse(args['keys'].first)
    column_parent = ColumnParent.from_json(args['column_parent'].first)
    check_column_family(keyspace, column_parent.column_family)
    column_parent.massage_for_cassandra! @@keyspaces[keyspace]
    predicate = SlicePredicate.from_json(args['predicate'].first)
    predicate.massage_for_cassandra! @@keyspaces[keyspace], column_parent.column_family, 
                                 column_parent.super_column
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      results = client.multiget_slice keyspace, keys, column_parent, predicate, consistency_level
      results.each do |k, v| 
        v.each do |cosc| 
          cosc.massage_for_json! @@keyspaces[keyspace], column_parent.column_family, column_parent.super_column
        end
      end
      results
    end
  end
  
  def self.get_count(args)
    check_args(args, ['keyspace', 'key', 'column_parent', 'consistency_level'])
    keyspace = args['keyspace'].first 
    key = args['key'].first
    column_parent = ColumnParent.from_json(args['column_parent'].first)
    check_column_family(keyspace, column_parent.column_family)
    column_parent.massage_for_cassandra! @@keyspaces[keyspace]
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      count = client.get_count keyspace, key, column_parent, consistency_level
    end
  end
  
  def self.get_range_slices(args)
    check_args(args, ['keyspace', 'column_parent', 'predicate', 'range', 'consistency_level'])
    keyspace = args['keyspace'].first 
    column_parent = ColumnParent.from_json(args['column_parent'].first)
    check_column_family(keyspace, column_parent.column_family)
    column_parent.massage_for_cassandra! @@keyspaces[keyspace]
    predicate = SlicePredicate.from_json(args['predicate'].first)
    predicate.massage_for_cassandra! @@keyspaces[keyspace], column_parent.column_family, 
                                 column_parent.super_column
    range = KeyRange.from_json(args['range'].first)
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      results = client.get_range_slices keyspace, column_parent, predicate, range, consistency_level
      results.each do |keyslice|
        keyslice.columns.each do |cosc| 
          cosc.massage_for_json! @@keyspaces[keyspace], column_parent.column_family, column_parent.super_column
        end
      end
      results
    end
  end
  
  def self.insert(args)
    check_args(args, ['keyspace', 'key', 'column_path', 'timestamp', 'value', 'consistency_level'])
    keyspace = args['keyspace'].first 
    key = args['key'].first
    column_path = ColumnPath.from_json(args['column_path'].first)
    check_column_family(keyspace, column_path.column_family)
    column_path.massage_for_cassandra! @@keyspaces[keyspace]
    timestamp = CassandraProxyHelper.process_timestamp(args['timestamp'].first)
    value = args['value'].first
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      client.insert keyspace, key, column_path, value, timestamp, consistency_level
    end
    timestamp
  end

  def self.batch_mutate(args)
    check_args(args, ['keyspace', 'mutation_map', 'consistency_level'])
    keyspace = args['keyspace'].first 
    mutation_map = JSON.parse args['mutation_map'].first
    time_for_auto = Time.now
    mutation_map.each_value do |sub_map| 
      sub_map.each do |k, v| 
        check_column_family(keyspace, k)
        sub_map[k] = v.map do |mut_hash| 
          mut = Mutation.from_hash(mut_hash, time_for_auto) 
          mut.massage_for_cassandra! @@keyspaces[keyspace], k 
        end
      end
    end
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      client.batch_mutate keyspace, mutation_map, consistency_level 
    end
    CassandraProxyHelper.process_timestamp("auto", time_for_auto)
  end

  def self.remove(args)
    keyspace = args['keyspace'].first 
    key = args['key'].first
    column_path = ColumnPath.from_json(args['column_path'].first)
    check_column_family(keyspace, column_path.column_family)
    timestamp = CassandraProxyHelper.process_timestamp(args['timestamp'].first)
    puts "Timestamp for remove: #{timestamp}" if @@verbose
    consistency_level = args['consistency_level'].first.to_i
    execute_query do |client| 
      client.remove keyspace, key, column_path, timestamp, consistency_level 
    end    
  end
  
  def self.describe_keyspace(args) 
    keyspace = args['keyspace'].first
    if @@keyspaces[keyspace]
      @@keyspaces[keyspace]
    else
      raise "Could not find keyspace #{keyspace}."
    end
  end
  
  def self.execute_query    
    transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@@cassandra_host, @@cassandra_port))
    client = Cassandra::Client.new(Thrift::BinaryProtocol.new(transport)) 
    begin 
      transport.open
      yield client
    ensure
      transport.close
    end
  end
  
  @@cassandra_host = "127.0.0.1"
  @@cassandra_port = 9160
  @@proxy_port = 10000
  def self.proxy_port
    @@proxy_port
  end
  @@keyspaces = {}
  @@verbose = false
  
  OptionParser.new do |opts|
    opts.banner = "Usage: ruby cassandra_proxy.rb [options]"
    opts.on("-h", "--host HOST", "Cassandra server host") do |h|
      @@cassandra_host = h
    end
    opts.on("-p", "--port PORT", "Cassandra server port") do |p|
      @@cassandra_port = p
    end
    opts.on("-x", "--proxy_port PORT", "Cassandra proxy server port") do |x|
      @@proxy_port = x
    end
    opts.on("-t", "--threadpool-size SIZE", "size of threadpool (maximum concurrency)") do |size|
      EventMachine.threadpool_size = size.to_i
    end
    opts.on("-v", "--verbose", "verbose (print request/response data)") do |v|
      @@verbose = v
    end
  end.parse!

  execute_query do |client|
    client.describe_keyspaces.each do |ks|
      @@keyspaces[ks] = {}
    end
  end
  
  @@keyspaces.each_key do |ks|
    execute_query do |client|
      @@keyspaces[ks] = client.describe_keyspace ks
    end
  end
end

EM.run {
  EM.start_server "0.0.0.0", CassandraProxy.proxy_port, CassandraProxy
  puts "Cassandra proxy started."
}


