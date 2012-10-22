class EmSequelAsync::Mysql
  # NOTE: Watcher and Client are largely cloned from the mysql2 gem, but
  #       adapted for Pigeonrocket
  
  module Query
    def initialize(client, sql, callback, &block)
      @client = client
      @sql = sql
      @callback = callback || block
      
      @start_time = Time.now
    end

    def notify_readable
      detach

      @client.logger(:debug, "(%.6fs) [C] %s" % [ Time.now - @start_time, @sql ])
      
      result = 
        begin
          @client.async_result
        rescue => e
          e
        end

      @callback.call(result, Time.now - @start_time, @client)
      
      @client.ready!
    end
  end

  class Client < ::Mysql2::Client
    attr_accessor :pool
    
    def ready!
      @pool and @pool.add(self)
    end
    
    def logger(type, message)
      @pool and @pool.logger.send(type, message)
    end

    def query(sql, options = { })
      if (EventMachine.reactor_running? and block_given?)
        super(sql, options.merge(:async => true))

        logger(:debug, "(...) [?] %s" % [ @sql ])

        EventMachine.watch(self.socket, EmSequelAsync::Mysql::Query, self, sql, Proc.new).notify_readable = true
      else
        start_time = Time.now

        result = super(sql, options)
        
        logger(:debug, "(%.6fs) [F] %s" % [ Time.now - start_time, sql ])
        
        result
      end
    end
  end
  
  class ClientPool
    def self.size=(value)
      @size = value.to_i
      @size = 1 if (@default_size <= 0)
    end
    
    def self.size
      @size ||= 4
    end
    
    def initialize(db)
      @options = { }

      db.opts.each do |key, value|
        case (key)
        when :database, :username, :password, :host, :port, :socket, :encoding, :charset, :compress, :timeout
          @options[key] = value
        when :user
          @options[:username] = value
        when :loggers
          if (value and !value.empty?)
            @options[:logging] = true
            @options[:logger] = value.first
          end
        end
      end

      @query_queue = [ ]

      @connections = { }
      @connection_pool = [ ]
      @connections_active = { }
    end
    
    def logger
      @options[:logger]
    end
    
    def add(connection)
      @connections[connection] = true
      
      if (@query_queue.empty?)
        @connections_active.delete(connection)
        @connection_pool << connection
      else
        query = @query_queue.pop
        
        connection.query(query[0], &query[1])
      end
    end
    
    def pool_connection
      connection = @connection_pool.pop

      if (!connection and @connections.length < self.class.size)
        connection = EmSequelAsync::Mysql::Client.new(@options)
      end

      @connections_active[connection] = Time.now
      
      connection
    end

    def query(query, callback = nil, &block)
      callback ||= block

      if (connection = self.pool_connection)
        connection.query(query, &callback)

        :executing
      else
        @query_queue << [ query, callback ]

        :queued
      end
    end
  end
end
