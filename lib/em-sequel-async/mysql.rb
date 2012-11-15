class EmSequelAsync::Mysql
  class ClientPool
    # == Class Methods ======================================================
    
    def self.size=(value)
      @size = value.to_i
      @size = 1 if (@size <= 0)
    end
    
    def self.size
      @size ||= 4
    end
    
    def self.trace?
      !!@trace
    end
    
    def self.trace
      @trace
    end
    
    def self.trace=(value)
      @trace = value
    end
    
    # == Instance Methods ===================================================
    
    def initialize(db)
      @options = {
        :symbolize_keys => true,
        :cast_booleans => true
      }

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
      
      @connection_limit = @options[:connections] || self.class.size
      
      if (EventMachine.reactor_running? and self.class.trace?)
        EventMachine::PeriodicTimer.new(1) do
          dump_file = "#{self.class.trace}.#{@options[:database]}"
          
          File.open(dump_file, 'w') do |f|
            f.puts @options[:database]

            @connections.each do |c, x|
              f.puts "\t#{c.inspect} -> #{x.inspect}"
            end
            @connection_pool.each do |c|
              f.puts "\t#{c.inspect} (pool)"
            end
          
            @query_queue.each do |query, callback|
              f.puts "\t#{query}"
            end
          end
        end
      end
    end
    
    def logger
      @options[:logger]
    end
    
    def log(level, *args)
      @options[:logger] and @options[:logger].send(level, *args)
    end
    
    def add(connection)
      @connections[connection] = nil
      
      if (@query_queue.empty?)
        @connections_active.delete(connection)
        @connection_pool << connection
      else
        self.delegate_query(connection, *@query_queue.pop)
      end
    end

    def delegate_query(connection, query, callback)
      @connections[connection] = [ query, callback ]

      start = Time.now
      deferrable = connection.query(query)
      
      deferrable.callback do |result|
        log(:debug, "(%.6fs) [OK] %s" % [ Time.now - start, query ])
        
        callback.call(result, (Time.now - start).to_f, connection)

        self.add(connection)
      end
      deferrable.errback do |err|
        log(:error, "(%.6fs) [ERR] %s (%s: %s)" % [ Time.now - start, query, err.class, err ])
        log(:error, err.backtrace)

        callback.call(false, (Time.now - start).to_f, connection, err)

        self.add(connection)
      end
    end
    
    def pool_connection
      connection = @connection_pool.pop
      
      if (!connection and @connections.length < @connection_limit)
        connection = Mysql2::EM::Client.new(@options)
      end

      @connections_active[connection] = true
      
      connection
    end

    def query(query, callback = nil, &block)
      callback ||= block

      if (connection = self.pool_connection)
        self.delegate_query(connection, query, callback)
        
        :executing
      else
        @query_queue << [ query, callback ]

        :queued
      end
    end
  end
end
