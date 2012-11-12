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
      @client.logger(:debug, "(%.6fs) [C] %s" % [ Time.now - @start_time, @sql ])
      
      result = 
        begin
          @client.async_result.to_a
        rescue => e
          e
        end
        
      puts "#{@client.object_id} RESU> #{result.inspect} #{@callback.inspect}"

      @callback.call(result, Time.now - @start_time, @client)
      
      @client.ready!
    end
  end

  class Client < ::Mysql2::Client
    attr_accessor :pool
    
    def initialize(*args)
      super(*args)
      
      self.query_options.merge!(
        :symbolize_keys => true,
        :async => true
      )
    end
    
    def ready!
      @pool and @pool.add(self)
    end
    
    def logger(type, message)
      @pool and @pool.logger and @pool.logger.send(type, message)
    end

    def query(sql, options = { })
      if (EventMachine.reactor_running? and block_given?)
        super(sql, options)

        puts "#{self.object_id} Q> #{sql}"
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
      @size = 1 if (@size <= 0)
    end
    
    def self.size
      @size ||= 4
    end
    
    def initialize(db)
      @options = {
        :symbolize_keys => true
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
      
      if (EventMachine.reactor_running?)
        EventMachine::PeriodicTimer.new(5) do
          puts @options[:database]
          @connections.each do |c, x|
            puts "\t#{c.inspect} -> #{x.inspect}"
          end
          @connection_pool.each do |c|
            puts "\t#{c.inspect} (pool)"
          end
        end
      end
    end
    
    def logger
      @options[:logger]
    end
    
    def add(connection)
      @connections[connection] = true
      
      puts "#{self.object_id} QUEUE SIZE #{@query_queue.length}"
      
      if (@query_queue.empty?)
        puts "#{self.object_id} SLEEPY "
        @connections_active.delete(connection)
        @connection_pool << connection
      else
        puts "DUTY"
        query = @query_queue.pop
        
        connection.query(query[0], &query[1])
      end
    end
    
    def pool_connection
      connection = @connection_pool.pop
      
      puts "#{self.object_id} GOTS #{connection.inspect}"

      if (!connection and @connections.length < self.class.size)
#        connection = EmSequelAsync::Mysql::Client.new(@options)
#        connection.pool = self
puts "#{self.object_id} MADE A NEW ONE"
        connection = Mysql2::EM::Client.new(@options)
      end

      @connections_active[connection] = Time.now
      
      connection
    end

    def query(query, callback = nil, &block)
      callback ||= block

      puts "#{self.object_id} >> Q> #{query.inspect} #{callback.inspect}"

      if (connection = self.pool_connection)
        puts "#{self.object_id} CONECTION #{connection.inspect}"
        start = Time.now
        
        defer = connection.query(query)
        @connections[connection] = [ query, callback ]
        puts "#{self.object_id} DEFER: #{defer.inspect}"
        defer.callback do |result|
          puts "CALLBACK! #{callback.inspect}"
          
          EventMachine.next_tick do
            puts 'REINJECT'
            self.add(connection)

            callback.call(result.to_a, (Time.now - start).to_f, connection)
          end
        end
        defer.errback do
          puts "FAIL CITY"
          callback.call(false, (Time.now - start).to_f, connection)

          EventMachine.next_tick do
            puts 'REINJECT'
            self.add(connection)
          end
        end

        :executing
      else
        puts "QD #{@connection_pool.inspect}"
        @query_queue << [ query, callback ]

        :queued
      end
    end

    def ___query(query, callback = nil, &block)
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
