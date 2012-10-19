require 'mysql2'
require 'sequel/adapters/mysql2'

module EmSequelAsync::SequelExtensions
  def self.install!
    Mysql2::Result.send(:extend, Result::ClassMethods)
    Mysql2::Result.send(:include, Result::InstanceMethods)
    
    Sequel::Mysql2::Database.send(:extend, Database::ClassMethods)
    Sequel::Mysql2::Database.send(:include, Database::InstanceMethods)
    
    Sequel::Dataset.send(:extend, Dataset::ClassMethods)
    Sequel::Dataset.send(:include, Dataset::InstanceMethods)

    Sequel::Model.send(:extend, Model::ClassMethods)
    Sequel::Model.send(:include, Model::InstanceMethods)
  end
  
  module Result
    module ClassMethods
    end
    
    module InstanceMethods
      def empty?
        !self.any?
      end
    end
  end
  
  module Database
    module ClassMethods
    end
    
    module InstanceMethods
      def async
        @async_db ||= EmSequelAsync::Mysql::ClientPool.new(self)
      end
    end
  end
  
  # NOTE: This interface is borrowed from the deprecated tmm1/em-mysql gem.
  
  module Dataset
    module ClassMethods
      # None defined.
    end
    
    module InstanceMethods
      STOCK_COUNT_OPTS = {
        :select => [ Sequel::LiteralString.new("COUNT(*)").freeze ],
        :order => nil
      }.freeze
      
      def async_insert(*args)
        self.db.async.execute(insert_sql(*args)) do |result, time, client|
          yield(client.last_id) if (block_given?)
        end

        return
      end

      def async_insert_ignore(*args)
        self.db.async.execute(insert_ignore.insert_sql(*args)) do |result, time, client|
          yield(client.affected_rows) if (block_given?)
        end

        return
      end

      def async_update(*args)
        self.db.async.execute(update_sql(*args)) do |result, time, client|
          yield(client.affected_rows) if (block_given?)
        end

        return
      end

      def async_delete
        self.db.async.execute(delete_sql) do |result, time, client|
          yield(client.affected_rows) if (block_given?)
        end

        return
      end

      def async_multi_insert(*args)
        self.db.async.execute(multi_insert_sql(*args).first) do |result, time, client|
          yield(client.affected_rows) if (block_given?)
        end
        
        return
      end

      def async_multi_insert_ignore(*args)
        self.db.async.execute(insert_ignore.multi_insert_sql(*args).first) do |result, time, client|
          yield(client.affected_rows) if (block_given?)
        end
        
        return
      end
      
      def async_fetch_rows(sql, iter = :each)
        self.db.async.execute(sql) do |result|
          case (result)
          when Mysql2::Result
            case (iter)
            when :each
              result.each do |row|
                yield(row)
              end
            else
              yield(result)
            end
          end
        end

        return
      end

      def async_first(sql)
        self.db.async.raw(sql, :all) do |rows|
          yield(rows && rows[0])
        end
      end

      def async_each
        async_fetch_rows(select_sql, :each) do |row|
          if (row_proc = @row_proc)
            yield(row_proc.call(row))
          else
            yield(row)
          end
        end

        return
      end

      def async_all
        async_fetch_rows(sql, :all) do |rows|
          if (row_proc = @row_proc)
            yield(rows.map { |row| row_proc.call(row) })
          else
            yield(rows)
          end
        end

        return
      end

      def async_count(&callback)
        if (options_overlap(COUNT_FROM_SELF_OPTS))
          from_self.async_count(&callback)
        else
          clone(STOCK_COUNT_OPTS).async_each do |row|
            yield(row.is_a?(Hash) ? row.values.first.to_i : row.values.values.first.to_i)
          end
        end

        return
      end
    end
  end

  module Model
    module ClassMethods
      [ :async_insert,
        :async_insert_ignore,
        :async_multi_insert,
        :async_multi_insert_ignore,
        :async_each,
        :async_all,
        :async_update,
        :async_count
      ].each do |method|
         eval %[
          def #{method}(*args, &callback)
            dataset.#{method}(*args, &callback)
          end
        ]
      end

      # This differs from async_multi_insert_ignore in that it takes an
      # array of hashes rather than a series of arrays. The columns are
      # automatically determined based on the keys of first hash provided.
      def async_multi_insert_ignore_hash(hashes)
        if (hash.empty?)
          yield if (block_given?)

          return
        end

        columns = hashes.first.keys

        insertions = hashes.collect do |row|
          columns.collect { |c| row[c] }
        end

        async_multi_insert_ignore(columns, insertions) do |n|
          yield(n) if (block_given?)
        end
      end

      # Async version of Model#[]
      def async_lookup(args)
        unless (Hash === args)
          args = primary_key_hash(args)
        end

        dataset.where(args).limit(1).async_all do |rows|
          yield(rows.any? ? rows.first : nil)
        end

        return
      end
    end

    module InstanceMethods
      def async_update(*args, &callback)
        this.async_update(*args, &callback)
        set(*args)

        self
      end

      def async_delete(&callback)
        this.async_delete(&callback)

        return
      end
    end
  end
end
