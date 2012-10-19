require File.expand_path('helper', File.dirname(__FILE__))

DB_CONFIG = {
  :default => {
    :adapter => 'mysql2',
    :username => 'test',
    :password => 'JAQv0RM6xFVb8yz06GiQ7mOq',
    :database => 'emsa_test_default'
  },
  :a => {
    :adapter => 'mysql2',
    :username => 'test',
    :password => 'JAQv0RM6xFVb8yz06GiQ7mOq',
    :database => 'emsa_test_a'
  },
  :b => {
    :adapter => 'mysql2',
    :username => 'test',
    :password => 'JAQv0RM6xFVb8yz06GiQ7mOq',
    :database => 'emsa_test_b'
  }
}.freeze

DB = Hash[DB_CONFIG.collect { |db, config| [ db, Sequel.connect(config) ] }]

{
  :default => %w[ db_default_a_models db_default_b_models ],
  :a => %w[ example_as ],
  :b => %w[ example_bs ]
}.each do |db, tables|
  create_config = DB_CONFIG[db].dup
  db_name = create_config.delete(:database)
  
  handle = Mysql2::Client.new(create_config)
  db_name = DB_CONFIG[db][:database]
  
  handle.query("DROP DATABASE IF EXISTS `#{db_name}`")
  handle.query("CREATE DATABASE `#{db_name}`")
  handle.query("USE `#{db_name}`")

  tables.each do |table|
    handle.query("CREATE TABLE `#{table}` (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255))")
  end
end

class DbDefaultAModel < Sequel::Model
end

class DbDefaultBModel < Sequel::Model
end

class DbAModel < Sequel::Model(DB[:a][:example_as])
end

class DbBModel < Sequel::Model(DB[:b][:example_bs])
end

class TestEmSequelAsync < Test::Unit::TestCase
  def test_module
    assert EmSequelAsync
  end
  
  def test_async_db_handle
    assert DbDefaultAModel.db
    assert DbDefaultAModel.db.async
    
    assert_equal DbDefaultAModel.db, DbDefaultBModel.db
    assert_not_equal DbAModel.db, DbDefaultBModel.db
    assert_not_equal DbAModel.db, DbBModel.db
    
    assert_equal DbDefaultAModel.db.async, DbDefaultBModel.db.async
    assert_not_equal DbAModel.db.async, DbDefaultBModel.db.async
    assert_not_equal DbAModel.db.async, DbBModel.db.async
  end

  def test_async_insert_df
    em do
      inserted = false
      
      defer do
        await do |callback|
          DbDefaultAModel.async_insert(:data => 'Test Name') do
            inserted = inserted_id
            callback.call
          end
        end
      end

      assert inserted > 0
    end
  end
  
  def test_async_insert
    EventMachine.run do
      inserted = false
      
      f = Fiber.new do
        DbDefaultAModel.async_insert(:data => 'Test Name') do |inserted_id|
          inserted = inserted_id
          f.resume
        end
        
        Fiber.yield

        assert inserted > 0
      end.resume
      
      EventMachine.stop_event_loop
    end
  end
end
