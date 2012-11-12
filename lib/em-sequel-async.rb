require 'mysql2/em'

module EmSequelAsync
  autoload(:Mysql, 'em-sequel-async/mysql')
  autoload(:SequelExtensions, 'em-sequel-async/sequel_extensions')
end
