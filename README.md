# em-sequel-async

This is an EventMachine compatible adapter layer for [Sequel](http://sequel.jeremyevans.net)
that's intended to be a drop-in replacement for [tmm1/em-mysql](https://github.com/tmm1/em-mysql).

Only the `mysql2` driver is supported at this time.

## Dependencies

This library requires Ruby 1.9.3 or better.

## Testing

To configure the test environment create a defaults file `.database.yml`
in the root of the project. The user defined here will need to have
`CREATE DATABASE` priviliges.

To test:

    rake test

## Copyright

Copyright (c) 2012-2019 Scott Tadman, PostageApp Ltd.
See LICENSE.txt for further details.
