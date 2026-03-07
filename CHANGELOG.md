# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

* `Storage` discoverer allows to fetch instance configuration from a cluster
  configuration in storage using `go-storage` (#56).
* `Cache` discoverer type that stores the first discovered result (#58).

### Changed

### Fixed

* `checkTimeout` race condition between deadline check and context
  cancellation that could cause Etcd `Discovery` to block indefinitely.

## [v1.0.0] - 2026-02-06

The first public release includes etcd and TcS support. Main features:

* Fetch Tarantool instances configurations from etcd and Tarantool Config
  Storage.
* Create an update instance configuration events stream due to changes in a
  cluster configuration storage.
* Filter configurations with a custom set of filters.
* Create a pool of connections depending on actual cluster configuration in
  a cluster configuration storage.
* All requests from the Go connector `tarantool/go-tarantool` are supported
  (include `tarantool/go-tarantool/crud` requests).

### Added

* `Instance` type describes an instance configuration.
* `Mode` enumeration allows to choose an instance mode to execute a request.
* `filter` subpackage with a set filters to fetch only required instances.
* `discoverer` subpackage with a set of discoverers to fetch instances
  configuration.
  * `Etcd` discoverer allows to fetch instance configuration from a cluster
    configuration in etcd.
  * `Filter` discoverer allows to filter a list of instances configurations.
  * `Connectable` discoverer returns a list of nodes from the inner discoverer
    that are available for connection.
  * `Tarantool` discoverer allows to fetch instence configuration from a
    Tarantool Config Storage.
* `scheduler` subpackage with a set of schedulers.
  * `Periodic` scheduler allows to schedule events due to a timeout.
  * `EtcdWatch` scheduler allows to schedule events due to updates in etcd.
* `subscriber` subpackage with a set of types to subscribe to new update
  instances configurations events.
  * `Connectable` filters the stream of events
    from the inner Subscriber and isolates from it only nodes available
    for connection.
  * `Schedule` subscriber combine scheduler and discoverer to generate update
    the events stream.
  * `Filter` subscriber allows to filter an update events stream due to
    instance configuration.
* `observer` subpackage with a set of observers to observe any update events.
  * `Accumulator` observer accumulates events and sends them by batches.
    While one batch is being sent, the other is accumulating.
* `pool` subpackage with a set of types to create a connection pool.
  * `NetDialerFactory` creates a connection settings to connect to an instance
    without TLS from the instance configuration.
  * `RoundRobinBalancer` helps to send requests from a pool to instances
    in round-robin.
  * `PriorityBalancer` helps to send requests from a pool to instances with
    a higher priority.
  * `Pool` of connections could be subscribed to a subscriber. It observes an
    update events stream and establishes connections or removes a connection
    from pool due to changes.
  * `DoerAdapter` adapts Pool to tarantool.Doer to send requests into the pool
    with specified mode.
