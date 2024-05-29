# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

* Field `Instance.Labels` is a map that contains data from the `labels`
  configuration path (#34). This field is a replacement for `Instance.AppTags`
  and `Instance.RolesTags`.
* Filter `LabelsContain` for filtering instance by labels (#34).
* `GetURIPreferUnix` util for `dial` submodule (previously a private part of
  `NetDialerFactory` implementation).

### Changed

* Filter `RolesContains` renamed to `RolesContain` (#34).
* Filter `RolesContain` returns true if empty (#34).

### Removed

* Fields `Instance.AppTags` and `Instance.RolesTags` (#34).
* Filters `AppTagsContains` and `RolesTagsContains` (#34).

### Fixed

## [v0.2.0] - 2024-04-03

The release adds missed features from RFC and fixes found bugs.

### Added

* `Connectable` discoverer (#4). It returns a list of nodes from the
  inner discoverer that are available for connection.
* `Accumulator` observer (#9). It accumulates events and sends them by batches.
  While one batch is being sent, the other is accumulating.
* `Connectable` subscriber (#9). It filters the stream of events
  from the inner Subscriber and isolates from it only nodes available
  for connection.

### Changed

* `DialerFactory` interface is moved to the main package from the `pool`
  package (#4).
* `NetDialerFactory` is moved to the `dial` package from the `pool`
  package (#4).

### Fixed

* An invalid instance mode in the pool (`ModeRO` instead of `ModeRW` and
  vice versa) (#9).
* Unnecessary logging on successful instance removal from a pool (#9).

## [v0.1.0] - 2024-04-03

The initial release of the library. The main features:

* Fetch Tarantool instances configurations from etcd.
* Create an update instance configuration events stream due to changes in etcd.
* Filter configurations with a custom set of filters.
* Create a pool of connections depending on actual cluster configuration in
  etcd.
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
* `scheduler` subpackage with a set of schedulers.
  * `Periodic` scheduler allows to schedule events due to a timeout.
  * `EtcdWatch` scheduler allows to schedule events due to updates in etcd.
* `subscriber` subpackage with a set of types to subscribe to new update
  instances configurations events.
  * `Schedule` subscriber combains scheduler and discoverer to generate update
    the events stream.
  * `Filter` subscriber allows to filter an update events stream due to
    instance configuration.
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
