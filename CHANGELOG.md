# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

### Changed

### Fixed

## [v3.0.0] - 2026-08-27

This release bumps `go-tarantool` to v3, and `go-storage` and `go-config`
to v2. These libraries expose their types through the go-discovery public
API, so this is a breaking release.

The most notable change is that `tarantool.Future` became an interface
returned by value instead of a pointer: `Pool.Do`, `DoerAdapter.Do`, and the
`ModeDoer.Do` interface method now return `tarantool.Future` instead of
`*tarantool.Future`. The `discoverer.NewStorage`, `NewEtcd`, and
`NewTarantool` functions now accept storage clients from `go-storage/v2`
(which itself builds on `go-config/v2`), so callers that pass custom clients
must migrate them to the new module versions.

### Changed

- `deps`: go-tarantool was bumped to v3 (#68).
- `deps`: go-storage and go-config were bumped to v2 (#69).
- `deps`: Go was bumped to v1.26.5.

## [v2.0.2] - 2026-08-05

This release bumps `go-config` to v1.5.0 and `go-storage` to v1.6.1,
pulling in their accumulated fixes and improvements. Most notably, it
fixes merging of configurations across inheritance levels
(global → group → replicaset → instance): `MergeDeep` is now the default
inheritance strategy, so a higher-priority layer that sets a single
sub-key no longer drops sibling sub-keys contributed by a lower-priority
layer.

### Changed

- Bump `go-config` from v1.1.0 to v1.5.0 (#67).
- Bump `go-storage` from v1.2.0 to v1.6.1 (#67).

## [v2.0.1] - 2026-05-07

This release introduces updated library name `go-discovery/v2`.

## [v2.0.0] - 2026-05-06

This release introduces migration from `tt/lib/cluster` to `go-config`
and `go-storage` libraries.

### Changed

- Replace `tt/lib/cluster` with `go-config` and `go-storage` libraries for
  configuration parsing and storage backends (#61).
- `NewEtcd` now requires the `discoverer.EtcdClient` interface instead of
  `clientv3.KV` and returns `*Storage` instead of `*Etcd` (#61).
- `NewTarantool` now requires the `discoverer.TarantoolClient` interface
  instead of `tarantool.Doer` and returns `*Storage` instead of `*Tarantool`
  (#61).
- The `Etcd` and `Tarantool` types have been removed; use the `Storage` type
  instead (#61).
- `ErrMissingEtcd`, `ErrMissingTarantool`, and `ErrTypedStorageNil` errors have
  been replaced by `ErrMissingStorage` (#61).
- Retry logic on `context.DeadlineExceeded` has been removed from the
  `Storage` discoverer (#61).
- Configuration key prefixes are now normalized to always include a leading
  slash (#61).
- The default deadline limit in 3 seconds is deleted from discoverers (#61).
- `NewStorageDiscoverer` has been renamed to `NewStorage` (#61).

## [v1.2.0] - 2026-05-5

This release introduces updated `Instance` object. From now on it contains
`roles_cfg` section, this allows callers to read role-specific configuration
without making additional RPC calls to Tarantool.

### Added

* `Instance` now exposes a `RolesCfg` field — a map of role name to its
  configuration as defined in the `roles_cfg` section of the cluster config
  (#62).

## [v1.1.0] - 2026-03-17

This release introduces two new discoverers:
* `Cache` discoverer to store the first discovered result.
* `Storage` discoverer allows get configuration from go-storage.

### Added

* `Storage` discoverer allows to fetch instance configuration from a cluster
  configuration in storage using `go-storage` (#56).
* `Cache` discoverer type that stores the first discovered result (#58).

### Fixed

* `checkTimeout` race condition between deadline check and context
  cancellation that could cause Etcd `Discovery` to block indefinitely (#57).

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
