# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2020-12-11
### Added
- Parallelization for source connector based on channels/patterns

### Removed
- Default configuration for Kafka topic

## [1.0.4] - 2020-11-29
### Added
- Added support for sinking arbitrary Redis commands, primarily for use with Redis modules

### Fixed
- Fixed linter configuration

## [1.0.3] - 2020-11-21
### Added
- Added support for Redis EXPIRE commands
- Added support for Redis EXPIREAT commands
- Added support for Redis PEXPIRE commands

### Changed
- Improved source connector partitioning documentation

### Fixed
- Source connector no longer logs every Redis message at an INFO level
- Added missing configuration property `topic` to the source connector documentation

## [1.0.2] - 2020-11-13
### Fixed
- Fixed POM description to include source capability
- Minor corrections to the demo documentation

## [1.0.1] - 2020-11-11
### Added
- Added quickstart properties for the source connector

### Fixed
- Fixed broken link and missing information in the Confluent Hub package

## [1.0.0] - 2020-11-10
### Added
- Initial release with source and sink connector
