# Change Log

## [Unreleased]
Nothing yet.

## [0.3.9] - 2021-01-12
### Fixed
- The `update-store` API was erroneously change in the last release, and also contained a consistency bug.
  Also added a new test for this function, as it never had one before.

## [0.3.8] - 2021-01-11
### Added
- The `update-store` operation had been removed due to lack of support in Asami,
  and the belief that it was no longer used. Added back in, with Asami support in 1.2.10.

## [0.3.7] - 2021-01-08
### Added
- The `run` operation now accepts Connection objects from Datomic or Asami.
- Added a flag to avoid calling shutdown upon exiting the CLI

### Changed
- Updated Asami storage to use Connections, and not internal graphs.

## [0.3.6] - 2020-12-15
### Changed
- Updated dependencies on Asami, Zuko and core.cache.

## [0.3.5] - 2020-09-08
### Fixed
- Fixed entity namespaces for the Asami and Datomic adapters

## [0.3.4] - 2020-09-04
### Fixed
- Picked up important bugfix from Zuko 0.3.1

## [0.3.3] - 2020-09-04
### Fixed
- Fixed duplicated functionality between the Storage and NodeAPI protocols

## [0.3.2] - 2020-09-04
### Fixed
- Removed warning for using a deprecated function

## [0.3.1] - 2020-09-03
### Changed
- Updated to Asami 1.2.3


## [0.3.0] - 2020-08-06
### Changed
- Updated to Asami 1.1.0

## 0.2.39 - 2020-06-30
### Fixed
- Fixed the way evaluations are considered in rules

## 0.2.37 - 2020-06-18
### Changed
- Shifted data management to Zuko

[Unreleased]: https://github.com/threatgrid/naga/compare/0.3.9...HEAD
[0.3.9]: https://github.com/threatgrid/naga/compare/0.3.8...0.3.9
[0.3.8]: https://github.com/threatgrid/naga/compare/0.3.7...0.3.8
[0.3.7]: https://github.com/threatgrid/naga/compare/0.3.6...0.3.7
[0.3.6]: https://github.com/threatgrid/naga/compare/0.3.5...0.3.6
[0.3.5]: https://github.com/threatgrid/naga/compare/0.3.4...0.3.5
[0.3.4]: https://github.com/threatgrid/naga/compare/0.3.3...0.3.4
[0.3.3]: https://github.com/threatgrid/naga/compare/0.3.2...0.3.3
[0.3.2]: https://github.com/threatgrid/naga/compare/0.3.1...0.3.2
[0.3.1]: https://github.com/threatgrid/naga/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/threatgrid/naga/compare/0.3.0...0.3.0
