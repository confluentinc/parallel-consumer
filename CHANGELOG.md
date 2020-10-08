# Change Log

## [0.0.0](https://github.com/confluentinc/parallel-consumer/releases/tag/v0.0.0) (2020-10-01)

### Features

* Docs: Fix transaction references and soften wording
* Fix readme spelling and links
* Fix example tests
* Rename: Methods
* Rename: Readme and generated references
* Rename: Package
* Rename: Main classes
* Rename: Script
* Rename: Parent
* Migrate other code samples to includes
* Readme: Correction - JDK8 supported
* Docs: Apply suggestions from code review
* Docs: Use maven template plugin to generate GH friendly includes
* Docs: Use-case elaboration
* Create and commit offset map
* Enable/disable test container logging
* Exclude Jenkins injected polution
* Release plugin settings
* CSID-380 Apply copyright notice
* CSID-380 Auto copyright and later license
* CSID-395 Fix group ids
* Backport to JDK 9 (8 coming)
* Jenkins file for Confluent CI
* Set site plugin versions - fixes missing class file error
* CSID-340 Removes Awaitility from main code
* CSID-274 Remove test collection harnesses from main, change to listeners
* CSID-372 AK MockProducer is not thread safe
* Filter out TestContainer TRACE and DEBUG levels
* CSID-276 - Ensure auto commit is disabled
* Javadoc, configurable threads, readme tweak
* TestContainer resuse
* Readme update, example apps and tests, api tweaks, small bugs
* Travis: cache .m2 dir to speed up dependency downloads
* Fix broker startup time allowance
* Fix dependency scope and management, unify/update TestContainer versions
* Update deps
* Travis fail at end (always run all tests), show dependency and plugin updates
* Fix Jacoco reports
* Delete in-flight manager
* Event based control, integration tests, back-pressure
* Fair iteration of work to prevent starvation
* Give control thread a name and remove extra pool
* Per key ordering fixes and simple load tests
* Migrate from Java8 Streams to simple callbacks
* Ordered parallel message processing by key
* Non blocking http IO support with Vert.x WebClient
* Travis JDK bump
* Readme
* Start: Close consumer
* Readme
* Draft v1 of generic async message processor
* Initial commit

### Bug Fixes

* Bug: test fix: offsetsAreNeverCommittedForMessagesStillInFlightShort
* Bug: Also open new tx if draining and commit on close
