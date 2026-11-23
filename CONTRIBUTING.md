# Contributing to replicos

Issues and pull requests are welcome. Replicos is a small library with one maintainer, so the most useful thing a
change can be is *narrow*: one topic, tests that fail without it, and a description of what you measured.

## Building

```bash
./mvnw -T1C clean install
```

**JDK 17 or newer is required to build**, even though the library itself runs on JDK 11 and ships Java 11 bytecode:
the test stack needs 17, and the shared surefire arguments include a flag that JDK 16 and older reject outright. The
build enforces this and says so.

## What the build checks, beyond the tests

- **Javadoc references.** Javadoc runs with `-Xdoclint:all,-missing`, so a `{@link}`, `@see` or `@throws` that does
  not resolve is an error, as is raw HTML in a comment: write `{@code List<String>}`, not `List<String>`. It reads a
  delomboked copy of the sources, which is why a `{@link}` to a Lombok-generated accessor resolves.
- **Licence headers.** Every source file carries the Apache header from `HEADER.txt`; `./mvnw license:format` adds it
  to a new file.
- **Enforcer rules**, which state the Maven and JDK minimums above, and forbid a release depending on a snapshot.

## Tests

- **Do not give a module its own `<argLine>`.** It replaces the shared surefire arguments wholesale, and what falls
  out is silent. Add arguments with `surefire.argLine.extra` instead.
- **The tests open real sockets on localhost.** Each takes a free port from `TestingPorts` rather than hard-coding one.
- Some tests assert the bytes a thread allocates. Run one alone before concluding a change broke it: another test can
  leave the JVM in a state that allocates.

## Changes to the write path

An append and everything the IO threads do per record run on the application's latency path. There:

- An optional behaviour must cost nothing when it is off: a strategy chosen once, not a branch per record.
- Nothing allocates: no strings, no exceptions, no collections, no boxing, no capturing lambda.
- If a change there is not obviously free, measure it with the JMH suite in `benchmarks/`, and put the before and
  after numbers in the pull request.

## Pull requests

Work on a branch, open a PR against `main`, and let CI finish: it builds and tests every module on a cold runner.
There is no CLA: contributions are under the [Apache License 2.0](LICENSE.txt), the same licence the project ships
under.

Security problems do not go in an issue or a PR: see [SECURITY.md](SECURITY.md).
