# Security policy

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private reporting:
[**Report a vulnerability**](https://github.com/lolaf-org/replicos/security/advisories/new). It is visible only to the
maintainer until an advisory is published.

Please include the replicos version, the JDK of each node, and the encryption settings in use (mode, cipher, provider),
along with enough detail to reproduce it. Replicos is maintained by one person: expect an acknowledgement within a
week, and a fix released as soon as one exists rather than on a schedule. You will be credited in the advisory unless
you ask not to be.

## Supported versions

Fixes go to the latest released version, and while the version is 0.x that means the latest minor: there are no
maintenance branches.

## Scope

Encryption is optional and configured by the application: running the lanes in clear, a weak shared secret, or a
secret shared with a node that should not have it are the application's decisions, not vulnerabilities in replicos.

What is in scope is replicos doing something the application did not ask for. Above all, **a frame accepted that its
peer did not send**: once a connection's keys are agreed every frame is authenticated, so a forged, altered or
replayed frame being applied is exactly the bug this section exists for. So is a lane that required encryption
carrying records in clear, a peer replicating another application being accepted, one stream's records applied into
another, and a follower writing into storage the leader reported as its own.
