# Changelog

## v3.0.1

Clarify the public API, in particular for PyDoctor-generated documentation:

- Consolidate the public API onto the top-level `jobserver` namespace:
  `Blocked`, `CallbackRaised`, `Future`, `Jobserver`, `JobserverExecutor`,
  and `LostResult`.
- Mark every internal helper private with a leading underscore.  Code that
  imported former internals such as `Resources` or `ExceptionWrapper` must
  adjust.

## v3.0

- Initial PyPI release
