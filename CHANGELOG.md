# 0.2.0

- New `retries` method to control how many attempts should be made before giving up.
- New `mirrors` method for specifying alternative servers to fetch from.
- Callback polling will no longer hold a completed task.
- Range header responses are now verified per request.
- Server responses now have their status validated.
- Received range headers are also validated.
- The `length` method has been removed.
- Scoped threads are now in use.

# 0.1.0

- Initial release