# Concurrent LLM Client-Server System

A C-based network application consisting of an interactive LLM client and a
multithreaded HTTP server. The server manages concurrent user sessions, keeps a
bounded conversation history, proxies completion requests to `smollm2`, and
exposes runtime statistics through a dedicated endpoint.

## Highlights

- POSIX TCP sockets and HTTP/JSON request handling
- Thread-per-connection concurrency with shared-state synchronization
- Per-client bounded prompt and response history
- Child-process lifecycle management for the local LLM service
- Retry, disconnect, malformed-request, and error-response handling
- Runtime connection, session, prompt, and token statistics
- Signal handling and controlled resource cleanup

## Project Structure

- `uqllmclient.c`: interactive command-line client
- `uqllmserver.c`: concurrent HTTP server and LLM proxy
- `Makefile`: build rules for the UQ CSSE2310 environment

## Build Requirements

The supplied `Makefile` depends on the UQ CSSE2310 teaching environment,
including `csse2310a3.h`, `libcsse2310a3`, and the `smollm2` executable. Build
the project in that environment with:

```sh
make
```

Remove generated binaries with:

```sh
make clean
```

## Academic Integrity

This repository is intended as a private portfolio archive. Keep it private and
follow your institution's academic-integrity and code-sharing requirements.
