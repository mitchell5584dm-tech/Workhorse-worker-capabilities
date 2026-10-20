# Workhorse assistant integration

Use `worker/README.md` as the execution contract for delegated tasks in this repo.
Inspect capabilities and verify task results before calling work complete.

- Preserve the existing Gradle Play Publisher code and licensing.
- Use the durable queue for supported, bounded work; use direct tools for work
  outside its capabilities. Do not claim Workhorse is connected to an LLM or
  external service unless that connection is actually verified.
- Independent jobs may run concurrently. Mutations sharing a repository, store,
  release or artifact must share a resource key or have explicit dependencies.
- Engineering/logistics estimates must state units, assumptions, constraints,
  acceptance checks and evidence. Live financial actions require the task's
  existing authorization and an explicitly configured integration.
- Never put credentials in task payloads, command policies, logs or repository files.
- Run `python -m unittest discover -s worker/tests -v` for worker changes. Run the
  original Gradle tests only when changing the Gradle implementation.
- Submit changes through a pull request as required by CONTRIBUTING.md.
