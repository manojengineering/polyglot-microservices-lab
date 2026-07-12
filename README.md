# Fluxgate — Distributed Rate Limiter

Design-stage repository. No implementation yet — this is the technical
design document written before a single line of code, in the style of a
pre-implementation TDD at an infrastructure-heavy engineering org.

See [`docs/TECHNICAL_DESIGN.md`](docs/TECHNICAL_DESIGN.md) for the full
architecture: problem statement, requirements, algorithm choice (GCRA over
token bucket / sliding window / fixed window), Redis data model, API design,
failure-recovery strategy, Kubernetes/Docker deployment, and the trade-offs
behind every decision above.
