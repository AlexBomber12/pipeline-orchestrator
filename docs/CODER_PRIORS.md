# Coder priority priors

Initial `coder_priority` values used by the selector before local metrics
accumulate. Values should be revisited every 2-3 months or whenever a
major model release materially changes public coding-benchmark results.

## Sources

- SWE-bench Verified: OpenAI GPT-5 launch post and Anthropic Claude Opus 4.5
  system card, last reviewed April 19, 2026
  - https://openai.com/index/introducing-gpt-5-for-developers/
  - https://www.anthropic.com/news/claude-opus-4-5
- Aider polyglot leaderboard, last reviewed April 19, 2026
  - https://aider.chat/docs/leaderboards/
- LiveCodeBench leaderboard, last reviewed April 19, 2026
  - https://livecodebench.github.io/leaderboard.html

## Current priors

| Coder  | Priority | Rationale |
|--------|----------|-----------|
| codex  | 81       | Based on GPT-5/Codex-family public coding results: 74.9 on SWE-bench Verified and 88.0 on Aider polyglot. Rounded average is 81. |
| claude | 76       | Based on Claude Opus public coding results: 80.9 on SWE-bench Verified and 72.0 on Aider polyglot. Rounded average is 76. |

## Methodology

For each coder, collect the latest public, comparable coding-benchmark
results we can verify directly from vendor or benchmark-maintainer pages.
Normalize each benchmark to a 0-100 scale, average the numeric scores, and
round to the nearest integer. Use the latest LiveCodeBench review as a
qualitative cross-check only when the current public page does not expose
directly comparable numeric entries for both coders.

## Revision log

- April 19, 2026: Initial values set to `codex=81`, `claude=76` from the
  latest public SWE-bench Verified and Aider polyglot references above.
