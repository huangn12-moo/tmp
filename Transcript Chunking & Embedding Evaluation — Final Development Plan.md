# Transcript Chunking & Embedding Evaluation — Development Plan

## 1. Objective

Build an **Airflow 2.x evaluation DAG** that evaluates transcript chunking, embedding, and retrieval strategies.

The evaluation should objectively answer:

1. Which chunking strategy provides the best retrieval quality?
2. Which embedding model provides the best retrieval quality?
3. How does semantic/vector retrieval compare with lexical/full-text retrieval?
4. Does hybrid retrieval outperform either method individually?
5. Which strategies perform better for different types of user questions?
6. Does better retrieval translate into better final LLM answers?

The evaluation must be **self-contained, reproducible, configurable, and independent of the production database**.

---

# 2. Scope and Storage Architecture

### Source of truth

**S3 is the only source of truth for transcript data.**

### Evaluation storage

**SQLite is the only database used by the evaluation pipeline.**

The evaluation DAG must **not depend on PostgreSQL, pgvector, pg_search, or any other production database/search service**.

All evaluation data and intermediate results must be stored in a run-scoped SQLite database.

Architecture:

```text
S3
Source Transcripts
      |
      v
Load transcripts
      |
      v
+----------------------------------+
|        Temporary SQLite DB       |
|                                  |
| transcripts                       |
| evaluation_questions              |
| evidence                          |
| chunks                            |
| embeddings                        |
| retrieval_results                 |
| evaluation_metrics                |
+----------------------------------+
      |
      +------------------+
      |                  |
      v                  v
   SQLite FTS5       Vector Search
      |                  |
      +--------+---------+
               |
               v
         Hybrid Retrieval
               |
               v
       Retrieval Evaluation
               |
               v
        Answer Generation
               |
               v
           Report
               |
               v
            Email
```

SQLite is disposable derived data.

A unique SQLite database must be created for each DAG run.

---

# 3. Airflow

Use **Airflow 2.x**.

Do not use Airflow 3 APIs.

Use Airflow 2 TaskFlow APIs, for example:

```python
from airflow import DAG
from airflow.decorators import task
```

Recommended task flow:

```text
validate_config
      |
load_transcripts
      |
generate_eval_questions
      |
validate_eval_questions
      |
generate_chunks
      |
generate_embeddings
      |
build_search_indexes
      |
run_retrieval_experiments
      |
evaluate_retrieval
      |
evaluate_answers
      |
generate_report
      |
send_email
      |
cleanup
```

Tasks should be independently retryable where practical.

Do not pass transcripts, chunks, embeddings, or large result sets through XCom.

Use SQLite and local temporary files for large intermediate artifacts.

---

# 4. DAG Run Configuration

All experiment-specific configuration must be provided through `dag_run.conf`.

Do not hard-code models, strategies, S3 paths, or email recipients in the DAG.

Example:

```json
{
  "source": {
    "s3_paths": [
      "s3://bucket/transcripts/2026/08/"
    ]
  },

  "evaluation": {
    "name": "transcript_search_v1",
    "random_seed": 42,
    "question_count_per_transcript": 10
  },

  "question_generation": {
    "model": "question-generation-model",
    "temperature": 0.2
  },

  "chunking": {
    "strategies": [
      "fixed",
      "speaker_aware",
      "qna"
    ],

    "fixed": {
      "chunk_size": 500,
      "overlap": 50
    },

    "speaker_aware": {
      "max_tokens": 800
    },

    "qna": {
      "max_tokens": 800
    }
  },

  "embedding": {
    "models": [
      "embedding-model-a",
      "embedding-model-b"
    ]
  },

  "retrieval": {
    "strategies": [
      "full_text",
      "vector",
      "hybrid"
    ],
    "top_k": [
      5,
      10,
      20
    ]
  },

  "answer_generation": {
    "enabled": true,
    "model": "answer-generation-model",
    "temperature": 0
  },

  "report": {
    "email_to": [
      "user@example.com"
    ]
  },

  "cleanup": {
    "enabled": true
  }
}
```

Validate all configuration before starting expensive work.

---

# 5. Model Configuration

Treat the following as separate model roles.

## 5.1 Question Generation Model

Used to generate:

- synthetic questions
- synthetic answers
- ground-truth evidence references

Configured as:

```text
question_generation.model
```

The exact model must be recorded in the evaluation metadata.

---

## 5.2 Embedding Model

Used for both:

- chunk embeddings
- query embeddings

Configured as:

```text
embedding.models[]
```

Every embedding model must be evaluated independently.

---

## 5.3 Answer Generation Model

Used only for end-to-end answer evaluation.

Configured as:

```text
answer_generation.model
```

It must be possible to run retrieval evaluation without answer generation.

This separation is important because:

> Retrieval failure and answer-generation failure are different problems.

---

# 6. Load Transcripts

Read transcripts from configured S3 paths.

Each transcript should retain:

```text
transcript_id
source_s3_path
company
meeting_type
date
industry
sector
transcript_text
```

Store the transcript in SQLite for the duration of the evaluation run.

S3 remains the canonical source.

Do not modify the S3 source data.

Preserve the original transcript text exactly so that evidence spans can be evaluated deterministically.

---

# 7. Synthetic Evaluation Dataset

Because real users cannot currently provide sample questions or ground truth, generate a synthetic evaluation dataset from the transcripts.

Recommended initial scale:

```text
20–30 transcripts
10–15 questions per transcript
300–450 questions
```

Each evaluation item must contain:

```json
{
  "eval_id": "q_001",
  "transcript_id": "meeting_123",
  "question": "What is management's outlook for China demand?",
  "answer": "Management expects ...",
  "question_type": "semantic",
  "difficulty": "medium",
  "evidence": [
    {
      "start_offset": 18342,
      "end_offset": 19281
    }
  ]
}
```

The **evidence span is the primary ground truth** for retrieval evaluation.

Do not use the generated answer alone as ground truth.

---

# 8. Question Types

Generate realistic user questions across multiple categories.

Recommended distribution:

| Question Type | Percentage |
|---|---:|
| Direct lookup | 20% |
| Semantic / paraphrase | 30% |
| Specific detail | 20% |
| Multi-evidence | 15% |
| Negative / contradiction | 10% |
| Comparison / temporal | 5% |

Examples:

### Direct

> What did management say about China demand?

### Semantic

Transcript:

> We have moderated our expectations for the Chinese market.

Question:

> Which companies are becoming more cautious about China?

### Specific detail

> What did management identify as the main driver of margin pressure?

### Multi-evidence

> What were the two main factors management cited for weaker demand?

### Negative

> Did management report weakness in China demand?

The transcript may say:

> We did not see weakness in China demand.

### Temporal / comparison

> How has management's China outlook changed compared with the previous quarter?

Questions must be answerable entirely from the transcript.

Do not require external knowledge.

Avoid questions that simply copy transcript wording.

---

# 9. Ground-Truth Evidence

Ground truth should reference the original transcript using stable offsets or paragraph/turn IDs.

Prefer:

```text
transcript_id
start_offset
end_offset
```

rather than only storing copied evidence text.

A question may have multiple evidence spans.

For example:

```text
evidence:
  - start: 10000
    end: 10500
  - start: 13200
    end: 13750
```

This supports multi-evidence questions.

---

# 10. Synthetic Dataset Validation

Validate every generated question before it enters the evaluation set.

Validate:

- question is non-empty
- answer is non-empty
- evidence exists
- evidence offsets are valid
- evidence belongs to the specified transcript
- evidence text is non-empty
- question is answerable from the transcript
- question does not require external knowledge
- question is not a direct copy of the transcript

Optionally use a second LLM validation pass to assess whether the evidence actually supports the generated answer.

Record:

```text
questions_generated
questions_accepted
questions_rejected
rejection_rate
```

Use the configured random seed for reproducibility.

---

# 11. Chunking Strategies

Implement three initial chunking strategies.

## 11.1 Fixed Chunking

Baseline.

Example:

```text
chunk_size = 500 tokens
overlap = 50 tokens
```

Both parameters must be configurable.

---

## 11.2 Speaker-Aware Chunking

Use speaker and paragraph boundaries when available.

Prefer not to split a speaker turn.

If a speaker turn exceeds the maximum size, split at natural sentence or paragraph boundaries.

Retain speaker metadata where available.

---

## 11.3 Q&A-Aware Chunking

For Q&A sections, group:

```text
Analyst Question
+
Management Answer
```

into a semantic unit.

The segmentation must be automatic.

Do not require manual annotation.

Use transcript speaker labels and roles when available.

If speaker roles are ambiguous, use deterministic rules first and use an LLM only for ambiguous cases.

If an answer is too long, split it into multiple sub-chunks while preserving the original question context in every sub-chunk.

Example:

```text
Q: What is your outlook for China demand?

A: Demand has...
```

and:

```text
Q: What is your outlook for China demand?

A: Pricing remains...
```

This ensures the embedding retains the question context.

---

# 12. Chunk Metadata

Every chunk must retain:

```text
chunk_id
transcript_id
strategy
start_offset
end_offset
text
```

Recommended additional fields:

```text
speaker
section
question_id
parent_chunk_id
```

The chunk must always be traceable back to the original S3 transcript.

---

# 13. SQLite Schema

The SQLite database should contain the complete evaluation state.

Suggested tables:

### `transcripts`

```text
transcript_id
source_s3_path
company
meeting_type
date
industry
sector
text
```

### `evaluation_questions`

```text
eval_id
transcript_id
question
answer
question_type
difficulty
```

### `evidence`

```text
eval_id
start_offset
end_offset
```

### `chunks`

```text
chunk_id
transcript_id
strategy
start_offset
end_offset
text
speaker
section
```

### `embeddings`

```text
chunk_id
embedding_model
embedding_dimension
embedding
```

### `retrieval_results`

```text
experiment_id
eval_id
retrieval_strategy
chunk_id
rank
score
```

### `evaluation_metrics`

```text
experiment_id
metric
value
question_type
difficulty
```

SQLite should be the single evaluation database.

---

# 14. SQLite Full-Text Search

Use SQLite FTS5 as the lexical/full-text baseline.

Do not depend on:

- PostgreSQL
- pg_search
- pgvector
- Elasticsearch
- OpenSearch
- another external search service

The full-text retriever should return a normalized result format:

```json
{
  "eval_id": "q_001",
  "retrieval_strategy": "full_text",
  "results": [
    {
      "rank": 1,
      "chunk_id": "chunk_123",
      "score": 0.91
    }
  ]
}
```

This provides the lexical baseline against which vector and hybrid retrieval can be evaluated.

---

# 15. Vector Search

Generate embeddings for every:

```text
chunking_strategy × embedding_model
```

combination.

Store embeddings in SQLite.

If the environment does not provide a SQLite vector extension, implement the initial vector search in the application layer.

The vector-search implementation must be isolated behind an interface so it can later be replaced by a dedicated vector index without changing the evaluation framework.

For every question:

1. Generate query embedding.
2. Compare against chunk embeddings.
3. Rank by similarity.
4. Return Top K.

Use the same embedding model for query and chunk embeddings.

---

# 16. Hybrid Retrieval

Implement hybrid retrieval using:

```text
SQLite FTS5
+
SQLite vector search
```

Use a deterministic fusion strategy such as Reciprocal Rank Fusion (RRF).

Do not use an LLM as the primary retrieval ranker.

Return normalized results:

```text
eval_id
retrieval_strategy
chunk_id
rank
score
```

---

# 17. Experiment Matrix

Generate experiment combinations dynamically from DAG Run configuration.

Example:

```text
fixed + embedding_A + vector
fixed + embedding_B + vector

speaker_aware + embedding_A + vector
speaker_aware + embedding_B + vector

qna + embedding_A + vector
qna + embedding_B + vector

qna + embedding_B + full_text
qna + embedding_B + hybrid
```

Do not hard-code experiment combinations.

Every experiment must have a unique `experiment_id`.

---

# 18. Retrieval Evaluation

For every experiment, calculate:

### Primary metrics

- Recall@5
- Recall@10
- Recall@20

### Secondary metrics

- MRR
- nDCG@10

The primary metric is:

> **Recall@10**

A retrieval result is considered a hit if the retrieved chunk overlaps the ground-truth evidence span according to a configurable overlap rule.

For questions with multiple evidence spans, report both:

- question-level recall
- evidence-level recall

where practical.

---

# 19. Breakdown Analysis

Break metrics down by:

### Question type

- direct
- semantic
- specific_detail
- multi_evidence
- negative
- comparison

### Difficulty

- easy
- medium
- hard
- very_hard

This should reveal, for example, whether:

- vector search helps semantic questions
- full-text search performs better for exact terminology
- negative questions cause retrieval errors
- Q&A chunking helps multi-evidence questions

---

# 20. Query Mutation

Optionally generate multiple natural-language variants of the same canonical question.

Example:

Canonical:

> What is management's outlook for China demand?

Variants:

> How is China demand looking?

> Is China still a headwind?

> What are they seeing in the Chinese market?

> Has the China outlook improved?

All variants must reference the same ground-truth evidence.

This tests robustness to different user phrasings.

---

# 21. End-to-End Answer Evaluation

Keep answer evaluation separate from retrieval evaluation.

### Retrieval evaluation

```text
Question
→ Retrieval
→ Ground-truth evidence
```

### Answer evaluation

```text
Question
+
Retrieved chunks
→ Answer Generation Model
→ Final Answer
```

Evaluate final answers on:

- correctness
- completeness
- groundedness
- evidence/citation accuracy

The answer-generation model must be configurable independently.

The report must distinguish:

```text
retrieval failure
```

from:

```text
answer-generation failure
```

---

# 22. Report

Generate:

1. JSON/CSV machine-readable metrics.
2. Human-readable HTML report.

The report should contain:

## Executive Summary

```text
Best chunking strategy
Best embedding model
Best retrieval strategy
Best Recall@10
Best MRR
```

## Overall Comparison

| Chunking | Embedding | Retrieval | Recall@5 | Recall@10 | Recall@20 | MRR |
|---|---|---|---:|---:|---:|---:|
| fixed | A | vector | | | | |
| speaker | A | vector | | | | |
| qna | A | vector | | | | |
| qna | B | hybrid | | | | |

## Question-Type Breakdown

Show retrieval metrics by question type.

## Difficulty Breakdown

Show retrieval metrics by difficulty.

## Failure Analysis

Include representative failures with:

```text
Question
Ground-truth evidence
Retrieved chunks
Retrieval scores
Experiment configuration
```

This is important for understanding failure modes.

---

# 23. Report Email

Send the final report to:

```text
dag_run.conf["report"]["email_to"]
```

The email should contain:

- evaluation name
- evaluation run ID
- number of transcripts
- number of evaluation questions
- best configuration
- Recall@10
- MRR
- key findings

Attach the detailed HTML/CSV report where practical.

If email delivery fails, the DAG should fail rather than silently report success.

---

# 24. Reproducibility

Every run must record:

```text
evaluation_run_id
dag_run_id
source_s3_paths
question_generation_model
embedding_models
answer_generation_model
chunking configuration
retrieval configuration
top_k
random_seed
timestamp
```

These values must appear in the final report.

The same source data, configuration, models, and random seed should produce a comparable evaluation dataset.

---

# 25. Cleanup

Create a unique SQLite database per DAG run.

For example:

```text
/tmp/transcript_eval_<dag_run_id>.sqlite
```

After successful report generation and email delivery:

If cleanup is enabled:

```text
cleanup.enabled = true
```

delete:

- temporary SQLite database
- temporary local files
- temporary intermediate artifacts

Never delete S3 source data.

If the DAG fails, preserve temporary artifacts where possible for debugging.

---

# 26. Code Structure

Keep Airflow orchestration separate from evaluation logic.

Recommended structure:

```text
evaluation/
│
├── dag.py
├── config.py
│
├── transcript_loader.py
├── question_generator.py
├── question_validator.py
│
├── chunker.py
├── chunking/
│   ├── fixed.py
│   ├── speaker_aware.py
│   └── qna.py
│
├── embedder.py
│
├── sqlite_store.py
├── fts_retriever.py
├── vector_retriever.py
├── hybrid_retriever.py
│
├── evaluator.py
├── answer_evaluator.py
├── report.py
└── emailer.py
```

The Airflow DAG should orchestrate these modules rather than contain the business logic itself.

---

# 27. Development Phases

## Phase 1 — End-to-End MVP

Implement:

- S3 transcript loading
- SQLite initialization
- synthetic question generation
- evidence-based ground truth
- fixed chunking
- one embedding model
- SQLite vector search
- Recall@5/10/20
- HTML report
- email

Use a very small transcript set initially.

Goal:

> Prove that the complete evaluation pipeline works end-to-end.

---

## Phase 2 — Search Baselines

Add:

- SQLite FTS5
- hybrid retrieval
- MRR
- nDCG
- experiment matrix

Goal:

> Compare lexical, vector, and hybrid retrieval.

---

## Phase 3 — Chunking Evaluation

Add:

- speaker-aware chunking
- Q&A-aware chunking
- question-type breakdown
- difficulty breakdown

Goal:

> Determine which chunking strategy produces the best retrieval quality.

---

## Phase 4 — Embedding Evaluation

Add additional embedding models.

Goal:

> Determine whether embedding model choice materially affects retrieval quality.

---

## Phase 5 — End-to-End Answer Evaluation

Add:

- configurable answer-generation model
- answer correctness
- completeness
- groundedness
- evidence/citation accuracy

Goal:

> Determine whether improved retrieval translates into better user-facing answers.

---

# 28. Key Design Principles

### 1. S3 is the source of truth

All transcript data originates from S3.

### 2. SQLite is the only evaluation database

The test pipeline must not depend on PostgreSQL or production search infrastructure.

### 3. Keep the evaluation self-contained

A single DAG run should contain everything required to reproduce and inspect the experiment.

### 4. Separate retrieval from generation

First determine whether the correct evidence was retrieved.

Then determine whether the LLM can use that evidence correctly.

### 5. Evidence is the ground truth

The primary evaluation question is:

> **Can the retrieval system retrieve the transcript evidence required to answer the user's question?**

### 6. Make all experiments configurable

Models, chunking strategies, retrieval strategies, Top K values, S3 paths, and email recipients must come from DAG Run configuration.

### 7. Make the vector layer replaceable

SQLite is temporary infrastructure for evaluation and should be replaceable later without rewriting the evaluation framework.

### 8. Optimize for reliable measurement first

The first objective is to build a trustworthy evaluation framework, not to prematurely optimize production-scale search infrastructure.

---

# 29. Success Criteria

The implementation is successful when an Airflow 2.x DAG can be triggered with a DAG Run configuration and automatically execute:

```text
S3 transcripts
      ↓
Synthetic questions + evidence
      ↓
Multiple chunking strategies
      ↓
Embeddings
      ↓
SQLite
      ↓
FTS5 / Vector / Hybrid retrieval
      ↓
Recall@5/10/20 + MRR + nDCG
      ↓
Optional answer evaluation
      ↓
HTML/CSV report
      ↓
Email
```

The final report must allow the team to objectively answer:

> **Which chunking + embedding + retrieval configuration performs best for our transcript search use case?**