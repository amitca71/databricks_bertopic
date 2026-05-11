# databricks_bertopic

This repository contains Databricks notebook workflow for BERTopic-based analysis, plus a Databricks Asset Bundle that deploys the workflow as a serverless job.

## What is in the repo

- `embedding.ipynb`
  Creates embeddings from the input table.
- `bertopic.ipynb`
  Runs BERTopic on the embedded data.
- `model_evaluation.ipynb`
  Evaluates BERTopic output.
- `incitement.ipynb`
  Runs a separate incitement-related analysis.
- `vector_search_setup.py`
  Creates and syncs a Databricks Vector Search index for agent retrieval.
- `reduced_embedding.ipynb`
  Additional embedding-related notebook.
- `import_hf_model.ipynb`
  Helper notebook for importing a Hugging Face model.
- `create_cost.ipynb`
  Notebook related to cost tracking or setup.
- `helper.ipynb`
  Utility notebook.
- `cost.lvdash.json`
  Dashboard asset.

## Databricks bundle

- `databricks.yml`
  Top-level bundle definition and default variables.
- `resources/bertopic_job.yml`
  Job resource definition for the notebook workflow.

The deployed job name is `bertopic_pipeline`.

## Current job graph

The bundle deploys a serverless Databricks job with this task graph:

- `embedding -> bertopic -> evaluation`
- `incitement` runs independently in parallel
- `vector_search_setup` runs after `incitement`

This is implemented without changing notebook code. The job uses notebook tasks only and does not define clusters, so it is compatible with Databricks serverless compute and suitable for Free Edition environments where only serverless compute is available.

## Default bundle variables

The bundle currently defines these defaults in `databricks.yml`:

- `input_table_name=bertopic_input`
- `embedding_model=databricks-qwen3-embedding-0-6b`
- `model_result_table=topic_info_local`
- `instruct_model=databricks-meta-llama-3-3-70b-instruct`
- `vector_search_endpoint_name=bertopic_vector_search`
- `vector_search_endpoint_type=STANDARD`
- `vector_search_index_name=bertopic_input_index`
- `vector_search_pipeline_type=TRIGGERED`
- `vector_search_embedding_source_column=text`
- `embedding_model=databricks-qwen3-embedding-0-6b` is also passed to Vector Search as the managed embedding model endpoint
- `vector_search_columns_to_sync=id,text,translated,incitement_label,incitement_confidence,incitement_prob_incitement`

## Vector Search

The Vector Search setup task creates a Databricks-managed embedding index over the `text` column in `amit.bertopic.bertopic_input`. It uses the same bundle `embedding_model` value as `embedding.ipynb`, so agents can query the index with `query_text`.

For agent-friendly filtering, the task derives scalar metadata columns from the existing `incitement` struct:

- `incitement_label` from `incitement.pred_label`
- `incitement_confidence` from `incitement.confidence`
- `incitement_prob_incitement` from `incitement.prob_incitement`

The default index name is `amit.bertopic.bertopic_input_index`. Agents can filter retrieval by `incitement_label`, for example `incitement_label = 'incitement'`.

Databricks cannot convert an existing self-managed embedding index to a managed embedding index in place. If `amit.bertopic.bertopic_input_index` was already created with `text_embedding`, delete that index first or deploy with a different `vector_search_index_name`.

The native dashboard at `dashboard/native_topic_and_toxicity.lvdash.json` includes a `semantic_vector_search` dataset and a `Semantic Search` page that call the same default index through the SQL `vector_search()` function. If you override `vector_search_index_name`, update that dashboard query as well.

## Validate and deploy

Validate:

```bash
databricks bundle validate --profile dbc-de54b796-a6c4
```

Deploy:

```bash
DATABRICKS_TF_EXEC_PATH=/usr/local/bin/terraform \
DATABRICKS_TF_VERSION=1.5.7 \
databricks bundle deploy --profile dbc-de54b796-a6c4
```

The Terraform environment variables are currently required on this machine to bypass a Databricks CLI Terraform download signature issue.

## Notes

- The notebooks are the source of truth for the workflow logic.
- The bundle only wires orchestration and parameters around those notebooks.
- The current default deployment target is `dev`.
