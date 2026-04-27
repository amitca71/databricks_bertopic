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

This is implemented without changing notebook code. The job uses notebook tasks only and does not define clusters, so it is compatible with Databricks serverless compute and suitable for Free Edition environments where only serverless compute is available.

## Default bundle variables

The bundle currently defines these defaults in `databricks.yml`:

- `input_table_name=bertopic_input`
- `embedding_model=databricks-qwen3-embedding-0-6b`
- `model_result_table=topic_info_local`
- `instruct_model=databricks-meta-llama-3-3-70b-instruct`

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
