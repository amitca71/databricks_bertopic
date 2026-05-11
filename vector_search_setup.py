# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta

from databricks.vector_search.client import VectorSearchClient


def sql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def qualified_sql_name(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def qualified_name(*parts: str) -> str:
    return ".".join(parts)


def get_text_widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def get_bool_widget(name: str, default: str) -> bool:
    value = get_text_widget(name, default).lower()
    return value in {"1", "true", "yes", "y"}


def find_values_by_key(value, target_key: str):
    normalized_target_key = target_key.replace("_", "").lower()
    matches = []
    if isinstance(value, dict):
        for key, nested_value in value.items():
            normalized_key = str(key).replace("_", "").lower()
            if normalized_key == normalized_target_key:
                matches.append(nested_value)
            matches.extend(find_values_by_key(nested_value, target_key))
    elif isinstance(value, list):
        for nested_value in value:
            matches.extend(find_values_by_key(nested_value, target_key))
    return matches


dbutils.widgets.removeAll()

catalog = get_text_widget("source_catalog", "amit")
schema = get_text_widget("source_schema", "bertopic")
input_table_name = get_text_widget("input_table_name", "bertopic_input")
endpoint_name = get_text_widget("vector_search_endpoint_name", "bertopic_vector_search")
endpoint_type = get_text_widget("vector_search_endpoint_type", "STANDARD").upper()
index_name_param = get_text_widget("vector_search_index_name", "bertopic_input_index")
pipeline_type = get_text_widget("vector_search_pipeline_type", "TRIGGERED").upper()
primary_key = get_text_widget("vector_search_primary_key", "id")
embedding_source_column = get_text_widget("vector_search_embedding_source_column", "text")
embedding_model_endpoint_name = get_text_widget(
    "vector_search_embedding_model_endpoint_name",
    "databricks-qwen3-embedding-0-6b",
)
columns_to_sync_param = get_text_widget(
    "vector_search_columns_to_sync",
    "id,text,translated,incitement_label,incitement_confidence,incitement_prob_incitement",
)
sync_computed_embeddings = get_bool_widget("vector_search_sync_computed_embeddings", "false")
sync_index = get_bool_widget("vector_search_sync_index", "true")

source_table_name = qualified_name(catalog, schema, input_table_name)
source_table_sql = qualified_sql_name(catalog, schema, input_table_name)
index_name = (
    index_name_param
    if index_name_param.count(".") == 2
    else qualified_name(catalog, schema, index_name_param)
)

if endpoint_type not in {"STANDARD", "STORAGE_OPTIMIZED"}:
    raise ValueError(
        "vector_search_endpoint_type must be STANDARD or STORAGE_OPTIMIZED, "
        f"got {endpoint_type}"
    )

if pipeline_type not in {"TRIGGERED", "CONTINUOUS"}:
    raise ValueError(
        "vector_search_pipeline_type must be TRIGGERED or CONTINUOUS, "
        f"got {pipeline_type}"
    )

if endpoint_type == "STORAGE_OPTIMIZED" and pipeline_type != "TRIGGERED":
    raise ValueError("STORAGE_OPTIMIZED vector search endpoints require TRIGGERED sync")

source_df = spark.table(source_table_name)
field_names = {field.name for field in source_df.schema.fields}

required_columns = [primary_key, embedding_source_column]
missing_required_columns = [name for name in required_columns if name not in field_names]
if missing_required_columns:
    raise ValueError(
        f"{source_table_name} is missing required columns: {missing_required_columns}"
    )

spark.sql(
    f"ALTER TABLE {source_table_sql} "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

if "incitement" in field_names:
    metadata_columns = {
        "incitement_label": "STRING",
        "incitement_confidence": "DOUBLE",
        "incitement_prob_incitement": "DOUBLE",
    }
    for column_name, column_type in metadata_columns.items():
        if column_name not in field_names:
            spark.sql(
                f"ALTER TABLE {source_table_sql} "
                f"ADD COLUMNS ({sql_identifier(column_name)} {column_type})"
            )

    spark.sql(
        f"""
        UPDATE {source_table_sql}
        SET
          incitement_label = incitement.pred_label,
          incitement_confidence = incitement.confidence,
          incitement_prob_incitement = incitement.prob_incitement
        WHERE incitement IS NOT NULL
          AND (
            NOT (incitement_label <=> incitement.pred_label)
            OR NOT (incitement_confidence <=> incitement.confidence)
            OR NOT (incitement_prob_incitement <=> incitement.prob_incitement)
          )
        """
    )
else:
    print(
        f"{source_table_name} has no incitement column. "
        "Vector Search will be created without incitement metadata."
    )

source_df = spark.table(source_table_name)
field_names = {field.name for field in source_df.schema.fields}

columns_to_sync = [
    column.strip() for column in columns_to_sync_param.split(",") if column.strip()
]
missing_sync_columns = [name for name in columns_to_sync if name not in field_names]
if missing_sync_columns:
    raise ValueError(
        f"{source_table_name} is missing vector_search_columns_to_sync columns: "
        f"{missing_sync_columns}"
    )

columns_to_sync_arg = columns_to_sync or None

print(
    "Configuring Vector Search with "
    f"endpoint={endpoint_name}, index={index_name}, source={source_table_name}, "
    f"primary_key={primary_key}, embedding_source_column={embedding_source_column}, "
    f"embedding_model_endpoint_name={embedding_model_endpoint_name}, "
    f"columns_to_sync={columns_to_sync_arg}"
)

client = VectorSearchClient(disable_notice=True)

if client.endpoint_exists(endpoint_name):
    print(f"Vector Search endpoint already exists: {endpoint_name}")
    client.wait_for_endpoint(endpoint_name, verbose=True, timeout=timedelta(hours=1))
else:
    print(f"Creating Vector Search endpoint: {endpoint_name}")
    client.create_endpoint_and_wait(
        name=endpoint_name,
        endpoint_type=endpoint_type,
        verbose=True,
        timeout=timedelta(hours=1),
    )

if client.index_exists(index_name=index_name):
    print(f"Vector Search index already exists: {index_name}")
    index = client.get_index(index_name=index_name)
    index_description = index.describe()
    existing_vector_columns = [
        value for value in find_values_by_key(index_description, "embedding_vector_column") if value
    ]
    existing_source_columns = [
        value for value in find_values_by_key(index_description, "embedding_source_column") if value
    ]

    if existing_vector_columns:
        raise ValueError(
            f"{index_name} already exists as a self-managed embedding index using "
            f"embedding vector column(s) {existing_vector_columns}. Databricks cannot "
            "convert it to a managed embedding index in place. Delete the existing "
            "index or deploy with a different vector_search_index_name."
        )

    if existing_source_columns and embedding_source_column not in existing_source_columns:
        raise ValueError(
            f"{index_name} already exists with embedding source column(s) "
            f"{existing_source_columns}, but this deployment expects "
            f"{embedding_source_column}."
        )
else:
    print(f"Creating Vector Search index: {index_name}")
    index = client.create_delta_sync_index_and_wait(
        endpoint_name=endpoint_name,
        source_table_name=source_table_name,
        index_name=index_name,
        pipeline_type=pipeline_type,
        primary_key=primary_key,
        embedding_source_column=embedding_source_column,
        embedding_model_endpoint_name=embedding_model_endpoint_name,
        sync_computed_embeddings=sync_computed_embeddings,
        columns_to_sync=columns_to_sync_arg,
        verbose=True,
        timeout=timedelta(hours=4),
    )
    if index is None:
        index = client.get_index(index_name=index_name)

if sync_index and pipeline_type == "TRIGGERED":
    print(f"Syncing Vector Search index: {index_name}")
    index.sync()
    index.wait_until_ready(verbose=True, timeout=timedelta(hours=4))

print(f"Vector Search index is ready for agents: {index_name}")
print("Use incitement_label for metadata filters, for example: incitement_label = 'incitement'")
print("This is a Databricks-managed embedding index. Agents can query it with query_text.")
