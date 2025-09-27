# =========================  zip_meetup_pipeline_15min.py  =========================
# DAG completo y documentado para:
# - Descomprimir un .zip con CSVs
# - Subirlos a S3
# - Crear/usar un External Stage en Snowflake
# - Cargar a tablas RAW con COPY INTO
# - Construir snapshots con sufijo {{ ts_nodash }}
# - Consolidar con MERGE en tablas fijas en CURATED
# - Ejecutar cada 15 minutos
# =================================================================================

# ---------- Importaciones estándar de Python ----------
from datetime import datetime, timedelta  # Para fechas y tiempos

# ---------- Núcleo de Airflow ----------
from airflow import DAG  # Objeto DAG
from airflow.models import Variable  # Para leer variables desde la UI de Airflow
import os, shutil, zipfile

# ---------- Operadores de Airflow ----------
from airflow.operators.empty import EmptyOperator           # Tareas "dummy" para marcar inicio/fin
from airflow.operators.bash import BashOperator            # Ejecutar comandos bash (usaremos 'unzip')
from airflow.operators.python import PythonOperator        # Ejecutar funciones Python (subida a S3)
from airflow.exceptions import AirflowException

# ---------- AWS / S3 ----------
from airflow.providers.amazon.aws.hooks.s3 import S3Hook   # Hook para interactuar con S3 (subir CSVs)

# ---------- Snowflake ----------
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# Operador que ejecuta SQL en Snowflake usando la conexión 'snowflake_default'

# --- Slack (webhook) ---
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# ============================= Parámetros vía Variables =============================
# Todas las Variables se leen con defaults razonables para facilitar pruebas locales.
SF_DATABASE       = Variable.get("sf_database",       default_var="MEETUP_DB")
SF_SCHEMA_RAW     = Variable.get("sf_schema_raw",     default_var="RAW")
SF_SCHEMA_CURATED = Variable.get("sf_schema_curated", default_var="CURATED")
SF_WAREHOUSE      = Variable.get("sf_warehouse",      default_var="COMPUTE_WH")

S3_BUCKET  = Variable.get("s3_zip_bucket",  default_var="mi-bucket-datos")  # Bucket destino para CSVs
S3_PREFIX  = Variable.get("s3_zip_prefix",  default_var="meetup/zip/")      # Prefijo dentro del bucket
ZIP_PATH   = Variable.get("zip_local_path", default_var="/opt/airflow/dags/files/archive.zip")  # Ruta al zip montado
EXTRACT_DIR = Variable.get("zip_extract_dir", default_var="/tmp/zip_load")  # Carpeta temporal donde se extraen CSVs

# Nombre del STORAGE INTEGRATION (Snowflake) para usar External Stage con S3
SF_STORAGE_INTEGRATION = Variable.get("sf_storage_integration", default_var="S3_INT")

# Nombre del External Stage que crearemos/actualizaremos en Snowflake
SF_STAGE_NAME = "ZIP_STAGE"

# ---------- Helpers de nombre totalmente calificado ----------
# Construimos nombres totalmente calificados (DB.SCHEMA.OBJETO) para evitar ambigüedades
def FQ(schema: str, name: str) -> str:
    """Devuelve un nombre totalmente calificado MEETUP_DB.SCHEMA.NOMBRE"""
    return f"{SF_DATABASE}.{schema}.{name}"

# Tablas RAW fijas (donde aterrizan los CSV)
T_RAW_CATEGORIES     = FQ(SF_SCHEMA_RAW,     "CATEGORIES")
T_RAW_CITIES         = FQ(SF_SCHEMA_RAW,     "CITIES")
T_RAW_EVENTS         = FQ(SF_SCHEMA_RAW,     "EVENTS")
T_RAW_GROUPS         = FQ(SF_SCHEMA_RAW,     "GROUPS")
T_RAW_GROUPS_TOPICS  = FQ(SF_SCHEMA_RAW,     "GROUPS_TOPICS")
T_RAW_MEMBERS        = FQ(SF_SCHEMA_RAW,     "MEMBERS")
T_RAW_MEMBERS_TOPICS = FQ(SF_SCHEMA_RAW,     "MEMBERS_TOPICS")
T_RAW_TOPICS         = FQ(SF_SCHEMA_RAW,     "TOPICS")
T_RAW_VENUES         = FQ(SF_SCHEMA_RAW,     "VENUES")

# Tablas CURATED fijas (consolidado estable)
T_DIM_CITY_BASE    = FQ(SF_SCHEMA_CURATED, "DIM_CITY")
T_DIM_TOPIC_BASE   = FQ(SF_SCHEMA_CURATED, "DIM_TOPIC")
T_FACT_EVENTS_BASE = FQ(SF_SCHEMA_CURATED, "FACT_EVENTS")
T_FACT_INCR_BASE   = FQ(SF_SCHEMA_CURATED, "FACT_EVENTS_INCR")

# ---------- Helper de path en S3 para un archivo ----------
def s3_key(filename: str) -> str:
    """ Construye la key 'prefijo/archivo.csv' asegurando el slash """
    prefix = S3_PREFIX
    if not prefix.endswith("/"):
        prefix += "/"
    return f"{prefix}{filename}"

# ================= Slack callbacks =================
def _slack_alert(text: str):
    """Envía un mensaje simple a Slack usando la conn 'slack_alerts'."""
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_alerts")
    hook.send(text=text)

def notify_slack_failure(context):
    """
    Callback de fallo: se invoca automáticamente cuando el DAG o una tarea falla.
    Usa `context` de Airflow para incluir información útil y link al log.
    """
    ti = context["task_instance"]
    msg = (
        ":rotating_light: *FALLÓ una tarea en Airflow*\n"
        f"*DAG*: `{ti.dag_id}`\n"
        f"*Task*: `{ti.task_id}`\n"
        f"*Run Id*: `{ti.run_id}`\n"
        f"*Execution Date*: `{context.get('execution_date')}`\n"
        f"*Try*: `{ti.try_number}`\n"
        f"*Log*: {ti.log_url}"
    )
    _slack_alert(msg)

def notify_slack_success(context):
    """
    Callback de éxito (opcional): se invoca cuando la corrida del DAG termina OK.
    Lo enganchamos desde un SlackWebhookOperator al final del flujo, para poder templar más fácil.
    """
    ti = context["task_instance"]
    msg = (
        ":white_check_mark: *DAG OK*\n"
        f"*DAG*: `{ti.dag_id}`\n"
        f"*Run Id*: `{ti.run_id}`\n"
        f"*Execution Date*: `{context.get('execution_date')}`"
    )
    _slack_alert(msg)

def _unzip_files():
    # Validar que el ZIP exista
    if not os.path.exists(ZIP_PATH):
        raise AirflowException(f"ZIP no encontrado: {ZIP_PATH}")

    # Limpiar y recrear carpeta destino
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    # Extraer
    try:
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            zf.extractall(EXTRACT_DIR)
    except zipfile.BadZipFile as e:
        raise AirflowException(f"Archivo ZIP inválido/corrupto: {ZIP_PATH}") from e

    # Log de verificación
    contents = os.listdir(EXTRACT_DIR)
    print(f"Extraído en: {EXTRACT_DIR}")
    for name in contents[:50]:
        print(" -", name)
    if not contents:
        raise AirflowException(f"ZIP extraído pero carpeta vacía: {EXTRACT_DIR}")

# =============================== Definición del DAG ================================
with DAG(
    dag_id="zip_meetup_pipeline_15min",                        # Identificador único del DAG
    description=(
        "Descomprime .zip, sube CSVs a S3, crea external stage y carga RAW con COPY, "
        "genera snapshots con sufijo {{ ts_nodash }} y consolida con MERGE. Corre cada 15 min."
    ),
    start_date=datetime(2025, 5, 1),                           # Fecha inicial (no hace catchup por defecto)
    schedule_interval=None,                          # Cada 15 minutos
    catchup=False,                                             # No re-ejecutar intervalos pasados automáticamente
    dagrun_timeout=timedelta(minutes=40),                      # Timeout por corrida
    #default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},  # Política simple de retries
    tags=["snowflake", "s3", "zip", "copy-into", "snapshots", "merge"],
    on_failure_callback=notify_slack_failure,
) as dag:

    # -------------------------- 1) Marcador de inicio --------------------------
    start = EmptyOperator(
        task_id="start"  # Tarea vacía, útil para leer la gráfica y anclar dependencias
    )

    # -------------------------- 2) Descomprimir el ZIP -------------------------
    # - El .zip debe estar montado en el contenedor (ver Variable ZIP_PATH).
    # - Extraemos todos los CSVs a EXTRACT_DIR, que limpiamos antes.
    unzip_files = PythonOperator(
        task_id="unzip_files",
        python_callable=_unzip_files,
    )

    # -------------------------- 3) Subir CSVs a S3 ----------------------------
    # - Usamos S3Hook de Airflow con la conexión 'aws_default'.
    # - Recorremos todos los archivos .csv y los cargamos al bucket/prefijo.
    def _upload_all_to_s3():
        """Sube todos los .csv extraídos a s3://S3_BUCKET/S3_PREFIX/"""
        import os
        hook = S3Hook(aws_conn_id="aws_default")
        for fname in os.listdir(EXTRACT_DIR):
            if fname.lower().endswith(".csv"):
                hook.load_file(
                    filename=f"{EXTRACT_DIR}/{fname}",    # Ruta local del archivo
                    key=s3_key(fname),                    # Clave en S3 (prefijo + nombre archivo)
                    bucket_name=S3_BUCKET,                # Bucket
                    replace=True,                         # Sobrescribir si ya existe
                )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_all_to_s3,  # Ejecuta la función Python anterior
    )

    # -------------------------- 4) Stage e Infra en Snowflake -------------------
    # - Crea Warehouse/DB/SCHEMAS si no existen.
    # - Crea FILE FORMAT estándar para CSV.
    # - Crea/actualiza STORAGE INTEGRATION (si aplica) y EXTERNAL STAGE apuntando a S3.
    # - El 'URL' del stage se arma con bucket+prefijo configurados en Variables.
    #   Nota: si ya creaste S3_INT fuera del DAG, puedes dejar CREATE OR REPLACE sin problemas.
    sf_prepare_stage_and_infra = SnowflakeOperator(
        task_id="sf_prepare_stage_and_infra",
        snowflake_conn_id="snowflake_default",  # Conexión validada en Airflow
        sql=f"""
    -- Selecciona el warehouse a usar
    USE WAREHOUSE {SF_WAREHOUSE};
    USE DATABASE {SF_DATABASE};
    USE SCHEMA {SF_DATABASE}.{SF_SCHEMA_RAW};

    -- FILE FORMAT CSV común en schema RAW
    CREATE OR REPLACE FILE FORMAT {SF_DATABASE}.{SF_SCHEMA_RAW}.CSV_ZIP_FF
        TYPE = CSV
        PARSE_HEADER = TRUE        -- Necesario para usar MATCH_BY_COLUMN_NAME
        SKIP_HEADER = 0            -- No saltar la primera fila; ya se parsea como cabecera
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL','null','');

    -- IMPORTANTE:
    -- YA NO creamos la STORAGE INTEGRATION aquí.
    -- Debe existir previamente (p. ej. {SF_STORAGE_INTEGRATION}) creada con ACCOUNTADMIN,
    -- y tu rol debe tener GRANT USAGE ON INTEGRATION.

    -- EXTERNAL STAGE apuntando al bucket/prefijo configurados, usando la integración existente
    CREATE OR REPLACE STAGE {SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}
    URL='s3://{S3_BUCKET}/{S3_PREFIX}'               -- nota la barra final
    STORAGE_INTEGRATION = {SF_STORAGE_INTEGRATION}
    FILE_FORMAT = {SF_DATABASE}.{SF_SCHEMA_RAW}.CSV_ZIP_FF
    ;
    """,
    )


    # -------------------------- 5) Cargar RAW con COPY INTO --------------------
    # - Definimos tablas RAW con columnas "mínimas" (ignoramos extras).
    # - Usamos COPY INTO con MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE para mapear por encabezado.
    sf_prepare_and_load_raw = SnowflakeOperator(
        task_id="sf_prepare_and_load_raw",
        snowflake_conn_id="snowflake_default",
        sql=f"""
        USE WAREHOUSE {SF_WAREHOUSE};

        -- CATEGORIES
        CREATE OR REPLACE TABLE {T_RAW_CATEGORIES} (
          category_id   STRING,
          category_name STRING,
          shortname     STRING,
          sort_name     STRING
        );
        COPY INTO {T_RAW_CATEGORIES}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/categories.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- CITIES
        CREATE OR REPLACE TABLE {T_RAW_CITIES} (
          city         STRING,
          city_id      STRING,
          country      STRING,
          latitude     FLOAT,
          longitude    FLOAT,
          member_count NUMBER,
          ranking      NUMBER,
          state        STRING,
          zip          STRING
        );
        COPY INTO {T_RAW_CITIES}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/cities.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- EVENTS
        CREATE OR REPLACE TABLE {T_RAW_EVENTS} (
          event_id        STRING,
          id              STRING,
          name            STRING,
          time            STRING,          -- lo parseamos luego
          yes_rsvp_count  NUMBER,
          group_id        STRING,
          group_name      STRING,
          city            STRING,
          country         STRING,
          venue_id        STRING
        );
        COPY INTO {T_RAW_EVENTS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/events.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- GROUPS
        CREATE OR REPLACE TABLE {T_RAW_GROUPS} (
          group_id     STRING,
          name         STRING,
          category_id  STRING,
          city         STRING,
          country      STRING,
          members      NUMBER
        );
        COPY INTO {T_RAW_GROUPS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/groups.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- GROUPS_TOPICS
        CREATE OR REPLACE TABLE {T_RAW_GROUPS_TOPICS} (
          topic_id   STRING,
          topic_key  STRING,
          topic_name STRING,
          group_id   STRING
        );
        COPY INTO {T_RAW_GROUPS_TOPICS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/groups_topics.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- MEMBERS
        CREATE OR REPLACE TABLE {T_RAW_MEMBERS} (
          member_id STRING,
          city      STRING,
          country   STRING,
          visited   STRING,
          group_id  STRING
        );
        COPY INTO {T_RAW_MEMBERS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/members.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- MEMBERS_TOPICS
        CREATE OR REPLACE TABLE {T_RAW_MEMBERS_TOPICS} (
          topic_id   STRING,
          topic_key  STRING,
          topic_name STRING,
          member_id  STRING
        );
        COPY INTO {T_RAW_MEMBERS_TOPICS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/members_topics.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- TOPICS
        CREATE OR REPLACE TABLE {T_RAW_TOPICS} (
          topic_id   STRING,
          topic_key  STRING,
          name       STRING
        );
        COPY INTO {T_RAW_TOPICS}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/topics.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';

        -- VENUES
        CREATE OR REPLACE TABLE {T_RAW_VENUES} (
          venue_id   STRING,
          name       STRING,
          address_1  STRING,
          city       STRING,
          country    STRING,
          latitude   FLOAT,
          longitude  FLOAT,
          state      STRING
        );
        COPY INTO {T_RAW_VENUES}
        FROM @{SF_DATABASE}.{SF_SCHEMA_RAW}.{SF_STAGE_NAME}/venues.csv
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE';
        """,
    )

    # -------------------------- 6) Snapshots con sufijo {{ ts_nodash }} --------
    # - Creamos tablas con nombres únicos por corrida (ej: DIM_CITY_20250925T021500Z)
    # - Añadimos campos “creativos” de negocio para justificar automatización
    sf_build_snapshots = SnowflakeOperator(
        task_id="sf_build_snapshots",
        snowflake_conn_id="snowflake_default",
        sql=f"""
        USE WAREHOUSE {SF_WAREHOUSE};

        -- Snapshot DIM_CITY
        CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_CITY_{{{{ ts_nodash }}}} AS
        WITH ev AS (
          SELECT
            COALESCE(event_id, id) AS event_id,
            UPPER(TRIM(city))      AS city,
            UPPER(TRIM(country))   AS country,
            COALESCE(yes_rsvp_count, 0) AS rsvp
          FROM {T_RAW_EVENTS}
          WHERE city IS NOT NULL AND country IS NOT NULL
        )
        SELECT
          UPPER(TRIM(c.city))    AS city_key,
          UPPER(TRIM(c.city))    AS city_name,
          UPPER(TRIM(c.country)) AS country_code,
          COALESCE(c.member_count,0) AS member_count,
          COUNT(ev.event_id)     AS total_events,
          SUM(COALESCE(ev.rsvp,0)) AS total_rsvp,
          /* score simple: RSVPs medios por evento */
          (SUM(COALESCE(ev.rsvp,0)) / NULLIF(COUNT(ev.event_id),0)) AS popularity_score,
          '{{{{ ts_nodash }}}}'  AS batch_id,
          CURRENT_TIMESTAMP()    AS snapshot_ts
        FROM {T_RAW_CITIES} c
        LEFT JOIN ev
          ON UPPER(TRIM(c.city))=ev.city AND UPPER(TRIM(c.country))=ev.country
        GROUP BY 1,2,3,4;

        -- Snapshot DIM_TOPIC
        CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_TOPIC_{{{{ ts_nodash }}}} AS
        WITH g AS (
          SELECT DISTINCT topic_id, topic_key, topic_name
          FROM {T_RAW_GROUPS_TOPICS}
        ),
        t AS (
          SELECT DISTINCT topic_id, topic_key, name
          FROM {T_RAW_TOPICS}
        )
        SELECT
          COALESCE(g.topic_id, t.topic_id)           AS topic_id,
          UPPER(COALESCE(g.topic_key, t.topic_key))  AS topic_key,
          COALESCE(g.topic_name, t.name)             AS topic_name,
          IFF(LENGTH(COALESCE(g.topic_name, t.name)) >= 10, 'LARGO', 'CORTO') AS topic_len_tag,
          '{{{{ ts_nodash }}}}' AS batch_id,
          CURRENT_TIMESTAMP()   AS snapshot_ts
        FROM g
        FULL OUTER JOIN t
          ON g.topic_id = t.topic_id;

        -- Snapshot FACT_EVENTS
        CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA_CURATED}.FACT_EVENTS_{{{{ ts_nodash }}}} AS
        WITH base AS (
          SELECT
            COALESCE(e.event_id, e.id) AS event_id,
            e.name                      AS event_name,
            TRY_TO_TIMESTAMP_NTZ(e.time) AS event_time_utc,
            UPPER(TRIM(e.city))         AS city_key,
            UPPER(TRIM(e.country))      AS country_code,
            COALESCE(e.yes_rsvp_count,0) AS rsvp_count,
            e.group_id,
            e.venue_id
          FROM {T_RAW_EVENTS} e
        )
        SELECT
          b.event_id, b.event_name, b.event_time_utc, b.city_key, b.country_code,
          b.rsvp_count,
          IFF(b.rsvp_count >= 50, TRUE, FALSE) AS is_popular,
          CASE
            WHEN b.rsvp_count >= 100 THEN '100+'
            WHEN b.rsvp_count >= 50  THEN '50-99'
            WHEN b.rsvp_count >= 10  THEN '10-49'
            ELSE '0-9'
          END AS rsvp_bucket,
          g.name      AS group_name,
          v.name      AS venue_name,
          '{{{{ ts_nodash }}}}' AS batch_id,
          CURRENT_TIMESTAMP()   AS snapshot_ts
        FROM base b
        LEFT JOIN {T_RAW_GROUPS} g  ON b.group_id = g.group_id
        LEFT JOIN {T_RAW_VENUES} v  ON b.venue_id = v.venue_id;
        """,
    )

    # -------------------------- 7) MERGE a tablas fijas CURATED ----------------
    # - Crea tablas base si no existen (LIKE snapshot de esta corrida)
    # - MERGE idempotente desde los snapshots recién creados
    sf_merge_consolidated = SnowflakeOperator(
        task_id="sf_merge_consolidated",
        snowflake_conn_id="snowflake_default",
        sql=f"""
        USE WAREHOUSE {SF_WAREHOUSE};

        -- Crear tablas base si no existen (copiando estructura del snapshot)
        CREATE TABLE IF NOT EXISTS {T_DIM_CITY_BASE}
        LIKE {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_CITY_{{{{ ts_nodash }}}};

        CREATE TABLE IF NOT EXISTS {T_DIM_TOPIC_BASE}
        LIKE {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_TOPIC_{{{{ ts_nodash }}}};

        CREATE TABLE IF NOT EXISTS {T_FACT_EVENTS_BASE}
        LIKE {SF_DATABASE}.{SF_SCHEMA_CURATED}.FACT_EVENTS_{{{{ ts_nodash }}}};

        CREATE TABLE IF NOT EXISTS {T_FACT_INCR_BASE}
        LIKE {SF_DATABASE}.{SF_SCHEMA_CURATED}.FACT_EVENTS_{{{{ ts_nodash }}}};

        -- MERGE DIM_CITY
        MERGE INTO {T_DIM_CITY_BASE} AS tgt
        USING {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_CITY_{{{{ ts_nodash }}}} AS src
        ON tgt.city_key = src.city_key
        WHEN MATCHED THEN UPDATE SET
          city_name        = src.city_name,
          country_code     = src.country_code,
          member_count     = src.member_count,
          total_events     = src.total_events,
          total_rsvp       = src.total_rsvp,
          popularity_score = src.popularity_score,
          batch_id         = src.batch_id,
          snapshot_ts      = src.snapshot_ts
        WHEN NOT MATCHED THEN INSERT (
          city_key, city_name, country_code, member_count, total_events,
          total_rsvp, popularity_score, batch_id, snapshot_ts
        ) VALUES (
          src.city_key, src.city_name, src.country_code, src.member_count, src.total_events,
          src.total_rsvp, src.popularity_score, src.batch_id, src.snapshot_ts
        );

        -- MERGE DIM_TOPIC
        MERGE INTO {T_DIM_TOPIC_BASE} AS tgt
        USING {SF_DATABASE}.{SF_SCHEMA_CURATED}.DIM_TOPIC_{{{{ ts_nodash }}}} AS src
        ON tgt.topic_id = src.topic_id AND tgt.topic_key = src.topic_key
        WHEN MATCHED THEN UPDATE SET
          topic_name   = src.topic_name,
          topic_len_tag= src.topic_len_tag,
          batch_id     = src.batch_id,
          snapshot_ts  = src.snapshot_ts
        WHEN NOT MATCHED THEN INSERT (
          topic_id, topic_key, topic_name, topic_len_tag, batch_id, snapshot_ts
        ) VALUES (
          src.topic_id, src.topic_key, src.topic_name, src.topic_len_tag, src.batch_id, src.snapshot_ts
        );

        -- MERGE FACT_EVENTS (base consolidada)
        MERGE INTO {T_FACT_EVENTS_BASE} AS tgt
        USING {SF_DATABASE}.{SF_SCHEMA_CURATED}.FACT_EVENTS_{{{{ ts_nodash }}}} AS src
        ON tgt.event_id = src.event_id
        WHEN MATCHED THEN UPDATE SET
          event_name    = src.event_name,
          event_time_utc= src.event_time_utc,
          city_key      = src.city_key,
          country_code  = src.country_code,
          rsvp_count    = src.rsvp_count,
          is_popular    = src.is_popular,
          rsvp_bucket   = src.rsvp_bucket,
          group_name    = src.group_name,
          venue_name    = src.venue_name,
          batch_id      = src.batch_id,
          snapshot_ts   = src.snapshot_ts
        WHEN NOT MATCHED THEN INSERT (
          event_id, event_name, event_time_utc, city_key, country_code, rsvp_count,
          is_popular, rsvp_bucket, group_name, venue_name, batch_id, snapshot_ts
        ) VALUES (
          src.event_id, src.event_name, src.event_time_utc, src.city_key, src.country_code, src.rsvp_count,
          src.is_popular, src.rsvp_bucket, src.group_name, src.venue_name, src.batch_id, src.snapshot_ts
        );

        -- MERGE FACT_EVENTS_INCR (histórico incremental)
        MERGE INTO {T_FACT_INCR_BASE} AS tgt
        USING {SF_DATABASE}.{SF_SCHEMA_CURATED}.FACT_EVENTS_{{{{ ts_nodash }}}} AS src
        ON tgt.event_id = src.event_id
        WHEN MATCHED THEN UPDATE SET
          event_name    = src.event_name,
          event_time_utc= src.event_time_utc,
          city_key      = src.city_key,
          country_code  = src.country_code,
          rsvp_count    = src.rsvp_count,
          is_popular    = src.is_popular,
          rsvp_bucket   = src.rsvp_bucket,
          group_name    = src.group_name,
          venue_name    = src.venue_name,
          batch_id      = src.batch_id,
          snapshot_ts   = src.snapshot_ts
        WHEN NOT MATCHED THEN INSERT (
          event_id, event_name, event_time_utc, city_key, country_code, rsvp_count,
          is_popular, rsvp_bucket, group_name, venue_name, batch_id, snapshot_ts
        ) VALUES (
          src.event_id, src.event_name, src.event_time_utc, src.city_key, src.country_code, src.rsvp_count,
          src.is_popular, src.rsvp_bucket, src.group_name, src.venue_name, src.batch_id, src.snapshot_ts
        );
        """,
    )

    # -------------------------- 8) Vista de conveniencia “LATEST” --------------
    # - Útil para consumidores que quieren “la última versión por event_id”
    sf_replace_view_latest = SnowflakeOperator(
        task_id="sf_replace_view_latest",
        snowflake_conn_id="snowflake_default",
        sql=f"""
        USE WAREHOUSE {SF_WAREHOUSE};
        CREATE OR REPLACE VIEW {FQ(SF_SCHEMA_CURATED, "FACT_EVENTS_LATEST")} AS
        SELECT *
        FROM {T_FACT_EVENTS_BASE}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY snapshot_ts DESC) = 1;
        """,
    )

    # Aviso de éxito por Slack usando el operador (puede templar valores Jinja)
    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id="slack_alerts",  # usa la Connection que creaste
        message=(
            ":white_check_mark: *DAG COMPLETADO*\n"
            "*DAG*: `{{ dag.dag_id }}`\n"
            "*Run Id*: `{{ run_id }}`\n"
            "*Execution Date*: `{{ ts }}`\n"
            "*Start*: `{{ dag_run.start_date }}`\n"
            "*End*: `{{ dag_run.end_date or macros.datetime.utcnow() }}`\n"
            "*Duration*: `{{ (dag_run.end_date - dag_run.start_date) if dag_run.end_date else 'n/a' }}`"
        ),
    )

    # -------------------------- 9) Marcador de fin -----------------------------
    end = EmptyOperator(
        task_id="end"
    )

    # -------------------------- Orquestación (dependencias) --------------------
    # start → unzip → upload S3 → preparar stage/infra → cargar RAW → snapshots → merge → view → end
    start >> unzip_files >> upload_to_s3 >> sf_prepare_stage_and_infra \
      >> sf_prepare_and_load_raw >> sf_build_snapshots >> sf_merge_consolidated \
      >> sf_replace_view_latest >> slack_success >> end
