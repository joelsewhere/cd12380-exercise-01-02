# ============================================================
#  SOLUTION: Templating & DAG Params
# ============================================================


# ── STEP 1 ───────────────────────────────────────────────────
from airflow.sdk import dag, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── STEP 2 ───────────────────────────────────────────────────
QUERY = """
SELECT
    region,
    SUM(sale_amount) AS total_sales
    {% if params.include_tax %},
    SUM(tax_amount) AS total_tax
    {% endif %}
FROM sales
WHERE sale_date BETWEEN '{{ params.start_date }}' AND '{{ params.end_date }}'
GROUP BY region
"""


# ── STEP 3 ───────────────────────────────────────────────────
@dag(
    params={
        "start_date":  Param("2024-01-01", type="string", format="date-time"),
        "end_date":    Param("2024-03-31", type="string", format="date-time"),
        "include_tax": Param(False, type="boolean"),
    },
)
def sales_report():

    # ── STEP 4 ─────────────────────────────────────────────
    SQLExecuteQueryOperator(
        task_id="run_query",
        conn_id="sales_db",
        sql=QUERY,
    )


sales_report()