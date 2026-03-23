# ============================================================
#  EXERCISE: Templating & DAG Params
# ============================================================
#
#  In this exercise you will build a DAG that queries a sales
#  database using runtime parameters and Jinja templating.
#
#  You will practice:
#    - Defining typed DAG params
#    - Reading a Jinja-templated SQL query
#    - Wiring params to a SQL operator
# ============================================================


# ── STEP 1 ───────────────────────────────────────────────────
# Import the following from the airflow package:
#   - dag           
#   - Param          
#   - SQLExecuteQueryOperator  
#### YOUR CODE HERE


# ── STEP 2 ───────────────────────────────────────────────────
# Read through the query below before moving on.
# Note how params are referenced inside the Jinja blocks —
# you will need to define matching Param objects in Step 3.
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
# Define the DAG using the @dag decorator.
#
# Pass a `params` argument.
# `params` should be a dict of Param objects — one for each
# query "params" referenced in QUERY above.
#### YOUR CODE HERE
def sales_report():

    # ── STEP 4 ─────────────────────────────────────────────
    # Instantiate the run_query task using SQLExecuteQueryOperator.
    # Ensure the operator uses the correct connection id and 
    # is configured to run the templated query above.
    #### YOUR CODE HERE


sales_report()