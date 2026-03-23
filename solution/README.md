# Exercise: Templating & DAG Params

In this exercise you will build a DAG that accepts runtime parameters and uses them to populate a pre-written Jinja-templated SQL query.

---

## Files

| File | Description |
|---|---|
| `dag.py` | Your working file — fill in the blanks |
---

## Instructions

1. Open `dag.py` and read through the pre-written `QUERY` in **Step 2**. Before writing any Python, make sure you can answer:
   - Which params does the query expect?
   - What does the `{% if %}` block control?

2. Work through **Steps 1, 3, and 4**, replacing every `#### YOUR CODE HERE` block with real code. The query itself requires no changes.

3. In **Step 3**, the `Param` objects you define must match the param names already used in `QUERY` — if the names don't match, Airflow will not render the template correctly.