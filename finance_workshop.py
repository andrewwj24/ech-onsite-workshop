# Databricks notebook source
# MAGIC %md
# MAGIC # Service line reporting workshop
# MAGIC
# MAGIC **Scenario:** The finance team wants to understand how patient visits and
# MAGIC financial results change by service line, facility, product, and month.
# MAGIC Emergency visit volume falls in the recent synthetic data. Join four
# MAGIC prepared silver sources into a reliable hospital-account result,
# MAGIC define shared metrics, and investigate which groups account for the
# MAGIC change. The dashboard will show headline volumes, a monthly trend,
# MAGIC and a service line breakdown with comparisons.
# MAGIC
# MAGIC The setup notebook creates the sources. Run the next cell once, enter
# MAGIC the full finance catalog.schema name in `source_schema`, then run the
# MAGIC remaining cells one at a time. Use serverless notebook compute. In
# MAGIC Genie Code prompts, replace <source_schema> with that same name.

# COMMAND ----------

dbutils.widgets.text("source_schema", "")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reporting request
# MAGIC
# MAGIC A **hospital account** (often called a HAR) groups the billing results
# MAGIC for one hospital visit or stay. The synthetic sources have one primary
# MAGIC encounter and one EPSi financial row per account. The eventual gold
# MAGIC result must have **one row per hospital account**. Use discharge date
# MAGIC as the reporting date.
# MAGIC
# MAGIC The team needs these shared metrics:
# MAGIC
# MAGIC - **Patient volume:** distinct patients in the selected period and group.
# MAGIC - **Encounter volume:** distinct primary encounters.
# MAGIC - **Surgical volume:** primary encounters marked as surgical cases.
# MAGIC - **Average case mix index (CMI):** average MS-DRG weight for inpatient
# MAGIC   accounts with a weight. Other patient types do not enter this average.
# MAGIC - **Estimated net revenue, direct costs, and indirect costs:** sums of
# MAGIC   the corresponding account amounts.
# MAGIC - **Revenue after direct costs:** estimated net revenue minus direct
# MAGIC   costs. This is also called contribution to indirect costs; it is not
# MAGIC   profit because indirect costs remain.
# MAGIC
# MAGIC Let people filter and group by discharge date or month, facility,
# MAGIC service line, product, patient type, and financial class. Patient
# MAGIC volume must be recalculated as a distinct count for each selected
# MAGIC group; adding patient counts across groups can double-count people.
# MAGIC
# MAGIC The data ends on **September 20, 2026**. For an honest comparison:
# MAGIC
# MAGIC - **MoM:** compare the latest complete month, August 2026, with July 2026.
# MAGIC - **YoY:** compare August 2026 with August 2025, or matched April–August
# MAGIC   periods in the two years.
# MAGIC - **MTD versus prior-year MTD:** compare September 1–20, 2026 with
# MAGIC   September 1–20, 2025. Do not compare a partial September with all of
# MAGIC   September 2025.
# MAGIC
# MAGIC The figures and reporting rules here are illustrative, not ECH's
# MAGIC production financial definitions. Fiscal years in this data begin
# MAGIC October 1, so October 2025 belongs to FY2026.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the source tables
# MAGIC
# MAGIC | Silver table | One row represents | Key fields |
# MAGIC |---|---|---|
# MAGIC | `silver_hsp_accounts` | A hospital account | `hospital_account_id`, `primary_encounter_id`, `discharge_department_id` |
# MAGIC | `silver_pat_enc_hsp` | Its primary encounter | `encounter_id`, `hospital_account_id` |
# MAGIC | `silver_clarity_departments` | A department mapping | `department_id` |
# MAGIC | `silver_epsi_account_financials` | Account-level financial results | `hospital_account_id` |
# MAGIC
# MAGIC Account, encounter, and patient IDs answer different volume questions.

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema").strip()
if len(source_schema.split(".")) != 2 or any(not part for part in source_schema.split(".")):
    raise ValueError("Enter the full catalog.schema name in the source_schema widget.")
print(f"Reading finance sources from {source_schema}")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   hospital_account_id, patient_id, primary_encounter_id,
# MAGIC   discharge_department_id, admission_date, discharge_date, total_charges
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_hsp_accounts')
# MAGIC ORDER BY hospital_account_id
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   encounter_id, hospital_account_id, patient_type,
# MAGIC   financial_class_group, surgical_case_flag,
# MAGIC   ms_drg_weight, length_of_stay_days
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_pat_enc_hsp')
# MAGIC ORDER BY encounter_id
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT department_id, facility, service_line, product
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_clarity_departments')
# MAGIC ORDER BY facility, service_line, product;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   hospital_account_id, estimated_net_revenue,
# MAGIC   total_direct_cost, total_indirect_cost, total_actual_payment
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epsi_account_financials')
# MAGIC ORDER BY hospital_account_id
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 1 — source map:** `silver_hsp_accounts` supplies patient and
# MAGIC primary encounter IDs, discharge dates, department IDs, and charges.
# MAGIC `silver_pat_enc_hsp` supplies patient type, surgical flag, and MS-DRG
# MAGIC weight. `silver_clarity_departments` supplies facility, service line,
# MAGIC and product. `silver_epsi_account_financials` supplies revenue, costs,
# MAGIC and payment. Join accounts to encounters on account and primary
# MAGIC encounter IDs, to departments on department ID, and to EPSi on
# MAGIC account ID.
# MAGIC
# MAGIC ### Ask Genie Code for a quick source summary
# MAGIC
# MAGIC > Inspect the four silver tables in <source_schema>. In a few bullets,
# MAGIC > tell me what one row represents in each table, how they join, and
# MAGIC > which fields answer volume and financial questions. Do not build
# MAGIC > anything yet.
# MAGIC
# MAGIC Check its summary against the SQL results and Notice 1.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Check the grain and metric rules
# MAGIC
# MAGIC The account is the reporting grain. The next queries show whether
# MAGIC source keys and joins preserve that grain, and which patient types
# MAGIC carry the inputs for surgical volume and CMI.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'accounts' AS source, COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT hospital_account_id) AS distinct_keys
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_hsp_accounts')
# MAGIC UNION ALL
# MAGIC SELECT 'encounters', COUNT(*), COUNT(DISTINCT encounter_id)
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_pat_enc_hsp')
# MAGIC UNION ALL
# MAGIC SELECT 'departments', COUNT(*), COUNT(DISTINCT department_id)
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_clarity_departments')
# MAGIC UNION ALL
# MAGIC SELECT 'EPSi financials', COUNT(*), COUNT(DISTINCT hospital_account_id)
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epsi_account_financials');

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS joined_rows,
# MAGIC   COUNT(DISTINCT a.hospital_account_id) AS distinct_accounts,
# MAGIC   COUNT(DISTINCT a.primary_encounter_id) AS distinct_encounters,
# MAGIC   COUNT(DISTINCT a.patient_id) AS distinct_patients
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_hsp_accounts') AS a
# MAGIC JOIN IDENTIFIER(:source_schema || '.silver_pat_enc_hsp') AS e
# MAGIC   ON a.primary_encounter_id = e.encounter_id
# MAGIC  AND a.hospital_account_id = e.hospital_account_id
# MAGIC JOIN IDENTIFIER(:source_schema || '.silver_clarity_departments') AS d
# MAGIC   ON a.discharge_department_id = d.department_id
# MAGIC JOIN IDENTIFIER(:source_schema || '.silver_epsi_account_financials') AS f
# MAGIC   ON a.hospital_account_id = f.hospital_account_id;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   patient_type,
# MAGIC   COUNT(*) AS encounters,
# MAGIC   COUNT_IF(surgical_case_flag) AS surgical_cases,
# MAGIC   COUNT(ms_drg_weight) AS encounters_with_drg_weight,
# MAGIC   ROUND(AVG(ms_drg_weight), 2) AS average_cmi
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_pat_enc_hsp')
# MAGIC GROUP BY patient_type
# MAGIC ORDER BY encounters DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 2 — grain and metric inputs:** The join should return one
# MAGIC row per hospital account. Count patients distinctly; a patient can
# MAGIC have more than one encounter. Surgical cases are inpatient in this
# MAGIC data, and only inpatient rows have an MS-DRG weight for CMI. Keep
# MAGIC account financial amounts at one row per account so joins do not
# MAGIC repeat revenue or costs.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Review time comparisons
# MAGIC
# MAGIC The next queries show the discharge-date cutoff and a recent
# MAGIC Emergency pattern. The metric view will own the shared measure
# MAGIC definitions after gold exists.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(discharge_date) AS first_discharge,
# MAGIC   MAX(discharge_date) AS latest_discharge,
# MAGIC   DATE_TRUNC('MONTH', MAX(discharge_date)) AS latest_month_start
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_hsp_accounts');

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   YEAR(a.discharge_date) AS discharge_year,
# MAGIC   MONTH(a.discharge_date) AS discharge_month,
# MAGIC   d.facility,
# MAGIC   d.product,
# MAGIC   COUNT(*) AS emergency_encounters
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_hsp_accounts') AS a
# MAGIC JOIN IDENTIFIER(:source_schema || '.silver_clarity_departments') AS d
# MAGIC   ON a.discharge_department_id = d.department_id
# MAGIC WHERE d.service_line = 'Emergency'
# MAGIC   AND a.discharge_date >= DATE'2025-04-01'
# MAGIC   AND a.discharge_date < DATE'2026-09-01'
# MAGIC   AND MONTH(a.discharge_date) BETWEEN 4 AND 8
# MAGIC GROUP BY ALL
# MAGIC ORDER BY discharge_month, discharge_year, d.facility, d.product;

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 3 — reporting periods and trend:** Discharge date is the
# MAGIC reporting date. September 2026 ends on the 20th, so MTD needs the
# MAGIC same day cutoff in the prior year; August is the latest complete
# MAGIC month for MoM and YoY. The April–August Emergency decline is
# MAGIC concentrated in Main Hospital's `ED Visit` product. This is an
# MAGIC observed contributor to the trend, not the cause of patient behavior.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Ask Genie Code to build through gold
# MAGIC
# MAGIC Notices 1–3 are ready to paste into one Genie Code request. Add the
# MAGIC reporting request above, then use this opening:
# MAGIC
# MAGIC > Build a SQL Lakeflow Spark Declarative Pipeline from the four silver
# MAGIC > tables in <source_schema> through one gold hospital-account result.
# MAGIC > Use materialized views for the batch outputs. Apply this reporting
# MAGIC > request:
# MAGIC >
# MAGIC > [Paste the reporting request]
# MAGIC >
# MAGIC > Use these observations from the source queries:
# MAGIC >
# MAGIC > [Paste Notice 1]
# MAGIC > [Paste Notice 2]
# MAGIC > [Paste Notice 3]
# MAGIC >
# MAGIC > Keep discharge date and month, and derive fiscal year and month
# MAGIC > using the October 1 calendar. Include revenue after direct costs
# MAGIC > as an account amount. Check account uniqueness, source and gold
# MAGIC > counts, and charge and revenue totals. Help me run and inspect the
# MAGIC > pipeline. Stop at gold.
# MAGIC
# MAGIC Review the generated SQL, pipeline graph, and gold rows before
# MAGIC creating the metric view.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ask Genie Code for a metric view
# MAGIC
# MAGIC > Create one Unity Catalog metric view for the finance team's service
# MAGIC > line reporting. Use the gold hospital-account result from the
# MAGIC > pipeline built from the four silver tables in <source_schema>. Find
# MAGIC > and inspect that gold result before creating the view. Each row is
# MAGIC > one hospital account, with patient and encounter IDs, discharge
# MAGIC > dates, facility, service line, product, patient type, surgical flag,
# MAGIC > MS-DRG weight, and account-level revenue and costs.
# MAGIC >
# MAGIC > Analysts need to explore patient and encounter volume, surgical
# MAGIC > cases, case mix, estimated net revenue, costs, and revenue after
# MAGIC > direct costs across months, service lines, and other useful groups.
# MAGIC > Make discharge date and month, facility, service line, product,
# MAGIC > patient type, and financial class useful dimensions. Include
# MAGIC > distinct patient and encounter counts, surgical cases, inpatient
# MAGIC > case mix index, estimated net revenue, direct and indirect costs,
# MAGIC > and revenue after direct costs as reusable measures. Count patients
# MAGIC > distinctly for each selection, and calculate case mix only from
# MAGIC > inpatient accounts with a DRG weight. Give the metrics clear names
# MAGIC > and descriptions.
# MAGIC >
# MAGIC > Provide dashboard-ready comparison SQL from the view:
# MAGIC > August 2026 versus July 2026 (MoM) and August 2025 (YoY);
# MAGIC > September 1–20, 2026 versus the same days in 2025 (MTD); and
# MAGIC > Emergency visits in April–August 2026 versus those months in 2025
# MAGIC > by facility and product. Return current and prior values, absolute
# MAGIC > changes, and percentage changes for encounter volume, estimated
# MAGIC > net revenue, and revenue after direct costs, both overall and by
# MAGIC > service line, facility, product, and patient type. Align the
# MAGIC > partial-month days because the data ends September 20, 2026. Create
# MAGIC > the view and give me the comparison SQL for the dashboard.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Ask Genie Code for a dashboard
# MAGIC
# MAGIC > I'm a service line analyst preparing a financial reporting AI/BI
# MAGIC > dashboard. Use the metric view built over the gold hospital-account
# MAGIC > result from <source_schema>. Find and inspect it, and use its
# MAGIC > measures throughout. The data covers October 2024 through
# MAGIC > September 20, 2026.
# MAGIC >
# MAGIC > Give us headline KPIs for distinct patients, encounters, surgical
# MAGIC > cases, inpatient case mix index, estimated net revenue, direct and
# MAGIC > indirect costs, and revenue after direct costs. Show monthly trend
# MAGIC > charts for volume and the main financial metrics. Add comparison
# MAGIC > views by service line, facility, and product with actual values,
# MAGIC > prior values, and absolute and percentage changes. For complete
# MAGIC > months, compare August 2026 with July 2026 and August 2025. Compare
# MAGIC > September 1–20, 2026 with the same days in 2025 so the partial
# MAGIC > month is aligned.
# MAGIC >
# MAGIC > Include an Emergency focus: compare April–August 2026 with the
# MAGIC > same months in 2025, show which facility and product account for
# MAGIC > the observed visit decline, and show the change in estimated net
# MAGIC > revenue and revenue after direct costs for those visits. Organize
# MAGIC > the dashboard into clear overview, trend, and comparison sections
# MAGIC > with useful date, service line, facility, product, and patient type
# MAGIC > filters. Use the comparison SQL from the metric view step, label
# MAGIC > every comparison period and cutoff, and create the dashboard.
# MAGIC
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Explore with Genie
# MAGIC
# MAGIC Link a Genie space to the same metric view. Try questions such as:
# MAGIC
# MAGIC - How much did Emergency encounter volume change from April–August
# MAGIC   2025 to April–August 2026?
# MAGIC - Which facility and product account for most of that change?
# MAGIC - How did estimated revenue and revenue after direct costs change for
# MAGIC   those Emergency visits?
# MAGIC - What is September MTD encounter volume versus the same days last year?
# MAGIC - Which service lines grew in volume but fell in revenue after direct
# MAGIC   costs?
# MAGIC
# MAGIC Ask for the counts and periods behind any explanation. The results
# MAGIC identify observed contributors to a trend, not the cause of patient
# MAGIC behavior.
