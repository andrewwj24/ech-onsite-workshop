# Databricks notebook source
# MAGIC %md
# MAGIC # Clinical reporting workshop
# MAGIC
# MAGIC **Scenario:** The research team asks for monthly insight into tobacco
# MAGIC screening and cessation intervention. Some interventions are documented
# MAGIC only in clinical notes. Your task is to turn three prepared silver sources
# MAGIC into trustworthy patient-level measure results, then make those results
# MAGIC available to a dashboard and Genie through a metric view.
# MAGIC
# MAGIC The source tables are in the clinical schema created by setup. Run the
# MAGIC next cell once, enter its full catalog.schema name in the source_schema
# MAGIC widget, then run the remaining cells one at a time. Use serverless
# MAGIC notebook compute. In Genie Code prompts, replace <source_schema> with
# MAGIC the same catalog.schema name.

# COMMAND ----------

dbutils.widgets.text("source_schema", "")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Source map
# MAGIC
# MAGIC | Silver table | Grain | Useful columns |
# MAGIC |---|---|---|
# MAGIC | `silver_epic_encounters` | One outpatient encounter per patient per month | `patient_id`, `encounter_id`, `reporting_month`, `clinic`, `age_at_encounter`, `tobacco_screen_status`, `structured_cessation_intervention`, `has_copd`, `has_diabetes` |
# MAGIC | `silver_clinical_notes` | One or more notes per encounter | `note_id`, `patient_id`, `encounter_id`, `note_date`, `note_text` |
# MAGIC | `silver_sdoh_assessments` | One assessment per patient | `patient_id`, `transportation_barrier`, `housing_instability`, `food_insecurity` |
# MAGIC
# MAGIC All data is synthetic. Each patient has at most one qualifying
# MAGIC encounter per month, while an encounter can have multiple notes.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Research team's measure request
# MAGIC
# MAGIC Use the month of an outpatient encounter as the reporting period. A
# MAGIC patient is eligible for screening and the combined rate when they are at
# MAGIC least 12 years old at that encounter. The data has one encounter per
# MAGIC patient per month. Use these definitions for research and insight;
# MAGIC do not add a payer restriction or a formal submission rule.
# MAGIC
# MAGIC 1. **Screening rate:** Eligible patient-months with a known structured tobacco
# MAGIC    status (`current_user` or `non_user`) divided by all eligible patient-months.
# MAGIC    `unknown` and `not_done` do not meet the screening numerator.
# MAGIC 2. **Cessation intervention rate:** Patient-months identified as `current_user`
# MAGIC    who have a qualifying intervention in structured data or a note from
# MAGIC    the same encounter, divided by eligible patient-months identified as
# MAGIC    `current_user`.
# MAGIC 3. **Combined rate:** Eligible patient-months in which the patient has a
# MAGIC    known tobacco status and, if identified as `current_user`, a qualifying
# MAGIC    intervention, divided by all eligible patient-months.
# MAGIC
# MAGIC A **cessation care gap** is an eligible `current_user` patient-month
# MAGIC without a qualifying intervention.
# MAGIC
# MAGIC Qualifying note evidence describes counseling, a quitline or cessation
# MAGIC program referral, or cessation pharmacotherapy given to this patient
# MAGIC during the encounter. A declined offer, future plan, historical mention,
# MAGIC or treatment given to someone else does not qualify. Preserve the note ID
# MAGIC and supporting text so a result can be reviewed.
# MAGIC
# MAGIC The clinical question is how rates and care gaps change over time and
# MAGIC which clinic, clinical, and SDOH cohorts account for those patterns.
# MAGIC Cohort comparisons describe associations; they do not establish causes.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. See the prepared sources
# MAGIC
# MAGIC The next three queries show the encounter fields, tobacco status
# MAGIC values, and SDOH attributes available for reporting.

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema").strip()
if len(source_schema.split(".")) != 2 or any(not part for part in source_schema.split(".")):
    raise ValueError("Enter the full catalog.schema name in the source_schema widget.")
print(f"Reading clinical sources from {source_schema}")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   encounter_id, patient_id, reporting_month, clinic,
# MAGIC   age_at_encounter, tobacco_screen_status,
# MAGIC   structured_cessation_intervention, has_copd, has_diabetes
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epic_encounters')
# MAGIC ORDER BY encounter_id
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   tobacco_screen_status,
# MAGIC   COUNT(*) AS encounter_months,
# MAGIC   COUNT_IF(age_at_encounter >= 12) AS age_12_plus_months
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epic_encounters')
# MAGIC GROUP BY tobacco_screen_status
# MAGIC ORDER BY encounter_months DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   patient_id, assessment_date, transportation_barrier,
# MAGIC   housing_instability, food_insecurity
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_sdoh_assessments')
# MAGIC ORDER BY transportation_barrier DESC, patient_id
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 1 — sources and eligibility:** `silver_epic_encounters` has
# MAGIC one encounter per patient-month, with age, clinic, tobacco status,
# MAGIC structured intervention, and clinical factors. Patients age 12+ with
# MAGIC `current_user` or `non_user` count as screened; cessation uses eligible
# MAGIC `current_user` rows. `silver_sdoh_assessments` has one row per patient
# MAGIC and joins on `patient_id`.
# MAGIC
# MAGIC ### Ask Genie Code for a quick read
# MAGIC
# MAGIC Replace <source_schema> with the full schema name you entered above.
# MAGIC
# MAGIC > Inspect the three silver tables in <source_schema>. In a few bullets,
# MAGIC > summarize what one row represents in each table, the join keys, and
# MAGIC > the fields useful for tobacco reporting. Do not build anything yet.
# MAGIC
# MAGIC Check its summary against the SQL results and Notice 1.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. See what structured data misses
# MAGIC
# MAGIC The next query pairs notes with encounters for eligible current
# MAGIC tobacco users whose structured intervention field is false. Read the
# MAGIC note text: some rows describe actual care and others describe a
# MAGIC declined offer, future plan, or prior care. A keyword match alone
# MAGIC cannot define the measure.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   e.encounter_id, e.patient_id, e.reporting_month,
# MAGIC   e.structured_cessation_intervention,
# MAGIC   n.note_id, n.note_text
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epic_encounters') AS e
# MAGIC JOIN IDENTIFIER(:source_schema || '.silver_clinical_notes') AS n
# MAGIC   ON e.encounter_id = n.encounter_id
# MAGIC WHERE e.age_at_encounter >= 12
# MAGIC   AND e.tobacco_screen_status = 'current_user'
# MAGIC   AND e.structured_cessation_intervention = false
# MAGIC   AND n.note_text RLIKE 'cessation|quitline|nicotine|counsel'
# MAGIC ORDER BY e.encounter_id, n.note_id
# MAGIC LIMIT 12;

# COMMAND ----------
# MAGIC %md
# MAGIC The next cell runs an AI function on four contrasting notes from the
# MAGIC silver table. It only displays sample labels; it creates no pipeline
# MAGIC output.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Synthetic notes reuse templates; choose one note ID per example text.
# MAGIC WITH examples AS (
# MAGIC   SELECT
# MAGIC     MIN(note_id) AS note_id, note_text
# MAGIC   FROM IDENTIFIER(:source_schema || '.silver_clinical_notes')
# MAGIC   WHERE note_text IN (
# MAGIC     'Discussed stopping tobacco and referred the patient to the quitline during this visit.',
# MAGIC     'Current tobacco use documented. Patient declined cessation counseling today.',
# MAGIC     'Prior tobacco counseling is mentioned in history; no new cessation treatment today.',
# MAGIC     'A family member received cessation counseling; no intervention was given to this patient.'
# MAGIC   )
# MAGIC   GROUP BY note_text
# MAGIC )
# MAGIC SELECT
# MAGIC   note_id, note_text,
# MAGIC   ai_classify(
# MAGIC     note_text,
# MAGIC     '{"provided":"Cessation counseling, quitline referral, or cessation medication was actually provided to this patient during this encounter","not_provided":"No intervention for this patient during this encounter, including a declined offer, future plan, historical mention, or care for someone else"}',
# MAGIC     map('version', '2.0')
# MAGIC   ):response[0]::STRING AS suggested_label
# MAGIC FROM examples
# MAGIC ORDER BY note_id;

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 2 — note evidence:** Some interventions appear in
# MAGIC `silver_clinical_notes` even when the structured intervention field is
# MAGIC false. Qualifying evidence describes care given to this patient in
# MAGIC this encounter. Declined offers, future plans, historical mentions,
# MAGIC and care for someone else do not qualify. Use a built-in AI function
# MAGIC to create reusable silver note evidence; keep the note ID and
# MAGIC supporting phrase for review.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Check the reporting grain
# MAGIC
# MAGIC The encounter table defines one patient-month. Notes can have more
# MAGIC than one row for that encounter; SDOH has one assessment per patient.
# MAGIC Run these counts before designing the gold join.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS rows_after_join,
# MAGIC   COUNT(DISTINCT e.encounter_id) AS distinct_encounters
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_epic_encounters') AS e
# MAGIC LEFT JOIN IDENTIFIER(:source_schema || '.silver_clinical_notes') AS n
# MAGIC   ON e.encounter_id = n.encounter_id;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS sdoh_rows,
# MAGIC   COUNT(DISTINCT patient_id) AS distinct_patients
# MAGIC FROM IDENTIFIER(:source_schema || '.silver_sdoh_assessments');

# COMMAND ----------
# MAGIC %md
# MAGIC **Notice 3 — reporting grain:** A direct encounter-to-note join can
# MAGIC produce several rows for one encounter. Combine note evidence by
# MAGIC encounter before joining it to the encounter result. Gold must keep
# MAGIC one row per patient and reporting month; SDOH joins by `patient_id`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Build the pipeline with Genie Code
# MAGIC
# MAGIC Notices 1–3 are ready to paste into one Genie Code request. Add the
# MAGIC research team's measure request above, then use this opening:
# MAGIC
# MAGIC > Build a SQL Lakeflow Spark Declarative Pipeline from the three silver
# MAGIC > tables in <source_schema> through a gold patient-month result. Use
# MAGIC > materialized views for the batch outputs. Apply this measure request:
# MAGIC >
# MAGIC > [Paste the research team's measure request]
# MAGIC >
# MAGIC > Use these observations from the source queries:
# MAGIC >
# MAGIC > [Paste Notice 1]
# MAGIC > [Paste Notice 2]
# MAGIC > [Paste Notice 3]
# MAGIC >
# MAGIC > Keep clinic, clinical factors, and SDOH available for reporting.
# MAGIC > Check gold row count and patient-month uniqueness, numerator counts
# MAGIC > against denominators, and positive and negative note examples. Help
# MAGIC > me run and inspect the pipeline. Stop at gold.
# MAGIC
# MAGIC Review the generated SQL and pipeline graph. Check a note-only
# MAGIC positive case and a declined or historical mention before creating
# MAGIC the metric view.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ask Genie Code for a metric view
# MAGIC
# MAGIC Start a new Genie Code chat and paste this request:
# MAGIC
# MAGIC > I'm a clinical reporting analyst. Create one Unity Catalog metric
# MAGIC > view named <source_schema>.tobacco_reporting_metrics over the gold
# MAGIC > patient-month result built from the clinical silver tables in
# MAGIC > <source_schema>. Find the gold result and inspect its columns. It has
# MAGIC > one row per patient and reporting month, with numerator and
# MAGIC > denominator flags for screening, cessation intervention, and the
# MAGIC > combined measure, plus clinic, clinical, and SDOH attributes.
# MAGIC >
# MAGIC > We need monthly screening, cessation intervention, and combined
# MAGIC > rates, plus a count of cessation care gaps: eligible current tobacco
# MAGIC > users without a qualifying intervention. Include the numerator and
# MAGIC > denominator counts behind each rate. Let analysts group and filter
# MAGIC > by reporting month, clinic, clinical factors, and SDOH, with rates
# MAGIC > that recalculate correctly for each selection. Use clear names and
# MAGIC > descriptions so the view is ready for dashboards and Genie,
# MAGIC > including month-to-month and patient-group comparisons.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Ask Genie Code for a dashboard
# MAGIC
# MAGIC Start a separate Genie Code chat and paste this request:
# MAGIC
# MAGIC > I'm a clinical reporting analyst preparing a tobacco quality
# MAGIC > AI/BI dashboard for the research team. Use
# MAGIC > <source_schema>.tobacco_reporting_metrics and its measures
# MAGIC > throughout. The data covers October 2025 through September 2026
# MAGIC > and combines structured encounters,
# MAGIC > note-derived intervention evidence, and SDOH.
# MAGIC >
# MAGIC > Give us headline KPIs for the latest month's screening, cessation
# MAGIC > intervention, and combined rates, plus the number of cessation care
# MAGIC > gaps. Show numerator and denominator counts behind each rate and
# MAGIC > each rate's percentage-point change from the prior month. Add
# MAGIC > monthly trend charts for rates and care gaps. Show where the
# MAGIC > later-month cessation decline is concentrated across clinics and
# MAGIC > patient groups. Compare October 2025–March 2026 with
# MAGIC > April–September 2026, showing rate changes in percentage points
# MAGIC > and denominator counts.
# MAGIC > Make transportation barriers, other SDOH needs, and clinical factors
# MAGIC > such as COPD or diabetes available for investigation.
# MAGIC >
# MAGIC > Organize the dashboard into clear overview, trend, and cohort
# MAGIC > comparison sections. Add useful month, clinic, and patient-group
# MAGIC > filters; choose charts that make differences easy to see. Create
# MAGIC > the dashboard from the metric view.
# MAGIC
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Explore with Genie
# MAGIC
# MAGIC Link a Genie space to the same metric view. Inspect the counts behind
# MAGIC any surprising rate, and review a few note-supported patient cases
# MAGIC before interpreting cohort patterns.
# MAGIC
# MAGIC Suggested Genie questions:
# MAGIC
# MAGIC - How has the cessation intervention rate changed by month and clinic?
# MAGIC - Which clinic has the largest increase in patients with a cessation
# MAGIC   care gap in the later months?
# MAGIC - Within each clinic, how does the rate differ for patients with and
# MAGIC   without a transportation barrier?
# MAGIC - What are the numerator and denominator behind each monthly rate?
