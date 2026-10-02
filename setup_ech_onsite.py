# Databricks notebook source
# MAGIC %md
# MAGIC # ECH onsite starter data
# MAGIC
# MAGIC Run this notebook before the workshop or rerun one track to reset its
# MAGIC starter data. It creates prepared silver source tables for the clinical
# MAGIC and finance workshops in the catalog you enter.
# MAGIC
# MAGIC **To run interactively:** Run the next cell once to display its widgets,
# MAGIC enter your catalog in `catalog`, and choose `both`, `clinical`, or
# MAGIC `finance` in `run_track`. Then select the following code cell and choose
# MAGIC **Run all below**. Only the selected schema is cleared and rebuilt;
# MAGIC `both` rebuilds each schema in turn. Serverless notebook compute is
# MAGIC supported. Use an identity that can create schemas and managed tables
# MAGIC and drop existing objects in the selected schema, including learner
# MAGIC outputs.
# MAGIC The clinical and finance generators are separate code cells. After
# MAGIC running the shared settings cell, you may instead run just the chosen
# MAGIC track cell and the final checks cell.
# MAGIC
# MAGIC Both exercises start from silver. Their gold results, metric views,
# MAGIC dashboards, and Genie experiences are built by learners.
# MAGIC The synthetic source-shaped frames are used only inside this setup run;
# MAGIC learners build the managed Spark Declarative Pipeline from the silver tables.

# COMMAND ----------

import re

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "")
dbutils.widgets.text("clinical_schema", "ech_onsite_clinical")
dbutils.widgets.text("financial_schema", "ech_onsite_financial")
dbutils.widgets.dropdown("run_track", "both", ["both", "clinical", "finance"])
dbutils.widgets.text("patient_count", "6000")
dbutils.widgets.text("finance_account_count", "72000")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog").strip()
CLINICAL_SCHEMA = dbutils.widgets.get("clinical_schema").strip()
FINANCIAL_SCHEMA = dbutils.widgets.get("financial_schema").strip()
RUN_TRACK = dbutils.widgets.get("run_track").strip().lower()
if RUN_TRACK not in {"both", "clinical", "finance"}:
    raise ValueError("run_track must be both, clinical, or finance.")
RUN_CLINICAL = RUN_TRACK in {"both", "clinical"}
RUN_FINANCE = RUN_TRACK in {"both", "finance"}
PATIENT_COUNT = int(dbutils.widgets.get("patient_count").strip()) if RUN_CLINICAL else None
FINANCE_ACCOUNT_COUNT = (
    int(dbutils.widgets.get("finance_account_count").strip()) if RUN_FINANCE else None
)
SEED = 42
FINANCE_AS_OF_DATE = "2026-09-20"


def quoted(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", identifier):
        raise ValueError(f"Invalid Unity Catalog identifier: {identifier!r}")
    return f"`{identifier}`"


if not CATALOG:
    raise ValueError("Enter the target Unity Catalog in the catalog widget.")
if RUN_CLINICAL and RUN_FINANCE and CLINICAL_SCHEMA == FINANCIAL_SCHEMA:
    raise ValueError("Clinical and financial schema names must differ.")
if RUN_CLINICAL and not re.fullmatch(r"ech_onsite_clinical(?:[_-][A-Za-z0-9_-]+)?", CLINICAL_SCHEMA):
    raise ValueError("clinical_schema must be ech_onsite_clinical or a suffixed onsite schema.")
if RUN_FINANCE and not re.fullmatch(r"ech_onsite_financial(?:[_-][A-Za-z0-9_-]+)?", FINANCIAL_SCHEMA):
    raise ValueError("financial_schema must be ech_onsite_financial or a suffixed onsite schema.")
if RUN_CLINICAL and not 1000 <= PATIENT_COUNT <= 100000:
    raise ValueError("patient_count must be between 1,000 and 100,000.")
if RUN_FINANCE and not 24000 <= FINANCE_ACCOUNT_COUNT <= 200000:
    raise ValueError("finance_account_count must be between 24,000 and 200,000.")

CATALOG_SQL = quoted(CATALOG)


def table_name(schema: str, table: str) -> str:
    return f"{CATALOG_SQL}.{quoted(schema)}.{quoted(table)}"


DROP_KIND = {
    "VIEW": "VIEW",
    "METRIC VIEW": "VIEW",
    "MATERIALIZED VIEW": "MATERIALIZED VIEW",
    "STREAMING TABLE": "STREAMING TABLE",
    "BASE TABLE": "TABLE",
    "MANAGED": "TABLE",
    "EXTERNAL": "TABLE",
}
DROP_ORDER = {"VIEW": 0, "MATERIALIZED VIEW": 1, "STREAMING TABLE": 2, "TABLE": 3}


def reset_schema_tables(schema: str) -> None:
    namespace = f"{CATALOG_SQL}.{quoted(schema)}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
    existing = spark.sql(
        f"SELECT table_name, table_type FROM {CATALOG_SQL}.information_schema.tables "
        f"WHERE table_schema = '{schema}'"
    ).collect()
    objects = []
    for row in existing:
        table_type = row["table_type"].upper().replace("_", " ")
        if table_type not in DROP_KIND:
            raise RuntimeError(
                f"Cannot reset {namespace}: unsupported object type {table_type} "
                f"for {row['table_name']}."
            )
        objects.append((row["table_name"], table_type, DROP_KIND[table_type]))

    for object_name, table_type, drop_kind in sorted(
        objects, key=lambda item: (DROP_ORDER[item[2]], item[0])
    ):
        object_sql = "`" + object_name.replace("`", "``") + "`"
        full_name = f"{namespace}.{object_sql}"
        try:
            spark.sql(f"DROP {drop_kind} IF EXISTS {full_name}")
        except Exception as exc:
            raise RuntimeError(
                f"Could not drop {table_type} {full_name}. Check permissions and "
                "stop any pipeline that owns it before rerunning setup."
            ) from exc
    print(f"Cleared {len(objects)} table/view objects from {CATALOG}.{schema}")


def write_starter(frame, table: str) -> None:
    target = table_name(CLINICAL_SCHEMA, table)
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )


def uniform(salt: str, *columns):
    """Stable value in [0, 1), independent of Spark partitioning."""
    hashed = F.xxhash64(F.lit(SEED), F.lit(salt), *columns)
    return F.pmod(hashed, F.lit(1_000_000)).cast("double") / F.lit(1_000_000.0)


def choose(options: list[str], index):
    return F.element_at(
        F.array(*[F.lit(option) for option in options]),
        (F.pmod(index, F.lit(len(options))) + F.lit(1)).cast("int"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Clinical starter data
# MAGIC
# MAGIC This section resets and seeds only the clinical schema when `run_track`
# MAGIC is `clinical` or `both`.
# MAGIC
# MAGIC The source data covers October 2025 through September 2026. Screening is
# MAGIC fairly steady. Cessation intervention falls in one clinic during the later
# MAGIC months and is lower among patients with transportation barriers. These
# MAGIC patterns give analysts something to investigate by clinic, time, clinical
# MAGIC factors, and SDOH. Some qualifying interventions appear only in notes.
# MAGIC Note documentation patterns remain stable over time so the research
# MAGIC question centers on patient cohorts and care gaps.

# COMMAND ----------

if RUN_CLINICAL:
    reset_schema_tables(CLINICAL_SCHEMA)
    patient_id = F.col("patient_id")
    people = spark.range(PATIENT_COUNT).select(
        (F.col("id") + F.lit(100_001)).cast("long").alias("patient_id")
    )

    people = (
        people
        .withColumn(
            "age_on_2026_01_01",
            (F.floor(uniform("age", patient_id) * F.lit(78)) + F.lit(8)).cast("int"),
        )
        .withColumn(
            "birth_date",
            F.make_date(F.lit(2026) - F.col("age_on_2026_01_01"), F.lit(1), F.lit(1)),
        )
        .withColumn(
            "clinic_code",
            F.when(uniform("clinic", patient_id) < 0.30, "NORTH")
            .when(uniform("clinic", patient_id) < 0.65, "SOUTH")
            .when(uniform("clinic", patient_id) < 0.83, "EAST")
            .otherwise("WEST"),
        )
        .withColumn("has_copd", uniform("copd", patient_id) < 0.17)
        .withColumn("has_diabetes", uniform("diabetes", patient_id) < 0.23)
        .withColumn("transportation_barrier", uniform("transport", patient_id) < 0.21)
        .withColumn(
            "housing_instability",
            uniform("housing", patient_id)
            < (F.lit(0.10) + F.when(F.col("transportation_barrier"), 0.08).otherwise(0.0)),
        )
        .withColumn(
            "food_insecurity",
            uniform("food", patient_id)
            < (F.lit(0.13) + F.when(F.col("transportation_barrier"), 0.07).otherwise(0.0)),
        )
        .withColumn(
            "coverage_type",
            F.when(
                (F.col("age_on_2026_01_01") >= 65)
                & (uniform("coverage", patient_id) < 0.70),
                "Medicare",
            )
            .when(uniform("coverage", patient_id) < 0.52, "Medi-Cal")
            .when(uniform("coverage", patient_id) < 0.91, "Commercial")
            .otherwise("Other"),
        )
        .withColumn(
            "is_tobacco_user",
            uniform("tobacco", patient_id)
            < (
                F.lit(0.18)
                + F.when(F.col("has_copd"), 0.14).otherwise(0.0)
                + F.when(F.col("transportation_barrier"), 0.05).otherwise(0.0)
            ),
        )
    )

    raw_sdoh = people.select(
        (patient_id * F.lit(10) + F.lit(1)).cast("long").alias("ASSESSMENT_ID"),
        patient_id.alias("PAT_ID"),
        F.lit("2025-09-15").alias("ASSESSMENT_DATE"),
        F.when(F.col("transportation_barrier"), "Y").otherwise("N").alias("TRANSPORTATION_BARRIER_C"),
        F.when(F.col("housing_instability"), "Y").otherwise("N").alias("HOUSING_INSTABILITY_C"),
        F.when(F.col("food_insecurity"), "Y").otherwise("N").alias("FOOD_INSECURITY_C"),
    )

if RUN_CLINICAL:
    months = spark.range(12).select(F.col("id").cast("int").alias("month_index"))
    encounters = (
        people.crossJoin(F.broadcast(months))
        .where(uniform("visit", patient_id, F.col("month_index")) < 0.37)
        .withColumn(
            "reporting_month",
            F.add_months(F.to_date(F.lit("2025-10-01")), F.col("month_index")),
        )
        .withColumn(
            "encounter_date",
            F.date_add(
                F.col("reporting_month"),
                (F.floor(uniform("day", patient_id, F.col("month_index")) * 24) + 1).cast("int"),
            ),
        )
        .withColumn(
            "encounter_id",
            (patient_id * F.lit(100) + F.col("month_index") + F.lit(1)).cast("long"),
        )
        .withColumn("screen_roll", uniform("screen", patient_id, F.col("month_index")))
        .withColumn(
            "tobacco_screen_code",
            F.when(
                F.col("screen_roll") < 0.81,
                F.when(F.col("is_tobacco_user"), "CURRENT").otherwise("NON_USER"),
            )
            .when(F.col("screen_roll") < 0.85, "UNKNOWN")
            .otherwise("NOT_DONE"),
        )
        .withColumn(
            "intervention_probability",
            F.greatest(
                F.lit(0.20),
                F.least(
                    F.lit(0.90),
                    F.lit(0.76)
                    - F.when(
                        (F.col("clinic_code") == "SOUTH") & (F.col("month_index") >= 6),
                        0.28,
                    ).otherwise(0.0)
                    - F.when(F.col("transportation_barrier"), 0.12).otherwise(0.0)
                    + F.when(F.col("has_copd"), 0.04).otherwise(0.0),
                ),
            ),
        )
        .withColumn(
            "synthetic_intervention",
            (F.col("tobacco_screen_code") == "CURRENT")
            & (
                uniform("intervention", patient_id, F.col("month_index"))
                < F.col("intervention_probability")
            ),
        )
        .withColumn(
            "structured_intervention",
            F.col("synthetic_intervention")
            & (uniform("documentation", patient_id, F.col("month_index")) < 0.60),
        )
    )

    raw_encounters = encounters.select(
        F.col("encounter_id").alias("PAT_ENC_CSN_ID"),
        patient_id.alias("PAT_ID"),
        F.date_format(F.col("encounter_date"), "yyyy-MM-dd").alias("CONTACT_DATE"),
        F.date_format(F.col("birth_date"), "yyyy-MM-dd").alias("BIRTH_DATE"),
        F.col("clinic_code").alias("CLINIC_CODE"),
        F.col("coverage_type").alias("COVERAGE_TYPE"),
        F.when(F.col("has_copd"), "Y").otherwise("N").alias("COPD_YN"),
        F.when(F.col("has_diabetes"), "Y").otherwise("N").alias("DIABETES_YN"),
        F.col("tobacco_screen_code").alias("TOBACCO_SCREEN_C"),
        F.when(F.col("structured_intervention"), "Y").otherwise("N").alias("CESSATION_INTERVENTION_YN"),
    )

if RUN_CLINICAL:
    positive_notes = [
        "Current tobacco use reviewed today. Brief cessation counseling provided; patient made a quit plan.",
        "Discussed stopping tobacco and referred the patient to the quitline during this visit.",
        "Tobacco cessation intervention today: nicotine replacement therapy started and use reviewed.",
        "Counseled the patient on tobacco cessation today and arranged follow-up support.",
        "Patient received brief advice to quit tobacco and accepted a cessation program referral.",
    ]
    no_intervention_notes = [
        "Current tobacco use documented. Patient declined cessation counseling today.",
        "Smoking status reviewed. No cessation intervention was provided at this visit.",
        "Prior tobacco counseling is mentioned in history; no new cessation treatment today.",
        "Plan to discuss quitting at a future visit. No counseling today.",
        "A family member received cessation counseling; no intervention was given to this patient.",
    ]
    general_notes = [
        "Routine follow-up completed and clinical history reviewed.",
        "Medication list reviewed and follow-up plan discussed.",
        "General visit documentation completed. No tobacco intervention described.",
    ]
    structured_intervention_notes = [
        "Current tobacco use recorded. Clinical history and follow-up plan reviewed.",
        "Patient currently uses tobacco. Routine visit documentation completed.",
        "Tobacco status recorded as current use. Medication list reconciled.",
    ]

    note_choice = (uniform("note_template", F.col("encounter_id")) * 1000).cast("int")
    main_note_text = (
        F.when(
            F.col("synthetic_intervention") & ~F.col("structured_intervention"),
            choose(positive_notes, note_choice),
        )
        .when(
            (F.col("tobacco_screen_code") == "CURRENT")
            & ~F.col("synthetic_intervention"),
            choose(no_intervention_notes, note_choice),
        )
        .when(
            F.col("tobacco_screen_code") == "CURRENT",
            choose(structured_intervention_notes, note_choice),
        )
        .otherwise(choose(general_notes, note_choice))
    )

    main_notes = encounters.select(
        (F.col("encounter_id") * F.lit(10) + F.lit(1)).cast("long").alias("NOTE_ID"),
        patient_id.alias("PAT_ID"),
        F.col("encounter_id").alias("PAT_ENC_CSN_ID"),
        F.date_format(F.col("encounter_date"), "yyyy-MM-dd").alias("NOTE_DATE"),
        F.lit("Progress Note").alias("NOTE_TYPE"),
        main_note_text.alias("NOTE_TEXT"),
    )

    secondary_notes = (
        encounters
        .where(uniform("second_note", F.col("encounter_id")) < 0.14)
        .select(
            (F.col("encounter_id") * F.lit(10) + F.lit(2)).cast("long").alias("NOTE_ID"),
            patient_id.alias("PAT_ID"),
            F.col("encounter_id").alias("PAT_ENC_CSN_ID"),
            F.date_format(F.col("encounter_date"), "yyyy-MM-dd").alias("NOTE_DATE"),
            F.lit("After Visit Summary").alias("NOTE_TYPE"),
            F.lit("Follow-up instructions and medication reconciliation reviewed.").alias("NOTE_TEXT"),
        )
    )

    raw_notes = main_notes.unionByName(secondary_notes)

# Prepared silver inputs: the three clinical source domains stay separate.
# Note text remains unclassified for the learner pipeline.

if RUN_CLINICAL:
    silver_encounters = raw_encounters.select(
        F.col("PAT_ENC_CSN_ID").cast("long").alias("encounter_id"),
        F.col("PAT_ID").cast("long").alias("patient_id"),
        F.to_date("CONTACT_DATE", "yyyy-MM-dd").alias("encounter_date"),
        F.to_date("BIRTH_DATE", "yyyy-MM-dd").alias("birth_date"),
        F.col("CLINIC_CODE").alias("clinic"),
        F.col("COVERAGE_TYPE").alias("coverage_type"),
        (F.col("COPD_YN") == "Y").alias("has_copd"),
        (F.col("DIABETES_YN") == "Y").alias("has_diabetes"),
        F.when(F.col("TOBACCO_SCREEN_C") == "CURRENT", "current_user")
        .when(F.col("TOBACCO_SCREEN_C") == "NON_USER", "non_user")
        .when(F.col("TOBACCO_SCREEN_C") == "UNKNOWN", "unknown")
        .otherwise("not_done")
        .alias("tobacco_screen_status"),
        (F.col("CESSATION_INTERVENTION_YN") == "Y").alias("structured_cessation_intervention"),
    )
    silver_encounters = (
        silver_encounters
        .withColumn("reporting_month", F.trunc(F.col("encounter_date"), "month"))
        .withColumn(
            "age_at_encounter",
            F.floor(F.months_between(F.col("encounter_date"), F.col("birth_date")) / 12).cast("int"),
        )
    )
    write_starter(silver_encounters, "silver_epic_encounters")

    silver_sdoh = raw_sdoh.select(
        F.col("ASSESSMENT_ID").cast("long").alias("assessment_id"),
        F.col("PAT_ID").cast("long").alias("patient_id"),
        F.to_date("ASSESSMENT_DATE", "yyyy-MM-dd").alias("assessment_date"),
        (F.col("TRANSPORTATION_BARRIER_C") == "Y").alias("transportation_barrier"),
        (F.col("HOUSING_INSTABILITY_C") == "Y").alias("housing_instability"),
        (F.col("FOOD_INSECURITY_C") == "Y").alias("food_insecurity"),
    )
    write_starter(silver_sdoh, "silver_sdoh_assessments")

    silver_notes = raw_notes.select(
        F.col("NOTE_ID").cast("long").alias("note_id"),
        F.col("PAT_ID").cast("long").alias("patient_id"),
        F.col("PAT_ENC_CSN_ID").cast("long").alias("encounter_id"),
        F.to_date("NOTE_DATE", "yyyy-MM-dd").alias("note_date"),
        F.col("NOTE_TYPE").alias("note_type"),
        F.col("NOTE_TEXT").alias("note_text"),
    )
    write_starter(silver_notes, "silver_clinical_notes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Finance starter data
# MAGIC
# MAGIC This section resets and seeds only the finance schema when `run_track`
# MAGIC is `finance` or `both`.
# MAGIC
# MAGIC The finance sources cover October 2024 through September 20, 2026.
# MAGIC September 2026 is deliberately partial, so learners can compare
# MAGIC month-to-date with the same days a year earlier. August 2026 is the
# MAGIC latest complete month for month-over-month and year-over-year views.
# MAGIC Emergency visits fall in the later 2026 months, concentrated in one
# MAGIC facility and product. The financial impact is visible in estimated
# MAGIC revenue and costs. This is synthetic evidence for analysis, not a
# MAGIC claim about what caused a real change at El Camino Health.
# MAGIC
# MAGIC The four silver tables resemble a small part of a Clarity-to-EPSi
# MAGIC reporting flow. The learner pipeline will join them at one hospital
# MAGIC account per row. A hospital account, often called a HAR, groups the
# MAGIC billing results for a hospital visit or stay.

# COMMAND ----------

if RUN_FINANCE:
    reset_schema_tables(FINANCIAL_SCHEMA)
    finance_department_rows = [
        (101, "Main Hospital", "Emergency", "ED Visit"),
        (102, "Main Hospital", "Emergency", "Observation"),
        (201, "North Campus", "Emergency", "ED Visit"),
        (202, "North Campus", "Emergency", "Observation"),
        (103, "Main Hospital", "Heart and Vascular", "Cardiology"),
        (104, "Main Hospital", "Heart and Vascular", "Cardiac Surgery"),
        (203, "North Campus", "Heart and Vascular", "Cardiology"),
        (105, "Main Hospital", "Orthopedics", "Joint Replacement"),
        (106, "Main Hospital", "Orthopedics", "Sports Medicine"),
        (205, "North Campus", "Orthopedics", "Joint Replacement"),
        (107, "Main Hospital", "Women's Health", "Maternity"),
        (207, "North Campus", "Women's Health", "Maternity"),
        (108, "Main Hospital", "Neurosciences", "Neurology"),
        (109, "Main Hospital", "General Medicine", "General Medicine"),
    ]
    silver_clarity_departments = spark.createDataFrame(
        finance_department_rows,
        "department_id INT, facility STRING, service_line STRING, product STRING",
    )

    finance_id = F.col("seed_account_id")
    finance_dept_draw = uniform("finance_department", finance_id)
    finance_base = (
        spark.range(FINANCE_ACCOUNT_COUNT)
        .select(F.col("id").cast("long").alias("seed_account_id"))
        .withColumn("month_index", F.pmod(finance_id, F.lit(24)).cast("int"))
        .withColumn(
            "discharge_month",
            F.add_months(F.to_date(F.lit("2024-10-01")), F.col("month_index")),
        )
        .withColumn(
            "discharge_date",
            F.date_add(
                F.col("discharge_month"),
                F.floor(
                    uniform("finance_day", finance_id)
                    * F.dayofmonth(F.last_day(F.col("discharge_month")))
                ).cast("int"),
            ),
        )
        .withColumn(
            "department_id",
            F.when(finance_dept_draw < 0.20, 101)
            .when(finance_dept_draw < 0.24, 102)
            .when(finance_dept_draw < 0.35, 201)
            .when(finance_dept_draw < 0.37, 202)
            .when(finance_dept_draw < 0.48, 103)
            .when(finance_dept_draw < 0.50, 104)
            .when(finance_dept_draw < 0.57, 203)
            .when(finance_dept_draw < 0.65, 105)
            .when(finance_dept_draw < 0.67, 106)
            .when(finance_dept_draw < 0.73, 205)
            .when(finance_dept_draw < 0.82, 107)
            .when(finance_dept_draw < 0.87, 207)
            .when(finance_dept_draw < 0.93, 108)
            .otherwise(109)
            .cast("int"),
        )
        .join(F.broadcast(silver_clarity_departments), "department_id")
        .where(F.col("discharge_date") <= F.to_date(F.lit(FINANCE_AS_OF_DATE)))
        .where(
            ~(
                (F.col("department_id") == 101)
                & (F.col("discharge_date") >= F.to_date(F.lit("2026-04-01")))
                & (uniform("finance_ed_decline", finance_id) < 0.45)
            )
        )
        .withColumn("hospital_account_id", finance_id + F.lit(10_000_001))
        .withColumn("primary_encounter_id", finance_id + F.lit(20_000_001))
        .withColumn(
            "patient_id",
            (F.floor(uniform("finance_patient", finance_id) * F.lit(40_000)) + F.lit(400_001)).cast("long"),
        )
        .withColumn(
            "patient_type",
            F.when(F.col("product") == "ED Visit", "Emergency")
            .when(F.col("product") == "Observation", "Observation")
            .when(
                F.col("product").isin(
                    "Cardiac Surgery", "Joint Replacement", "Maternity", "Neurology", "General Medicine"
                ),
                "Inpatient",
            )
            .otherwise("Outpatient"),
        )
        .withColumn(
            "surgical_case_flag",
            F.col("product").isin("Cardiac Surgery", "Joint Replacement"),
        )
        .withColumn(
            "length_of_stay_days",
            F.when(
                F.col("patient_type") == "Inpatient",
                F.floor(uniform("finance_los", finance_id) * F.lit(6)).cast("int") + F.lit(1),
            )
            .when(F.col("patient_type") == "Observation", F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn("admission_date", F.date_sub(F.col("discharge_date"), F.col("length_of_stay_days")))
        .withColumn(
            "financial_class_group",
            F.when(uniform("finance_payer", finance_id) < 0.42, "Commercial")
            .when(uniform("finance_payer", finance_id) < 0.75, "Medicare")
            .when(uniform("finance_payer", finance_id) < 0.95, "Medi-Cal")
            .otherwise("Other"),
        )
        .withColumn(
            "ms_drg_weight",
            F.when(
                F.col("patient_type") == "Inpatient",
                F.round(
                    F.lit(0.8)
                    + uniform("finance_drg_weight", finance_id) * F.lit(2.2)
                    + F.when(F.col("surgical_case_flag"), F.lit(0.3)).otherwise(F.lit(0.0)),
                    2,
                ),
            ).cast("double"),
        )
        .withColumn(
            "charge_baseline",
            F.when(F.col("product") == "ED Visit", 3400)
            .when(F.col("product") == "Observation", 7100)
            .when(F.col("product") == "Cardiology", 12000)
            .when(F.col("product") == "Cardiac Surgery", 75000)
            .when(F.col("product") == "Joint Replacement", 56000)
            .when(F.col("product") == "Sports Medicine", 9800)
            .when(F.col("product") == "Maternity", 27000)
            .when(F.col("product") == "Neurology", 20000)
            .otherwise(16000),
        )
        .withColumn(
            "total_charges",
            F.round(
                F.col("charge_baseline")
                * (F.lit(0.75) + uniform("finance_charge", finance_id) * F.lit(0.50)),
                2,
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "revenue_fraction",
            F.when(F.col("financial_class_group") == "Commercial", 0.55)
            .when(F.col("financial_class_group") == "Medicare", 0.38)
            .when(F.col("financial_class_group") == "Medi-Cal", 0.30)
            .otherwise(0.25),
        )
        .withColumn(
            "estimated_net_revenue",
            F.round(F.col("total_charges") * F.col("revenue_fraction"), 2).cast("decimal(18,2)"),
        )
        .withColumn(
            "direct_cost_fraction",
            F.when(F.col("surgical_case_flag"), F.lit(0.61))
            .when(F.col("patient_type") == "Emergency", F.lit(0.54))
            .otherwise(F.lit(0.52))
            + uniform("finance_direct_cost", finance_id) * F.lit(0.08),
        )
        .withColumn(
            "total_direct_cost",
            F.round(F.col("estimated_net_revenue") * F.col("direct_cost_fraction"), 2).cast("decimal(18,2)"),
        )
        .withColumn(
            "total_indirect_cost",
            F.round(
                F.col("estimated_net_revenue")
                * (F.lit(0.18) + uniform("finance_indirect_cost", finance_id) * F.lit(0.07)),
                2,
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "total_actual_payment",
            F.round(
                F.col("estimated_net_revenue")
                * (F.lit(0.90) + uniform("finance_payment", finance_id) * F.lit(0.06)),
                2,
            ).cast("decimal(18,2)"),
        )
    )

    silver_hsp_accounts = finance_base.select(
        F.col("hospital_account_id"),
        F.col("patient_id"),
        F.col("primary_encounter_id"),
        F.col("department_id").alias("discharge_department_id"),
        F.col("admission_date"),
        F.col("discharge_date"),
        F.col("total_charges"),
    )
    silver_pat_enc_hsp = finance_base.select(
        F.col("primary_encounter_id").alias("encounter_id"),
        F.col("hospital_account_id"),
        F.col("patient_type"),
        F.col("financial_class_group"),
        F.col("surgical_case_flag"),
        F.col("ms_drg_weight"),
        F.col("length_of_stay_days"),
    )
    silver_epsi_account_financials = finance_base.select(
        F.col("hospital_account_id"),
        F.col("estimated_net_revenue"),
        F.col("total_direct_cost"),
        F.col("total_indirect_cost"),
        F.col("total_actual_payment"),
    )


    def write_finance_starter(frame, table: str) -> None:
        target = table_name(FINANCIAL_SCHEMA, table)
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target)
        )


    write_finance_starter(silver_hsp_accounts, "silver_hsp_accounts")
    write_finance_starter(silver_pat_enc_hsp, "silver_pat_enc_hsp")
    write_finance_starter(silver_clarity_departments, "silver_clarity_departments")
    write_finance_starter(silver_epsi_account_financials, "silver_epsi_account_financials")

# COMMAND ----------

manifest_rows = []

if RUN_CLINICAL:
    clinical_tables = [
        "silver_epic_encounters",
        "silver_clinical_notes",
        "silver_sdoh_assessments",
    ]
    manifest_rows.extend(
        (f"{CATALOG}.{CLINICAL_SCHEMA}.{name}", spark.table(table_name(CLINICAL_SCHEMA, name)).count())
        for name in clinical_tables
    )
    saved_encounters = spark.table(table_name(CLINICAL_SCHEMA, "silver_epic_encounters"))
    saved_notes = spark.table(table_name(CLINICAL_SCHEMA, "silver_clinical_notes"))
    saved_sdoh = spark.table(table_name(CLINICAL_SCHEMA, "silver_sdoh_assessments"))
    if saved_encounters.groupBy("patient_id", "reporting_month").count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("A patient has more than one encounter in a reporting month.")
    if saved_notes.join(saved_encounters.select("encounter_id"), "encounter_id", "left_anti").limit(1).count():
        raise AssertionError("A clinical note has no matching encounter.")
    if saved_sdoh.groupBy("patient_id").count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("A patient has more than one SDOH assessment.")
    print(f"Created clinical starter data in {CATALOG}.{CLINICAL_SCHEMA}")

if RUN_FINANCE:
    finance_tables = [
        "silver_hsp_accounts",
        "silver_pat_enc_hsp",
        "silver_clarity_departments",
        "silver_epsi_account_financials",
    ]
    manifest_rows.extend(
        (f"{CATALOG}.{FINANCIAL_SCHEMA}.{name}", spark.table(table_name(FINANCIAL_SCHEMA, name)).count())
        for name in finance_tables
    )
    saved_finance_accounts = spark.table(table_name(FINANCIAL_SCHEMA, "silver_hsp_accounts"))
    saved_finance_encounters = spark.table(table_name(FINANCIAL_SCHEMA, "silver_pat_enc_hsp"))
    saved_finance_departments = spark.table(table_name(FINANCIAL_SCHEMA, "silver_clarity_departments"))
    saved_finance_actuals = spark.table(table_name(FINANCIAL_SCHEMA, "silver_epsi_account_financials"))
    if saved_finance_accounts.groupBy("hospital_account_id").count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("A finance hospital account ID appears more than once.")
    if saved_finance_encounters.groupBy("encounter_id").count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("A finance encounter ID appears more than once.")
    if saved_finance_actuals.groupBy("hospital_account_id").count().where(F.col("count") > 1).limit(1).count():
        raise AssertionError("An EPSi account has more than one finance row.")
    if saved_finance_accounts.join(
        saved_finance_encounters,
        saved_finance_accounts.primary_encounter_id == saved_finance_encounters.encounter_id,
        "left_anti",
    ).limit(1).count():
        raise AssertionError("A finance account has no matching primary encounter.")
    if saved_finance_accounts.join(
        saved_finance_actuals, "hospital_account_id", "left_anti"
    ).limit(1).count():
        raise AssertionError("A finance account has no EPSi financial row.")
    if saved_finance_accounts.join(
        saved_finance_departments,
        saved_finance_accounts.discharge_department_id == saved_finance_departments.department_id,
        "left_anti",
    ).limit(1).count():
        raise AssertionError("A finance account has no department mapping.")

    finance_story = (
        saved_finance_accounts.join(
            saved_finance_departments,
            saved_finance_accounts.discharge_department_id == saved_finance_departments.department_id,
        )
        .where((F.col("facility") == "Main Hospital") & (F.col("product") == "ED Visit"))
        .where(F.month("discharge_date").between(4, 8))
        .agg(
            F.sum(F.when(F.year("discharge_date") == 2025, 1).otherwise(0)).alias("prior_year"),
            F.sum(F.when(F.year("discharge_date") == 2026, 1).otherwise(0)).alias("current_year"),
        )
        .first()
    )
    if not finance_story["prior_year"] or finance_story["current_year"] >= finance_story["prior_year"]:
        raise AssertionError("The planned Main Hospital ED volume decline is not present.")
    print(f"Created finance starter data in {CATALOG}.{FINANCIAL_SCHEMA}")
    print(f"Finance demo snapshot: through {FINANCE_AS_OF_DATE}; latest complete month is August 2026.")
    print(
        "Main Hospital ED visits, April–August: "
        f"2025={finance_story['prior_year']}, 2026={finance_story['current_year']}"
    )

display(spark.createDataFrame(manifest_rows, ["table_name", "row_count"]))
print("Open the participant notebook for the selected track.")
