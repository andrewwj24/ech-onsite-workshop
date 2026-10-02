# ECH onsite workshop setup

## Add the workshop to a Databricks workspace

1. In **Workspace**, open a folder where you can create content. Select
   **Create > Git folder**.
2. Paste `https://github.com/andrewwj24/ech-onsite-workshop.git`,
   choose **GitHub** as the provider, name the folder `ech_onsite`, and select
   **Create Git folder**. No GitHub credential is needed to clone this public
   repository.
3. Open `setup_ech_onsite.py` from the new Git folder. The `.py` files in this
   repository are Databricks source notebooks, so no separate import is needed.

Run the first code cell in `setup_ech_onsite.py` to display the widgets. Enter
a target Unity Catalog in `catalog`, and choose `both`, `clinical`, or `finance` in
`run_track`. Then select the following code cell and choose **Run all below**.
Serverless notebook compute is supported. Use an identity that can create
schemas and managed tables in the catalog and drop existing objects in the
selected schema. The two possible schemas are:

- `<catalog>.ech_onsite_clinical`
- `<catalog>.ech_onsite_financial`

The setup uses a fixed seed. It creates the following prepared silver Delta
tables with the default 6,000 clinical patients and 72,000 initial finance
accounts:

| Schema | Silver table | Grain | Approximate rows |
|---|---|---|---:|
| Clinical | `silver_epic_encounters` | Patient encounter | 27,000 |
| Clinical | `silver_clinical_notes` | Note | 30,000 |
| Clinical | `silver_sdoh_assessments` | Patient assessment | 6,000 |
| Finance | `silver_hsp_accounts` | Hospital account | 70,000 |
| Finance | `silver_pat_enc_hsp` | Primary encounter | 70,000 |
| Finance | `silver_clarity_departments` | Department mapping | 14 |
| Finance | `silver_epsi_account_financials` | Account financial results | 70,000 |

Clinical data covers October 2025–September 2026 and includes a visible
cessation trend and clinic, clinical, and SDOH cohort patterns. Finance data
covers October 2024–September 20, 2026. September 2026 is partial; August
2026 is the latest complete month. Emergency visits fall in the later 2026
months, concentrated in one facility and product, with a corresponding
financial impact. The `finance_account_count` widget changes the initial
number of accounts before the planned decline and partial-month cutoff.

The `run_track` widget controls the reset:

| Choice | Action |
|---|---|
| `both` (default) | Reset and seed the clinical schema, then reset and seed the finance schema |
| `clinical` | Reset and seed only the clinical schema |
| `finance` | Reset and seed only the finance schema |

The unselected schema is not created, read, cleared, or regenerated. In a
selected schema, setup drops all tables and views, including learner-created
gold tables, metric views, and any `bronze_*` tables left by an earlier
draft. It then recreates that track's silver starter tables. The schema and
its grants remain in place. Use `both` for the ECH team's first setup run;
choose one track when you need to rebuild it without affecting the other.
The clinical and finance generators are also in separate code cells, so an
admin can run the shared settings cell, the chosen track cell, and the final
checks cell individually after setting `run_track` to that track.

The setup notebook uses temporary source-shaped frames to produce silver
tables. Learners create SQL Lakeflow Spark Declarative Pipelines from these
prepared inputs through gold. The clinical exercise adds reusable note
evidence in its pipeline. Neither exercise requires bronze tables.

The final setup cell displays counts and checks only the selected track.
Clinical checks cover patient-month uniqueness, note links, and SDOH
uniqueness. Finance checks cover account and encounter keys, source joins,
and the planned Emergency trend.

The reset also removes materialized views and streaming tables when permitted.
If one is owned by a running pipeline or your identity lacks permission to
drop an object, setup stops with the object's name so you can resolve that
ownership or permission issue before rerunning.

Open `clinical_workshop.py` as the participant notebook. Enter
the full clinical catalog.schema name in its source_schema widget. Its SQL
cells let participants inspect the silver tables, try an AI function on four
notes, and see how a direct note join multiplies encounter rows. A short
Genie Code question summarizes the sources; short observations beside the SQL
results become the learner's request for a pipeline through gold. After they
review gold, they start a new Genie Code chat to create the metric view and
monthly reporting measures. In another chat, an analyst asks for a dashboard
over that view, including monthly and patient-group comparisons.
The setup does not create those learner outputs.

Open `finance_workshop.py` for the finance track and enter the
full finance catalog.schema name in its `source_schema` widget. Learners use
SQL to inspect accounts, primary encounters, department mappings, and EPSi
account results. Three short observations become a request for Genie Code to
build an account-grain gold pipeline. In a new chat, an analyst creates one
metric view with reusable volume, CMI, revenue, and cost definitions, plus dates
and dimensions for comparison. In another chat, the analyst asks for
a dashboard with complete-month MoM and YoY comparisons, matched-day MTD
versus prior-year MTD, headline volumes, a monthly trend, service line
variance, financial impact, and interactive filters. The dashboard uses the
metric view directly, and Genie can use the same view for follow-up questions.

The finance sources resemble Clarity account, encounter, and department data
plus EPSi account-level financial results. All data, fiscal rules, and amounts
are synthetic workshop definitions, not production ECH financial definitions.
The setup notebook requires no external files or third-party Python packages.
