# Service line reporting: facilitator prompt examples

Use these after the finance gold table exists. Before pasting a prompt, replace
`<gold_table>` and `<metric_view>` with fully qualified Unity Catalog names
(`catalog.schema.object`). The gold table and metric view may be in different
schemas. Start a fresh Genie Code chat for each asset.

## Metric view

> I'm a service line analyst. Create one Unity Catalog metric view at
> `<metric_view>` from `<gold_table>`. Inspect the gold table first. It has one row
> per hospital account, with patient and encounter IDs, discharge date,
> facility, service line, product, patient type, financial class, surgical
> flag, MS-DRG weight, and account-level revenue and costs.
>
> Define reusable measures for distinct patients and encounters, surgical
> cases, inpatient case mix index, estimated net revenue, direct and indirect
> costs, and revenue after direct costs. Let us group and filter by discharge
> date and month, fiscal period, facility, service line, product, patient type,
> and financial class. Patient counts must stay distinct for each selection;
> case mix uses only inpatient accounts with a DRG weight.
>
> Keep the view independent of any particular reporting period. The dashboard
> will compare the same measures across dates for MoM, YoY, and matched-day MTD;
> do not add window or fixed-period comparison measures to the metric view.
> Use clear names and descriptions for dashboards and Genie. The data ends
> September 20, 2026.

## Dashboard

> I'm a service line analyst preparing an AI/BI dashboard. Use `<metric_view>`
> as the source of our shared business measures. Build comparisons by querying
> those measures for each period; do not create another metric view or redefine
> the volume, case mix, revenue, or cost measures. The data covers October 2024
> through September 20, 2026.
>
> Show headline patient and encounter counts, surgical cases, inpatient case
> mix index, estimated net revenue, direct and indirect costs, and revenue
> after direct costs. Add monthly volume and financial trends. Compare results
> by service line, facility, and product with actual values, prior values, and
> absolute and percentage changes. For complete months, compare August 2026
> with July 2026 (MoM) and August 2025 (YoY). For partial September, compare
> September 1–20, 2026 with the same days in 2025 (matched-day MTD).
>
> Investigate the Emergency visit decline in April–August 2026 versus the same
> months in 2025. Show which facility and product contribute to the change,
> along with the change in estimated net revenue and revenue after direct
> costs for those visits. Organize the dashboard into clear overview, trend,
> and comparison sections with useful date, service line, facility, product,
> and patient type filters. Label every comparison period and cutoff.
