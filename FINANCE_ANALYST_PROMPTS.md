# Service line reporting: facilitator prompt examples

Use these after the finance gold result exists. Replace `<source_schema>` with
the full catalog.schema name. Start a fresh Genie Code chat for each request.
The gold object name may vary across teams, so each prompt asks Genie Code to
find it in the schema.

## Metric view

> I'm a service line analyst. Create one Unity Catalog metric view named
> <source_schema>.service_line_metrics over the gold hospital-account result
> built from the finance silver tables in <source_schema>. Find the gold
> result and inspect its columns. Each row is one hospital account, with
> patient and encounter IDs, discharge dates, facility, service line,
> product, patient type, surgical flag, MS-DRG weight, and account-level
> revenue and costs.
>
> We need reusable measures for distinct patients and encounters, surgical
> cases, inpatient case mix index, estimated net revenue, direct and indirect
> costs, and revenue after direct costs. Let analysts group and filter by
> discharge date and month, fiscal period, facility, service line, product,
> patient type, and financial class. Patient counts must stay distinct for
> each selection; case mix uses only inpatient accounts with a DRG weight.
> Use clear names and descriptions so the view supports monthly trends, MoM,
> YoY, and matched-day MTD comparisons in dashboards and Genie. The data ends
> September 20, 2026.

## Dashboard

> I'm a service line analyst preparing a financial reporting AI/BI dashboard.
> Use <source_schema>.service_line_metrics and its measures throughout. The
> data covers October 2024 through September 20, 2026.
>
> Give us headline KPIs for distinct patients, encounters, surgical cases,
> inpatient case mix index, estimated net revenue, direct and indirect costs,
> and revenue after direct costs. Show monthly trend charts for volume and the
> main financial metrics. Add comparison views by service line, facility,
> and product with actual values, prior values, and absolute and percentage
> changes. For complete months, compare August 2026 with July 2026 and August
> 2025. Compare September 1–20, 2026 with the same days in 2025 so the partial
> month is aligned.
>
> Include an Emergency focus: compare April–August 2026 with the same months
> in 2025, show which facility and product account for the observed visit
> decline, and show the change in estimated net revenue and revenue after
> direct costs for those visits. Organize the dashboard into clear overview,
> trend, and comparison sections with useful date, service line, facility,
> product, and patient type filters. Label every comparison period and cutoff,
> and create the dashboard from the metric view.
