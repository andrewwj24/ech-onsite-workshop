# Clinical reporting: facilitator prompt examples

Use these after the clinical gold table exists. Before pasting a prompt,
replace `<gold_table>` and `<metric_view>` with fully qualified Unity Catalog
names (`catalog.schema.object`). The gold table and metric view may be in
different schemas. Start a fresh Genie Code chat for each asset.

## Metric view

> I'm a clinical reporting analyst. Create one Unity Catalog metric view at
> `<metric_view>` from `<gold_table>`. Inspect the gold table first. It has one row
> per patient and reporting month, with numerator and denominator flags for
> screening, cessation intervention, and the combined measure, plus clinic,
> clinical, and SDOH attributes. The gold result already incorporates
> note-derived intervention evidence.
>
> Define reusable numerator and denominator counts and rates for all three
> measures, plus a count of cessation care gaps: eligible current tobacco
> users without a qualifying intervention. Let us group and filter by
> reporting month, clinic, clinical factors, and SDOH. Rates should use the
> summed numerator and denominator so they recalculate for each selection.
>
> Keep the view independent of any particular month or patient-group
> comparison. The dashboard will compare the same measures across those
> selections; do not add window or fixed-period comparison measures to the
> metric view. Use clear names and descriptions for dashboards and Genie.

## Dashboard

> I'm a clinical reporting analyst preparing a tobacco quality AI/BI
> dashboard for the research team. Use `<metric_view>` as the source of our
> shared business measures. Build comparisons by querying those measures for
> each period and patient group; do not create another metric view or redefine
> the rates or care-gap measure. The data covers October 2025 through
> September 2026 and includes structured encounters, note-derived
> intervention evidence, and SDOH.
>
> Show the latest month's screening, cessation intervention, and combined
> rates and cessation care gaps, with the numerator and denominator counts
> behind each rate and percentage-point changes from the prior month. Add
> monthly rate and care-gap trends. Compare October 2025–March 2026 with
> April–September 2026, showing rate changes in percentage points and the
> denominator counts behind them.
>
> Explore where the later-month cessation decline is concentrated across
> clinics and patient groups, including transportation barriers, other SDOH
> needs, and clinical factors such as COPD or diabetes. Use clear overview,
> trend, and cohort comparison sections with useful month, clinic, and
> patient-group filters. Label the comparison periods and show a finding
> supported by rates and counts.
