# Clinical reporting: facilitator prompt examples

Use these after the clinical gold result exists. Replace `<source_schema>` with
the full catalog.schema name. Start a fresh Genie Code chat for each request.
The gold object name may vary across teams, so each prompt asks Genie Code to
find it in the schema.

## Metric view

> I'm a clinical reporting analyst. Create one Unity Catalog metric view
> named <source_schema>.tobacco_reporting_metrics over the gold patient-month
> result built from the clinical silver tables in <source_schema>. Find the
> gold result and inspect its columns. It has one row per patient and reporting
> month, with numerator and denominator flags for screening, cessation
> intervention, and the combined measure, plus clinic, clinical, and SDOH
> attributes.
>
> We need monthly screening, cessation intervention, and combined rates, plus
> a count of cessation care gaps: eligible current tobacco users without a
> qualifying intervention. Include the numerator and denominator counts
> behind each rate. Let analysts group and filter by reporting month, clinic,
> clinical factors, and SDOH, with rates that recalculate correctly for each
> selection. Use clear names and descriptions so the view is ready for
> dashboards and Genie, including month-to-month and patient-group
> comparisons.

## Dashboard

> I'm a clinical reporting analyst preparing a tobacco quality AI/BI
> dashboard for the research team. Use
> <source_schema>.tobacco_reporting_metrics and its measures throughout. The
> data covers October 2025 through September 2026 and combines structured
> encounters, note-derived intervention evidence, and SDOH.
>
> Give us headline KPIs for the latest month's screening, cessation
> intervention, and combined rates, plus the number of cessation care gaps.
> Show numerator and denominator counts behind each rate and each rate's
> percentage-point change from the prior month. Add monthly trend charts for
> rates and care gaps. Show where the later-month cessation decline is
> concentrated across clinics and patient groups. Compare October 2025–March
> 2026 with April–September 2026, showing rate changes in percentage points
> and denominator counts. Make transportation barriers, other SDOH needs, and
> clinical factors such as COPD or diabetes available for investigation.
>
> Organize the dashboard into clear overview, trend, and cohort comparison
> sections. Add useful month, clinic, and patient-group filters; choose charts
> that make differences easy to see. Create the dashboard from the metric view.
