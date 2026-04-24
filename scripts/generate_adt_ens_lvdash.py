#!/usr/bin/env python3
"""Build dashboards/adt_ens_bronze.lvdash.json and resources/lakeview/adt_events_dashboard.json."""
import json
from copy import deepcopy

OUT = "dashboards/adt_ens_bronze.lvdash.json"
OUT2 = "resources/lakeview/adt_events_dashboard.json"
V2 = 2
V3 = 3
DS = "ds_adt_all"

DS_ADT_ALL = (
    "SELECT event_time_stamp, "
    "CAST(event_time_stamp AS DATE) AS event_date, "
    "date_format(date_trunc('day', event_time_stamp), 'yyyy-MM-dd') AS event_day, "
    "HOUR(event_time_stamp) AS hour_of_day, "
    "CASE WHEN dayofweek(event_time_stamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_kind, "
    "coalesce(nullif(trim(message_event_type), ''), '(null)') AS message_event_type, "
    "coalesce(nullif(trim(patient_class), ''), '(null)') AS patient_class, "
    "coalesce(nullif(trim(department), ''), '(empty)') AS department, "
    "coalesce(nullif(trim(facility), ''), '(empty)') AS facility, "
    "coalesce(nullif(trim(sending_facility), ''), '(empty)') AS sending_facility, "
    "coalesce(nullif(trim(admission_type), ''), '(null)') AS admission_type, "
    "coalesce(nullif(trim(financial_class), ''), '(null)') AS financial_class, "
    "coalesce(nullif(trim(CAST(discharge_diposition AS STRING)), ''), '(null)') AS discharge_disposition, "
    "patient_mrn, CAST(1 AS INT) AS row_n, "
    "CASE WHEN coalesce(nullif(trim(message_event_type), ''), '') = 'A01' THEN 1 ELSE 0 END AS a01_f "
    "FROM bronze.ensemble.ens_adt "
    "WHERE message_type = 'ADT' AND event_time_stamp IS NOT NULL "
    "AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)"
)

DATASETS = [
    {"name": "ds_adt_all", "displayName": "ADT message rows (365d, filterable)", "queryLines": [DS_ADT_ALL]},
    {
        "name": "ds_total_events",
        "displayName": "Total ADT rows (365d, all) — reference",
        "queryLines": [
            "SELECT COUNT(*) AS total_events FROM bronze.ensemble.ens_adt WHERE message_type = 'ADT' AND event_time_stamp IS NOT NULL AND event_time_stamp >= date_sub(CURRENT_DATE(), 365)"
        ],
    },
    {
        "name": "ds_admissions_today",
        "displayName": "A01 today (all) — reference",
        "queryLines": [
            "SELECT COUNT(*) AS admissions_today FROM bronze.ensemble.ens_adt WHERE message_type = 'ADT' AND message_event_type = 'A01' AND event_time_stamp IS NOT NULL AND to_date(event_time_stamp) = CURRENT_DATE()"
        ],
    },
]

SEL = {"version": 2, "defaultSelection": {"operator": {"operator": "AND", "args": []}}}


def fq(n, disagg, fields):
    return {"name": n, "query": {"datasetName": DS, "fields": fields, "disaggregated": disagg}}


def filter_date_picker(wname, qn):
    return {
        "widget": {
            "name": wname,
            "queries": [
                fq(
                    qn,
                    False,
                    [
                        {"name": "daily(event_date)", "expression": 'DATE_TRUNC("DAY", `event_date`)'},
                        {
                            "name": "daily(event_date)_associativity",
                            "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                        },
                    ],
                )
            ],
            "spec": {
                "version": V2,
                "widgetType": "filter-date-picker",
                "encodings": {
                    "fields": [
                        {
                            "fieldName": "daily(event_date)",
                            "displayName": "Calendar date",
                            "queryName": qn,
                        }
                    ]
                },
                "frame": {"showTitle": True, "title": "Calendar date"},
                "selection": deepcopy(SEL),
            },
        },
        "position": {"x": 0, "y": 0, "width": 2, "height": 1},
    }


def filter_select(wname, qn, col, display, ftitle, x, y, w=2, h=1):
    return {
        "widget": {
            "name": wname,
            "queries": [
                fq(
                    qn,
                    False,
                    [
                        {"name": col, "expression": f"`{col}`"},
                        {
                            "name": f"{col}_associativity",
                            "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                        },
                    ],
                )
            ],
            "spec": {
                "version": V2,
                "widgetType": "filter-single-select",
                "encodings": {
                    "fields": [
                        {
                            "fieldName": col,
                            "displayName": display,
                            "queryName": qn,
                        }
                    ]
                },
                "frame": {"showTitle": True, "title": ftitle},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def W(widget_name, y, h, wpos, wwid, **kw):
    base = {
        "widget": {"name": widget_name, **kw},
        "position": {"x": wpos, "y": y, "width": wwid, "height": h},
    }
    return base


def mq(cols, disagg=True, dataset=DS):
    flds = [{"name": c, "expression": f"`{c}`"} for c in cols]
    return {
        "name": "main_query",
        "query": {"datasetName": dataset, "fields": flds, "disaggregated": disagg},
    }


def mqa(expr_name, expression, disagg=False, dataset=DS):
    return {
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [{"name": expr_name, "expression": expression}],
            "disaggregated": disagg,
        },
    }


vfmt = {
    "style": {"bold": True},
    "format": {
        "type": "number-plain",
        "abbreviation": "compact",
        "decimalPlaces": {"type": "max", "places": 0},
    },
}

counter_spec_2 = lambda title, field, disp="": {  # noqa: E731
    "version": V2,
    "widgetType": "counter",
    "encodings": {
        "value": {
            "fieldName": field,
            "displayName": disp,
            **vfmt,
        }
    },
    "frame": {"title": title, "showTitle": True},
}

# bar horizontal: quantitative x, categorical y
hbar = lambda title, ycat, ymes: {  # noqa: E731
    "version": V3,
    "widgetType": "bar",
    "encodings": {
        "x": {
            "fieldName": ymes,
            "displayName": "Count",
            "scale": {"type": "quantitative"},
        },
        "y": {
            "fieldName": ycat,
            "displayName": ycat,
            "scale": {"type": "categorical"},
        },
        "label": {"show": True},
    },
    "frame": {"title": title, "showTitle": True},
}


def filter_strip(pfx: str, day_kind_on_row1: bool = True) -> list:
    """6 filters in two rows. Row0: date + event + class. Row1: dep + fac + (sending|daykind)."""
    a = pfx
    w = [
        filter_date_picker(f"{a}_f_date", f"q_{a}_date"),
        filter_select(
            f"{a}_f_evt", f"q_{a}_evt", "message_event_type", "Trigger", "message_event_type", 2, 0
        ),
        filter_select(
            f"{a}_f_pcl", f"q_{a}_pcl", "patient_class", "class", "patient_class", 4, 0
        ),
        filter_select(f"{a}_f_dep", f"q_{a}_dep", "department", "Dept", "department", 0, 1),
        filter_select(f"{a}_f_fac", f"q_{a}_fac", "facility", "Site", "facility", 2, 1),
    ]
    if day_kind_on_row1:
        w.append(
            filter_select(
                f"{a}_f_dk", f"q_{a}_dk", "day_kind", "Wknd?", "day kind", 4, 1
            )
        )
    else:
        w.append(
            filter_select(
                f"{a}_f_snd", f"q_{a}_snd", "sending_facility", "MSH-4", "sending", 4, 1
            )
        )
    return w


def run():
    p1 = "p1"
    p2 = "p2"
    p3 = "p3"
    l1 = []
    l1.extend(filter_strip(p1, day_kind_on_row1=True))
    l1.append(
        W(
            f"{p1}_hint",
            2,
            1,
            0,
            6,
            multilineTextboxSpec={
                "lines": [
                    "**Slicing:** all pages share **`ds_adt_all`**; pick dates and dimensions, then read **selection** counters vs the **ref** (unfiltered) row.\n"
                ]
            },
        )
    )
    l1.append(
        W(
            f"{p1}_title",
            3,
            2,
            0,
            6,
            multilineTextboxSpec={
                "lines": [
                    "# ADT — `bronze.ensemble.ens_adt` (multi-page)\n",
                    "**1 Overview** — KPIs, daily, trigger mix, class pie, message sample. **2 Time** — hour, weekend, daily trend, A01 by day. **3 Location** — depts, facilities, sending, admission/fin/dispo.\n"
                ]
            },
        )
    )
    # y5 selection KPIs
    for i, (part, ex, tit) in enumerate(
        [
            ("rows", "COUNT(`row_n`)", "ADT rows (selection)"),
            ("dmr", "COUNT(DISTINCT `patient_mrn`)", "Distinct patient_mrn (selection)"),
            ("a01s", "SUM(`a01_f`)", "A01 count (selection)"),
        ]
    ):
        l1.append(
            {
                "widget": {
                    "name": f"{p1}_kpi_{part}",
                    "queries": [mqa("m", ex)],
                    "spec": counter_spec_2(tit, "m", ""),
                },
                "position": {"x": i * 2, "y": 5, "width": 2, "height": 2},
            }
        )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_k_ref_tot",
                "queries": [mq(["total_events"], disagg=True, dataset="ds_total_events")],
                "spec": counter_spec_2("Ref: 365d row count (unfiltered)", "total_events", "all"),
            },
            "position": {"x": 0, "y": 7, "width": 1, "height": 1},
        }
    )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_k_ref_a01",
                "queries": [mq(["admissions_today"], disagg=True, dataset="ds_admissions_today")],
                "spec": counter_spec_2("Ref: A01 today (unfiltered)", "admissions_today", "A01"),
            },
            "position": {"x": 1, "y": 7, "width": 1, "height": 1},
        }
    )
    l1.append(
        W(
            f"{p1}_ref_n",
            7,
            1,
            2,
            4,
            multilineTextboxSpec={
                "lines": [
                    "**Ref** = fixed SQL, no slicers. Compare to **selection** above.\n"
                ]
            },
        )
    )
    l1.append(
        W(
            f"{p1}_h1",
            8,
            1,
            0,
            6,
            multilineTextboxSpec={"lines": ["## Volume & class (selection)\n"]},
        )
    )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_b_day",
                "queries": [mq(["event_day", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "event_day",
                            "displayName": "day",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "msgs",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "Messages / day", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 9, "width": 6, "height": 3},
        }
    )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_b_evt",
                "queries": [mq(["message_event_type", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "message_event_type",
                            "displayName": "trigger",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "By message_event_type", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 12, "width": 3, "height": 3},
        }
    )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_pie_c",
                "queries": [mq(["patient_class", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "pie",
                    "encodings": {
                        "angle": {"fieldName": "row_n", "displayName": "n"},
                        "color": {
                            "fieldName": "patient_class",
                            "displayName": "class",
                            "scale": {"type": "categorical"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "patient_class", "showTitle": True},
                },
            },
            "position": {"x": 3, "y": 12, "width": 3, "height": 3},
        }
    )
    l1.append(
        {
            "widget": {
                "name": f"{p1}_t1",
                "queries": [mq(["event_time_stamp", "message_event_type", "patient_class", "department", "facility"])],
                "spec": {
                    "version": 1,
                    "frame": {"title": "Message sample (selection, scroll)", "showTitle": True},
                    "widgetType": "table",
                    "encodings": {
                        "columns": [
                            {
                                "fieldName": c,
                                "displayAs": "string",
                                "visible": True,
                                "order": i,
                                "title": t,
                            }
                            for i, (c, t) in enumerate(
                                [
                                    ("event_time_stamp", "time"),
                                    ("message_event_type", "t"),
                                    ("patient_class", "cl"),
                                    ("department", "dept"),
                                    ("facility", "fac"),
                                ]
                            )
                        ]
                    },
                },
            },
            "position": {"x": 0, "y": 15, "width": 6, "height": 3},
        }
    )

    l2 = []
    l2.extend(filter_strip(p2, day_kind_on_row1=False))  # sending on row1 right
    l2.append(
        W(
            f"{p2}_h",
            2,
            1,
            0,
            6,
            multilineTextboxSpec={
                "lines": ["# Time: hour, calendar, A01 (selection, **use calendar filter** above)\n"]
            },
        )
    )
    l2.append(
        {
            "widget": {
                "name": f"{p2}_b_hr",
                "queries": [mq(["hour_of_day", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "hour_of_day",
                            "displayName": "h",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "Hour of day (0-23)", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 3, "width": 3, "height": 3},
        }
    )
    l2.append(
        {
            "widget": {
                "name": f"{p2}_b_wd",
                "queries": [mq(["day_kind", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "day_kind",
                            "displayName": "kind",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {
                        "title": "Weekend (Sun/Sat) vs Weekday (Spark dayofweek)",
                        "showTitle": True,
                    },
                },
            },
            "position": {"x": 3, "y": 3, "width": 3, "height": 3},
        }
    )
    l2.append(
        {
            "widget": {
                "name": f"{p2}_b_day2",
                "queries": [mq(["event_day", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "line",
                    "encodings": {
                        "x": {
                            "fieldName": "event_day",
                            "displayName": "day",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                    },
                    "frame": {"title": "Daily (line, categorical x)", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 6, "width": 6, "height": 3},
        }
    )
    l2.append(
        {
            "widget": {
                "name": f"{p2}_b_a01d",
                "queries": [mq(["event_day", "a01_f"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "event_day",
                            "displayName": "day",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "a01_f",
                            "displayName": "A01",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "Admits (A01) / day in selection", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 9, "width": 6, "height": 3},
        }
    )
    l2.append(
        {
            "widget": {
                "name": f"{p2}_t2",
                "queries": [mq(["event_day", "hour_of_day", "message_event_type", "a01_f"])],
                "spec": {
                    "version": 1,
                    "frame": {"title": "Time fields (export)", "showTitle": True},
                    "widgetType": "table",
                    "encodings": {
                        "columns": [
                            {
                                "fieldName": c,
                                "displayAs": "string",
                                "visible": True,
                                "order": i,
                                "title": t,
                            }
                            for i, (c, t) in enumerate(
                                [
                                    ("event_day", "day"),
                                    ("hour_of_day", "hr"),
                                    ("message_event_type", "t"),
                                ]
                            )
                        ]
                        + [
                            {
                                "fieldName": "a01_f",
                                "displayAs": "number",
                                "visible": True,
                                "order": 3,
                                "title": "a1",
                            }
                        ]
                    },
                },
            },
            "position": {"x": 0, "y": 12, "width": 6, "height": 3},
        }
    )

    l3 = []
    l3.extend(filter_strip(p3, day_kind_on_row1=True))
    l3.append(
        W(
            f"{p3}_h",
            2,
            1,
            0,
            6,
            multilineTextboxSpec={
                "lines": [
                    "# Location, routing, PV1 billing (selection) — `discharge_diposition` in bronze\n"
                ]
            },
        )
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_hb_d",
                "queries": [mq(["department", "row_n"])],
                "spec": hbar("Department (all values in selection)", "department", "row_n"),
            },
            "position": {"x": 0, "y": 3, "width": 3, "height": 4},
        }
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_b_fac",
                "queries": [mq(["facility", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "facility",
                            "displayName": "f",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "Site `facility`", "showTitle": True},
                },
            },
            "position": {"x": 3, "y": 3, "width": 3, "height": 4},
        }
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_hb_s",
                "queries": [mq(["sending_facility", "row_n"])],
                "spec": hbar("sending_facility (MSH-4)", "sending_facility", "row_n"),
            },
            "position": {"x": 0, "y": 7, "width": 3, "height": 4},
        }
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_b_at",
                "queries": [mq(["admission_type", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "admission_type",
                            "displayName": "a",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "admission_type (PV1-14 area)", "showTitle": True},
                },
            },
            "position": {"x": 3, "y": 7, "width": 3, "height": 2},
        }
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_b_fn",
                "queries": [mq(["financial_class", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "financial_class",
                            "displayName": "fc",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "financial_class", "showTitle": True},
                },
            },
            "position": {"x": 3, "y": 9, "width": 3, "height": 2},
        }
    )
    l3.append(
        {
            "widget": {
                "name": f"{p3}_b_dch",
                "queries": [mq(["discharge_disposition", "row_n"])],
                "spec": {
                    "version": V3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "discharge_disposition",
                            "displayName": "d",
                            "scale": {"type": "categorical"},
                        },
                        "y": {
                            "fieldName": "row_n",
                            "displayName": "n",
                            "scale": {"type": "quantitative"},
                        },
                        "label": {"show": True},
                    },
                    "frame": {"title": "discharge disposition (typos per bronze column name)", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 11, "width": 6, "height": 3},
        }
    )

    doc = {
        "datasets": DATASETS,
        "pages": [
            {
                "name": "pg1_overview",
                "displayName": "1. Overview (filtered)",
                "layout": l1,
                "pageType": "PAGE_TYPE_CANVAS",
            },
            {
                "name": "pg2_time",
                "displayName": "2. Time & patterns",
                "layout": l2,
                "pageType": "PAGE_TYPE_CANVAS",
            },
            {
                "name": "pg3_loc",
                "displayName": "3. Location & billing",
                "layout": l3,
                "pageType": "PAGE_TYPE_CANVAS",
            },
        ],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
            "genieSpace": {"isEnabled": True, "enablementMode": "ENABLED"},
            "applyModeEnabled": False,
        },
    }
    for path in (OUT, OUT2):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)
    print("Wrote", OUT, "and", OUT2)


if __name__ == "__main__":
    run()
