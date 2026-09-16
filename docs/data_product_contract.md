# Data Product Contract — University Chapters

**Contract Version:** v1.0  
**Data Product:** University Chapters  
**Owner:** Data Engineering Team  
**Status:** Active  
**Source:** Public ArcGIS University Chapters API  
**Geographic Scope:** California (CA), Oregon (OR), Washington (WA)

---

## 1. Purpose

The University Chapters data product provides a clean, consumer-ready dataset
of university chapters within California, Oregon, and Washington.

### Consumer Use Cases

- Business Intelligence and reporting
- Geographic analysis
- University/chapter lookup
- Data analytics
- Downstream data products

---

## 2. Source

### Source System

Public ArcGIS Feature Service:

`UniversityChapters_Public`

### Source Endpoint

```text
https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query
```

### Source Query

```text
where=State IN ('CA','OR','WA')
outFields=*
returnGeometry=true
f=json
```

### Source Characteristics

- Publicly accessible source
- No authentication secrets are required
- Data is retrieved through the ArcGIS REST API
- Only CA, OR, and WA records are included in this data product

---

## 3. Geographic Scope

The data product is restricted to the following states:

| State Code | State |
|---|---|
| CA | California |
| OR | Oregon |
| WA | Washington |

Records outside these states are not included in the consumer-facing product.

---

## 4. Data Product Grain

**One row represents one university chapter.**

### Business Key

```text
chapter_id
```

### Uniqueness Rule

`chapter_id` must be unique within the published dataset.

Duplicate chapter IDs are removed during Silver processing.

---

## 5. Medallion Architecture

The data product follows a Bronze → Silver → Gold architecture.

```text
                    Public ArcGIS API
                           |
                           v
                     +-----------+
                     |  BRONZE   |
                     | Raw JSON  |
                     +-----------+
                           |
                           v
                     +-----------+
                     |  SILVER   |
                     | Cleaned   |
                     | Typed     |
                     | Deduped   |
                     | DQ checks |
                     +-----------+
                       /       \
                      /         \
                     v           v
              +-----------+   +-------------+
              | QUARANTINE|   |    GOLD     |
              | Hard Fail |   | OK/WARNING  |
              +-----------+   +-------------+
                                  |
                                  v
                           Consumer Product
```

---

## 6. Bronze Layer

Bronze contains the raw or near-as-received API payload.

### Responsibilities

- Preserve the source API payload
- Preserve ingestion history by run
- Maintain source lineage
- Avoid business transformations
- Provide enough information for downstream processing and investigation

### Bronze Path

```text
bronze/university_chapters/<ingest_run_id>/
```

### Bronze Payload

The API response is stored as JSON.

The raw feature structure is preserved so that downstream processing can
trace transformed records back to the source payload.

---

## 7. Silver Layer

Silver contains cleaned and structured university chapter records.

### Silver Processing

The Silver layer performs:

- JSON feature flattening
- Source-to-target column mapping
- Data type conversion
- State filtering
- Duplicate removal using `chapter_id`
- Coordinate data quality validation
- City warning validation
- Ingestion metadata assignment
- Data quality status assignment

### Silver Output

```text
output/silver/university_chapters
```

---

## 8. Quarantine Layer

Records that fail a hard data quality rule are written to a separate
quarantine dataset.

### Quarantine Path

```text
output/quarantine/university_chapters
```

### Quarantine Requirements

A quarantined record contains sufficient information for investigation,
including:

- Chapter information where available
- Source object ID
- Ingestion run ID
- Ingestion timestamp
- Original raw feature
- Failure reason code

Quarantined records must not enter the Silver or Gold consumer datasets.

---

# 9. Gold Layer

Gold is the consumer-facing data product.

### Gold Path

```text
output/gold/university_chapters/v1
```

### Publishing Rule

Records with the following statuses are published:

```text
OK
WARNING
```

Records with hard quality failures are excluded from Gold.

```text
QUARANTINED
```

must never be published to Gold.

---

## 10. Gold Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chapter_id` | string | No | Unique university chapter identifier |
| `chapter_name` | string | Yes | University chapter name |
| `city` | string | Yes | City associated with the chapter |
| `state` | string | No | Two-letter state code: CA, OR, or WA |
| `longitude` | double | No | Geographic longitude |
| `latitude` | double | No | Geographic latitude |
| `source_object_id` | long | Yes | Source ArcGIS OBJECTID |
| `ingest_run_id` | string | No | Identifier for the ingestion run |
| `ingest_timestamp` | string/timestamp | No | UTC ingestion timestamp |
| `dq_status` | string | No | Data quality status |
| `dq_warnings` | string | Yes | Data quality warning code(s) |

---

## 11. Source-to-Gold Mapping

| Source Field | Gold Field |
|---|---|
| `ChapterID` | `chapter_id` |
| `University_Chapter` | `chapter_name` |
| `City` | `city` |
| `State` | `state` |
| `geometry.x` | `longitude` |
| `geometry.y` | `latitude` |
| `OBJECTID` | `source_object_id` |

---

## 12. Data Quality Rules

### Q1 — Invalid Coordinates

Longitude and latitude are mandatory for publication.

#### Longitude

Valid range:

```text
-180 <= longitude <= 180
```

#### Latitude

Valid range:

```text
-90 <= latitude <= 90
```

A record fails Q1 when longitude or latitude is:

- Missing
- NULL
- Non-numeric
- Outside the valid geographic range

### Failure Action

The record is quarantined with:

```text
reason_code = INVALID_COORDINATES
```

The record must not enter Silver or Gold.

---

## 13. Data Quality Warning Rules

### W1 — Missing or Unknown City

A city generates a warning when the value is:

- NULL
- Blank
- `UNKNOWN`, case-insensitive

### Warning Action

The record remains publishable.

The following values are assigned:

```text
dq_status   = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

The record is allowed into Gold.

---

## 14. Data Quality Status

### OK

The record passes all mandatory quality checks and has no warnings.

```text
dq_status = OK
dq_warnings = empty
```

### WARNING

The record passes all hard quality checks but has one or more warning
conditions.

```text
dq_status = WARNING
dq_warnings = <warning code>
```

### QUARANTINED

The record fails a hard quality rule.

Quarantined records are retained separately for investigation and are not
published to the consumer-facing Gold dataset.

---

## 15. Data Quality Metrics

Each processing run should report the following metrics:

```text
rows_in
rows_quarantined
rows_warned
rows_ok
```

### Metric Definitions

| Metric | Definition |
|---|---|
| `rows_in` | Number of source features received for processing |
| `rows_quarantined` | Number of records failing hard quality rules |
| `rows_warned` | Number of publishable records with warnings |
| `rows_ok` | Number of publishable records with no warnings |

These metrics provide visibility into ingestion and data quality outcomes.

---

## 16. Consumer Contract

Consumers can expect:

1. One row per `chapter_id`.
2. Only CA, OR, and WA records.
3. Valid longitude and latitude values.
4. Hard-failed records are not published.
5. Warning records remain available with `dq_status = WARNING`.
6. Data quality status and warning information are exposed.
7. Ingestion metadata is available for lineage and troubleshooting.
8. The Gold dataset represents the consumer-facing published data product.

---

## 17. Freshness SLA

The data product is batch-oriented.

The target freshness SLA is:

```text
Within 24 hours of the previous successful ingestion.
```

Freshness depends on successful execution of the upstream ArcGIS API ingestion.

If the source API fails or returns an unusable or empty response, the pipeline
must not silently replace a valid Gold dataset with an empty dataset.

---

## 18. Error Handling

The pipeline should treat the following conditions as ingestion failures:

- API request failure
- HTTP error
- Invalid API response
- Unexpected response structure
- Unusable or empty source response

A source failure must not silently produce an empty Gold product.

The previous valid consumer dataset should not be replaced solely because the
source ingestion failed.

---

## 19. Lineage

The logical lineage is:

```text
ArcGIS Public API
       |
       v
     Bronze
       |
       v
     Silver
       |
       +------> Quarantine
       |
       v
      Gold
```

### Record Lineage

Gold records contain:

```text
source_object_id
ingest_run_id
ingest_timestamp
```

These fields provide source and ingestion traceability.

---

## 20. Privacy

The source data is publicly accessible.

This data product is not intended to contain personally identifiable
information (PII).

The pipeline does not intentionally introduce PII into the Gold product.

---

## 21. Versioning

The current consumer contract version is:

```text
v1
```

### Breaking Changes

A new contract version should be created for breaking changes such as:

- Removing a Gold column
- Changing the meaning of an existing column
- Changing the data product grain
- Changing a field type in a consumer-breaking manner
- Changing the meaning of a data quality status

### Non-Breaking Changes

Non-breaking additions may be introduced with appropriate documentation and
consumer impact assessment.

---

## 22. Known Limitations

- The product depends on availability and correctness of the public ArcGIS
  source.
- Source data quality may change between ingestion runs.
- City warnings do not prevent publication.
- Hard quality failures are quarantined and excluded from the consumer product.
- The current implementation is designed as a take-home/demo data product
  rather than a fully managed production platform.
- Spark transformations are currently demonstrated using local PySpark.
- A production implementation could run the Spark transformations on a managed
  Spark platform.

---

## 23. Testing

The project includes controlled data quality fixtures to validate the required
quality rules.

### Invalid Coordinates Test

An invalid longitude is introduced.

Expected result:

```text
reason_code = INVALID_COORDINATES
```

The record is quarantined and excluded from Silver/Gold publication.

### Unknown City Test

A record with:

```text
city = UNKNOWN
```

is introduced.

Expected result:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

The record remains publishable.

### Automated Tests

The project includes automated PySpark/Pytest tests covering:

- Invalid coordinate quarantine
- Unknown city warning
- Exclusion of quarantined records from Silver

---

## 24. Ownership

**Owner:** Data Engineering Team

The owner is responsible for:

- Data product schema
- Data quality rules
- Contract versioning
- Consumer-impact assessment
- Source and lineage documentation
- Documentation updates

---

## 25. Contract Summary

| Item | Contract |
|---|---|
| Data Product | University Chapters |
| Version | v1 |
| Source | Public ArcGIS Feature Service |
| Geographic Scope | CA, OR, WA |
| Grain | One row per `chapter_id` |
| Consumer Layer | Gold |
| Hard DQ Failure | Invalid coordinates |
| Hard Failure Action | Quarantine |
| Warning DQ | Missing/unknown city |
| Warning Action | Publish with `WARNING` |
| Gold Exclusion | Quarantined records |
| Freshness Target | Within 24 hours |
| PII | Not intentionally included |
| Owner | Data Engineering Team |

---

**End of Data Product Contract**