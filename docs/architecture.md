# University Chapters — Data Product Architecture

## 1. Overview

This project implements a thin Azure Medallion data pipeline for the
University Chapters data product.

The pipeline ingests university chapter data from a public ArcGIS REST API,
stores the source payload in Bronze, applies data cleansing and data quality
rules in Silver, separates hard-failed records into Quarantine, and publishes
consumer-ready records through Gold.

The geographic scope is limited to:

- California (CA)
- Oregon (OR)
- Washington (WA)

---

## 2. Architecture Diagram

```text
                         +-----------------------------+
                         |       Public ArcGIS API    |
                         | UniversityChapters_Public  |
                         +-------------+---------------+
                                       |
                                       |
                              API Ingestion
                                       |
                                       v
                         +-----------------------------+
                         |            ADF              |
                         |     Orchestration Layer     |
                         +-------------+---------------+
                                       |
                                       |
                                       v
                  +------------------------------------------+
                  |                  BRONZE                  |
                  |                                          |
                  | Raw / near-as-received API JSON payload  |
                  | Ingestion history by run                 |
                  |                                          |
                  | ADLS Gen2                                |
                  | bronze/university_chapters/<run_id>/     |
                  +--------------------+---------------------+
                                       |
                                       |
                              PySpark Processing
                                       |
                                       v
                  +------------------------------------------+
                  |                  SILVER                  |
                  |                                          |
                  | - Flatten JSON features                  |
                  | - Rename source columns                  |
                  | - Type conversion                        |
                  | - State filtering                        |
                  | - Deduplication                          |
                  | - Coordinate validation                  |
                  | - City warning validation                |
                  | - DQ status                              |
                  | - Ingestion metadata                     |
                  +--------------------+---------------------+
                                       |
                       +---------------+---------------+
                       |                               |
                 Hard DQ failure                 Publishable
                       |                               |
                       v                               v
          +---------------------------+     +-------------------------+
          |        QUARANTINE         |     |          GOLD           |
          |                           |     |                         |
          | Invalid coordinates       |     | OK records              |
          | reason_code:              |     | WARNING records         |
          | INVALID_COORDINATES       |     |                         |
          +---------------------------+     | Consumer-facing data   |
                                            | product                 |
                                            +------------+------------+
                                                         |
                                                         v
                                                  Data Consumers
```

---

## 3. Components

### 3.1 Public ArcGIS API

The source system is the public ArcGIS Feature Service:

```text
UniversityChapters_Public
```

The API query is restricted to CA, OR, and WA.

```text
where=State IN ('CA','OR','WA')
outFields=*
returnGeometry=true
f=json
```

The source provides university chapter attributes and geographic coordinates.

---

### 3.2 Azure Data Factory

Azure Data Factory is used as the ingestion/orchestration layer.

ADF is responsible for:

- Calling the public ArcGIS REST API
- Copying the API response into Bronze
- Organizing Bronze data by ingestion run
- Providing a repeatable ingestion process

The current implementation uses an ADF REST linked service and REST dataset.

---

### 3.3 Azure Data Lake Storage Gen2

ADLS Gen2 provides the data storage layer.

The logical Medallion structure is:

```text
bronze/
silver/
quarantine/
gold/
```

The current Bronze structure is:

```text
bronze/university_chapters/<ingest_run_id>/raw.json
```

Silver, Quarantine, and Gold are demonstrated through the local PySpark
processing output directories in this take-home implementation.

---

## 4. Bronze Layer

Bronze stores the source API payload with minimal transformation.

### Responsibilities

- Preserve source data
- Maintain ingestion history
- Preserve source structure
- Support replay and investigation
- Avoid business transformations

Example:

```text
bronze/
└── university_chapters/
    └── 20260916_082022/
        └── raw.json
```

The timestamp-based run identifier provides a simple mechanism for separating
individual ingestion runs.

---

## 5. Silver Layer

Silver transforms the raw API response into a structured dataset.

### Processing Steps

```text
Raw JSON
   |
   v
Explode features
   |
   v
Flatten attributes + geometry
   |
   v
Rename columns
   |
   v
Filter CA / OR / WA
   |
   v
Deduplicate by chapter_id
   |
   v
Coordinate DQ
   |
   +---- Invalid ---> Quarantine
   |
   v
City DQ warning
   |
   v
Silver
```

### Source-to-Silver Mapping

| Source | Silver |
|---|---|
| `ChapterID` | `chapter_id` |
| `University_Chapter` | `chapter_name` |
| `City` | `city` |
| `State` | `state` |
| `geometry.x` | `longitude` |
| `geometry.y` | `latitude` |
| `OBJECTID` | `source_object_id` |

---

## 6. Data Quality Processing

The Silver layer implements two required data quality rules.

### Hard Failure — Invalid Coordinates

Longitude must be between:

```text
-180 and 180
```

Latitude must be between:

```text
-90 and 90
```

Missing, null, non-numeric, or out-of-range coordinates result in:

```text
reason_code = INVALID_COORDINATES
```

The record is quarantined.

It does not enter the consumer-facing dataset.

---

### Warning — Missing or Unknown City

The following city values produce a warning:

```text
NULL
blank
UNKNOWN
```

The record remains publishable with:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

---

## 7. Quarantine

Quarantine provides a separate destination for hard-failed records.

Example:

```text
Silver Input
     |
     v
Invalid coordinates
     |
     v
Quarantine
```

Quarantined records retain sufficient source and ingestion information for
investigation.

The quarantine dataset contains fields such as:

```text
chapter_id
chapter_name
city
state
longitude
latitude
source_object_id
ingest_run_id
ingest_timestamp
raw_feature
reason_code
```

---

## 8. Gold Layer

Gold is the consumer-facing data product.

The Gold layer publishes:

```text
OK
WARNING
```

records.

It excludes hard-failed/quarantined records.

The publishing rule is:

```text
dq_status IN ('OK', 'WARNING')
```

### Gold Schema

```text
chapter_id
chapter_name
city
state
longitude
latitude
source_object_id
ingest_run_id
ingest_timestamp
dq_status
dq_warnings
```

---

## 9. Data Quality Flow

```text
                         Silver
                           |
                           v
                 +-------------------+
                 | Coordinate Check  |
                 +---------+---------+
                           |
                 +---------+---------+
                 |                   |
              Invalid              Valid
                 |                   |
                 v                   v
            Quarantine        +-------------+
                               | City Check |
                               +------+------+
                                      |
                            +---------+---------+
                            |                   |
                         Missing/Unknown       Valid
                            |                   |
                            v                   v
                         WARNING                OK
                            |                   |
                            +---------+---------+
                                      |
                                      v
                                    Gold
```

---

## 10. Data Quality Metrics

Each processing run reports:

```text
rows_in
rows_quarantined
rows_warned
rows_ok
```

Example successful source run:

```text
rows_in          : 3
rows_quarantined : 0
rows_warned      : 0
rows_ok          : 3
```

The project also contains controlled DQ fixtures demonstrating:

```text
1 quarantined record
1 warning record
```

---

## 11. Idempotency and Run Handling

The Bronze layer uses a timestamp-based ingestion run identifier.

Example:

```text
20260916_082022
```

Each ingestion run is stored under a separate Bronze directory.

This preserves historical source payloads and prevents a new ingestion from
overwriting previous raw payloads.

Silver and Gold processing can be rerun from a specific Bronze payload during
development and testing.

---

## 12. Error Handling

The ingestion process should not silently publish an empty Gold dataset when
the source API fails.

Important failure conditions include:

- API request failure
- HTTP error
- Invalid response
- Unexpected response structure
- Empty or unusable source response

A source ingestion failure should be treated separately from a valid ingestion
that happens to contain zero publishable records.

---

## 13. Security

The source API used by this assignment is public and does not require
credentials.

Azure storage credentials used during development are kept outside source
code.

No secrets should be committed to GitHub.

For a production implementation, managed identity and role-based access would
be preferred over embedded storage account credentials.

---

## 14. Current Implementation vs Production

This take-home implementation deliberately uses a lightweight architecture.

### Current Implementation

```text
ADF
 |
 v
ADLS Bronze
 |
 v
Local PySpark
 |
 +--> Silver
 +--> Quarantine
 +--> Gold
```

The local PySpark execution provides a reproducible demonstration of the Spark
transformation logic and data quality rules.

### Production Evolution

A production implementation could execute the Spark transformations on a
managed Spark platform such as Azure Databricks, Azure Synapse Spark, or
Microsoft Fabric.

The logical Medallion architecture would remain the same:

```text
ADF
 |
 v
ADLS Bronze
 |
 v
Managed Spark
 |
 +--> Silver
 +--> Quarantine
 +--> Gold
```

---

## 15. Testing Architecture

The project includes controlled DQ fixtures and automated Pytest tests.

The tests validate:

1. Invalid coordinates are quarantined.
2. Unknown city generates a warning.
3. Warning records remain in Silver.
4. Quarantined records do not enter Silver.

Testing flow:

```text
DQ Fixture
    |
    v
Silver DQ Transformation
    |
    +------> Quarantine assertion
    |
    +------> Warning assertion
    |
    +------> Silver exclusion assertion
```

---

## 16. Repository Structure

```text
data-engineer-azure-medallion/
│
├── docs/
│   ├── architecture.md
│   └── data_product_contract.md
│
├── fixtures/
│   ├── raw.json
│   └── dq_test.json
│
├── notebooks/
│
├── src/
│   ├── silver_transform.py
│   ├── silver_dq_test.py
│   └── gold_publish.py
│
├── tests/
│   └── test_silver_dq.py
│
├── output/
│   ├── silver/
│   ├── quarantine/
│   └── gold/
│
├── requirements.txt
└── README.md
```

---

## 17. Design Trade-offs

### Local PySpark

Local PySpark was selected for the take-home transformation demonstration
because managed Spark capacity was not required for validating the core
transformation and DQ logic.

This keeps the implementation lightweight and reproducible.

### Azure Data Factory

ADF is used for source ingestion and orchestration because the assignment
requires an Azure-based ingestion flow.

### Timestamp Run Identifier

A timestamp-based run identifier provides simple run-level separation without
introducing a separate metadata database.

For a production platform, a centrally managed pipeline run ID and metadata
store could provide stronger operational observability.

### Separate Quarantine

Quarantine is intentionally separated from Silver and Gold so that hard-failed
records cannot accidentally become consumer-facing data.

---

## 18. Architecture Summary

The final logical architecture is:

```text
Public ArcGIS API
       |
       v
Azure Data Factory
       |
       v
ADLS Gen2 Bronze
       |
       v
PySpark Silver Transformation
       |
       +--------------------+
       |                    |
       v                    v
  Quarantine             Silver
                             |
                             v
                          Gold
                             |
                             v
                       Data Consumers
```

The architecture provides:

- Source preservation
- Medallion separation
- Data quality enforcement
- Explicit quarantine
- Consumer-facing Gold publishing
- Run-level lineage
- Automated DQ validation
- A documented consumer contract