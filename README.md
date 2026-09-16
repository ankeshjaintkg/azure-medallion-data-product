# Data Engineer Dev Assignment — Azure Medallion Data Product

## University Chapters Data Product

A lightweight Azure Medallion data pipeline that ingests university chapter
data from a public ArcGIS REST API, stores the raw source payload in Bronze,
transforms and validates the data using PySpark in Silver, quarantines hard
data-quality failures, and publishes a consumer-facing Gold dataset.

---

# 1. Project Overview

This project implements a Bronze → Silver → Gold data product for university
chapter data.

The source is a public ArcGIS Feature Service containing university chapter
information and geographic coordinates.

The data product is restricted to:

- California (`CA`)
- Oregon (`OR`)
- Washington (`WA`)

The implementation demonstrates:

- Azure Data Factory ingestion
- Azure Data Lake Storage Gen2
- Bronze/Silver/Gold Medallion architecture
- PySpark transformations
- Data quality validation
- Hard-failure quarantine
- Warning-based data quality handling
- Automated Pytest validation
- Data Product Contract
- Source-to-consumer lineage
- Run-level ingestion metadata

---

# 2. Architecture

```text
                         +-----------------------------+
                         |       Public ArcGIS API    |
                         | UniversityChapters_Public  |
                         +-------------+---------------+
                                       |
                                       v
                         +-----------------------------+
                         |            ADF              |
                         |     Ingestion Layer         |
                         +-------------+---------------+
                                       |
                                       v
                  +------------------------------------------+
                  |                  BRONZE                  |
                  |                                          |
                  | Raw / near-as-received API JSON payload  |
                  |                                          |
                  | ADLS Gen2                                |
                  | bronze/university_chapters/<run_id>/     |
                  +--------------------+---------------------+
                                       |
                                       v
                              PySpark Processing
                                       |
                                       v
                  +------------------------------------------+
                  |                  SILVER                  |
                  |                                          |
                  | - Flatten JSON                            |
                  | - Rename columns                          |
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
                       v                               v
                +-------------+                +---------------+
                | QUARANTINE  |                |     GOLD      |
                |             |                |               |
                | Hard DQ     |                | OK            |
                | failures    |                | WARNING       |
                +-------------+                +-------+-------+
                                                       |
                                                       v
                                                Data Consumers
```

Detailed architecture documentation is available in:

```text
docs/architecture.md
```

---

# 3. Source

## Source System

Public ArcGIS Feature Service:

```text
UniversityChapters_Public
```

## Source Endpoint

```text
https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query
```

## Source Query

```text
where=State IN ('CA','OR','WA')
outFields=*
returnGeometry=true
f=json
```

## Geographic Scope

Only the following states are included:

| State Code | State |
|---|---|
| CA | California |
| OR | Oregon |
| WA | Washington |

---

# 4. Repository Structure

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

# 5. Technology Stack

| Component | Technology |
|---|---|
| Source | ArcGIS REST API |
| Ingestion | Azure Data Factory |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Transformation | Apache PySpark |
| Language | Python |
| Testing | Pytest |
| Version Control | Git / GitHub |
| Data Format | JSON / Parquet |
| Architecture | Medallion: Bronze → Silver → Gold |

---

# 6. Azure Components

The Azure implementation uses:

```text
Resource Group
    |
    +-- Azure Data Factory
    |
    +-- Azure Data Lake Storage Gen2
```

The current Azure resource group used during development is:

```text
rg-de-university-chapters
```

---

# 7. Azure Data Factory

Azure Data Factory is used to ingest the public ArcGIS API response.

The ingestion pipeline performs:

```text
ArcGIS REST API
      |
      v
ADF REST Dataset
      |
      v
ADLS Gen2 Bronze
```

The API is configured through an ADF REST linked service and REST dataset.

The API query is restricted to:

```text
CA
OR
WA
```

---

# 8. Bronze Layer

Bronze stores the API response as raw/near-as-received JSON.

Example Bronze structure:

```text
bronze/
└── university_chapters/
    └── 20260916_082022/
        └── raw.json
```

The timestamp directory acts as the ingestion run identifier.

Example:

```text
20260916_082022
```

## Bronze Responsibilities

Bronze is responsible for:

- Preserving the source payload
- Maintaining ingestion history
- Preserving source structure
- Supporting replay and investigation
- Avoiding business transformations

---

# 9. Silver Layer

Silver transforms the raw API payload into a structured dataset.

The transformation is implemented in:

```text
src/silver_transform.py
```

## Silver Processing

The transformation performs:

1. Read Bronze JSON.
2. Explode the ArcGIS `features` array.
3. Flatten `attributes` and `geometry`.
4. Rename source columns.
5. Filter to CA, OR, and WA.
6. Deduplicate using `chapter_id`.
7. Validate coordinates.
8. Quarantine hard failures.
9. Validate city values.
10. Apply warning status.
11. Add ingestion metadata.
12. Write Silver Parquet.

---

# 10. Source-to-Silver Mapping

| Source Field | Silver Field |
|---|---|
| `ChapterID` | `chapter_id` |
| `University_Chapter` | `chapter_name` |
| `City` | `city` |
| `State` | `state` |
| `geometry.x` | `longitude` |
| `geometry.y` | `latitude` |
| `OBJECTID` | `source_object_id` |

---

# 11. Silver Output

Local development output:

```text
output/silver/university_chapters
```

The Silver dataset contains:

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

# 12. Data Quality Rules

Two data quality rules are implemented.

## Q1 — Invalid Coordinates

Longitude must satisfy:

```text
-180 <= longitude <= 180
```

Latitude must satisfy:

```text
-90 <= latitude <= 90
```

The following conditions are treated as hard failures:

- Missing longitude
- Missing latitude
- NULL longitude
- NULL latitude
- Non-numeric coordinates
- Longitude outside `-180` to `180`
- Latitude outside `-90` to `90`

## Hard-Failure Action

The record is written to quarantine with:

```text
reason_code = INVALID_COORDINATES
```

The record does not enter the consumer-facing dataset.

---

# 13. Warning Rule

## W1 — Missing or Unknown City

A city generates a warning when it is:

```text
NULL
blank
UNKNOWN
```

The comparison for `UNKNOWN` is case-insensitive.

## Warning Action

The record remains publishable.

The record receives:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

The record is allowed into Gold.

---

# 14. Data Quality Status

## OK

The record passes all hard quality rules and has no warnings.

```text
dq_status = OK
dq_warnings = empty
```

## WARNING

The record passes all hard quality rules but has a warning.

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

## QUARANTINED

The record fails a hard quality rule.

```text
reason_code = INVALID_COORDINATES
```

Quarantined records are stored separately and are not published to Gold.

---

# 15. Quarantine

Quarantine output:

```text
output/quarantine/university_chapters
```

A quarantined record retains sufficient information for investigation.

Example fields:

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

# 16. Gold Layer

Gold is the consumer-facing dataset.

The implementation is in:

```text
src/gold_publish.py
```

Gold reads the Silver dataset and publishes only:

```text
OK
WARNING
```

records.

The publishing rule is:

```python
dq_status IN ('OK', 'WARNING')
```

Quarantined records are excluded.

---

# 17. Gold Output

The current local Gold output is:

```text
output/gold/university_chapters/v1
```

Gold contains:

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

# 18. Gold Publishing Behavior

The expected behavior is:

```text
                    Silver
                       |
              +--------+--------+
              |                 |
             OK              WARNING
              |                 |
              +--------+--------+
                       |
                       v
                      Gold
```

Hard failures follow:

```text
Invalid Coordinates
        |
        v
   QUARANTINE
        |
        X
      Gold
```

Therefore:

```text
OK            → Gold
WARNING       → Gold
QUARANTINED   → Not Gold
```

---

# 19. Ingestion Metadata

Each processed record receives:

```text
ingest_run_id
ingest_timestamp
```

Example:

```text
ingest_run_id = 20260916_082022
```

The timestamp-based identifier allows individual ingestion runs to be
distinguished.

---

# 20. Data Quality Metrics

Each Silver processing run reports:

```text
rows_in
rows_quarantined
rows_warned
rows_ok
```

Example successful API run:

```text
rows_in          : 3
rows_quarantined : 0
rows_warned      : 0
rows_ok          : 3
```

---

# 21. DQ Test Fixtures

Controlled test data is available in:

```text
fixtures/dq_test.json
```

The fixture contains two deliberate quality scenarios.

## Invalid Coordinate Fixture

```text
chapter_id = TEST-INVALID-001
longitude  = 250
latitude   = 35
```

Expected:

```text
reason_code = INVALID_COORDINATES
```

The record is quarantined.

## Unknown City Fixture

```text
chapter_id = TEST-WARNING-001
city       = UNKNOWN
```

Expected:

```text
dq_status   = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

The record remains publishable.

---
# 22. Automated Testing

The project includes fixture-driven automated tests for the required data-quality rules.

### DQ Fixture

The DQ fixture contains:

- One record with invalid coordinates
- One record with an unknown city

Run the DQ transformation:

```powershell
python src\silver_dq_test.py
```

Automated tests are located in:

```text
tests/test_silver_dq.py
```

The tests validate:

1. Invalid coordinates are quarantined.
2. Unknown city produces a warning.
3. Warning records remain in Silver.
4. Quarantined records do not enter Silver.

Run the tests with:

```bash
python -m pytest tests/test_silver_dq.py -v
```

Expected result:

```text
3 passed
```

---

# 23. Local Development Setup

## Prerequisites

Install:

- Python
- Java 17
- Git
- VS Code
- Azure account
- Azure Data Factory
- Azure Data Lake Storage Gen2

PySpark is executed locally for the transformation demonstration.

---

# 24. Create Python Environment

From the project directory:

```cmd
python -m venv .venv
```

Activate the environment:

```cmd
.venv\Scripts\activate
```

Install dependencies:

```cmd
python -m pip install -r requirements.txt
```

---

# 25. PySpark on Windows

The local PySpark setup requires Hadoop Windows utilities.

The environment variables used during development are:

```text
HADOOP_HOME=C:\hadoop
```

The Hadoop binaries are expected under:

```text
C:\hadoop\bin
```

including:

```text
winutils.exe
hadoop.dll
```

Verify:

```cmd
echo %HADOOP_HOME%
where winutils
```

Expected:

```text
C:\hadoop
C:\hadoop\bin\winutils.exe
```

---

# 26. Run Silver

The normal Silver transformation reads:

```text
fixtures/raw.json
```

Run:

```cmd
python src\silver_transform.py
```

Expected successful output:

```text
rows_in          : 3
rows_quarantined : 0
rows_warned      : 0
rows_ok          : 3
```

The real source fixture currently contains three CA records.

---

# 27. Run Gold

After Silver has completed successfully:

```cmd
python src\gold_publish.py
```

Expected:

```text
SILVER RECORD COUNT
3

GOLD RECORD COUNT
3
```

The normal source data should have:

```text
dq_status = OK
```

---

# 28. Run DQ Fixture Test Manually

The controlled DQ pipeline is:

```text
src/silver_dq_test.py
```

It uses:

```text
fixtures/dq_test.json
```

Run:

```cmd
python src\silver_dq_test.py
```

Expected DQ results:

```text
rows_in          : 2
rows_quarantined : 1
rows_warned      : 1
rows_ok          : 0
```

Expected behavior:

```text
TEST-INVALID-001
        |
        v
QUARANTINED
```

and:

```text
TEST-WARNING-001
        |
        v
WARNING
        |
        v
Silver / Gold
```

---

# 29. Important Test Data Cleanup

The DQ fixture intentionally writes test results into the local Silver and
Quarantine output directories.

After running DQ fixture tests, restore the normal source data by running:

```cmd
python src\silver_transform.py
```

Then rebuild Gold:

```cmd
python src\gold_publish.py
```

This leaves the local output directories containing the real source data for
the final demonstration.

---

# 30. Data Product Contract

The consumer contract is documented in:

```text
docs/data_product_contract.md
```

The contract defines:

- Data product purpose
- Owner
- Source
- Geographic scope
- Grain
- Schema
- Source-to-Gold mappings
- Data quality rules
- DQ status
- Quarantine behavior
- Gold publishing rules
- Freshness SLA
- Lineage
- Privacy
- Versioning
- Known limitations

Current contract version:

```text
v1.0
```

---

# 31. Architecture Documentation

Detailed architecture documentation is available in:

```text
docs/architecture.md
```

It covers:

- Azure components
- Medallion layers
- Data quality flow
- Quarantine
- Gold publishing
- Error handling
- Security
- Testing
- Design trade-offs
- Current vs production architecture

---

# 32. Error Handling

The ingestion design should not silently replace a valid Gold dataset with an
empty dataset because of an API failure.

Important failure conditions include:

- API request failure
- HTTP error
- Invalid API response
- Unexpected response structure
- Empty or unusable source response

These conditions should be treated separately from a valid source response.

---

# 33. Idempotency and Run Handling

Bronze uses a timestamp-based run identifier.

Example:

```text
20260916_082022
```

The run identifier is included in the Bronze directory structure:

```text
bronze/university_chapters/<ingest_run_id>/
```

This preserves ingestion history and prevents a new Bronze run from
overwriting the previous raw payload.

The transformation can be rerun from a specific fixture/source payload during
development and testing.

---

# 34. Security

The source API is public and does not require authentication credentials.

Azure storage credentials used during development must not be committed to
GitHub.

Do not commit:

```text
.env
connection strings
storage account keys
passwords
access tokens
secrets
```

For production, Azure Managed Identity and RBAC would be preferred.

---

# 35. Production Considerations

The current implementation is intentionally lightweight for the take-home
assignment.

Current architecture:

```text
ADF
 |
 v
ADLS Gen2 Bronze
 |
 v
Local PySpark
 |
 +--> Silver
 +--> Quarantine
 +--> Gold
```

A production deployment could use a managed Spark platform:

```text
ADF
 |
 v
ADLS Gen2 Bronze
 |
 v
Managed Spark
 |
 +--> Silver
 +--> Quarantine
 +--> Gold
```

Possible managed Spark platforms include:

- Azure Databricks
- Azure Synapse Spark
- Microsoft Fabric

The logical Medallion architecture would remain unchanged.

---

# 36. Design Decisions and Trade-offs

## Azure Data Factory

ADF is used for API ingestion and Azure orchestration.

This provides a clear separation between ingestion and transformation.

## ADLS Gen2

ADLS provides scalable object storage for Bronze data and supports the
Medallion architecture.

## Local PySpark

Local PySpark is used for the transformation demonstration.

This keeps the assignment implementation lightweight while still demonstrating
actual Spark-based transformation and DQ logic.

## Separate Quarantine

Hard-failed records are explicitly separated from Silver/Gold publication.

This reduces the risk of invalid records reaching consumers.

## Timestamp Run Identifier

A timestamp-based run identifier provides simple run-level traceability
without requiring a separate metadata database.

---

# 37. Data Product Contract Summary

| Item | Value |
|---|---|
| Product | University Chapters |
| Version | v1 |
| Source | Public ArcGIS REST API |
| Scope | CA, OR, WA |
| Grain | One row per `chapter_id` |
| Bronze | Raw API payload |
| Silver | Cleaned and validated data |
| Quarantine | Hard DQ failures |
| Gold | Consumer-facing data |
| Hard DQ | Invalid coordinates |
| Warning DQ | Missing/unknown city |
| Gold includes | OK, WARNING |
| Gold excludes | QUARANTINED |
| Freshness target | Within 24 hours |
| PII | Not intentionally included |

---

# 38. Validation Evidence

The implementation has been validated using both real source data and
controlled DQ fixtures.

## Real Source Validation

Successful Silver run:

```text
rows_in          : 3
rows_quarantined : 0
rows_warned      : 0
rows_ok          : 3
```

Successful Gold run:

```text
Silver records : 3
Gold records   : 3
```

## DQ Fixture Validation

Controlled test:

```text
rows_in          : 2
rows_quarantined : 1
rows_warned      : 1
```

The invalid-coordinate record was quarantined.

The unknown-city record was retained with:

```text
dq_status = WARNING
dq_warnings = MISSING_OR_UNKNOWN_CITY
```

## Automated Test Validation

Pytest:

```text
3 passed
```

---

# 39. End-to-End Execution

For a normal demonstration:

### Step 1 — Activate environment

```cmd
.venv\Scripts\activate
```

### Step 2 — Run Silver

```cmd
python src\silver_transform.py
```

### Step 3 — Run Gold

```cmd
python src\gold_publish.py
```

### Step 4 — Run automated tests

```cmd
python -m pytest tests\test_silver_dq.py -v
```

Note: running the DQ tests changes the local Silver output to the controlled
test fixture. After testing, rerun Silver and Gold to restore the normal
source output.

---

# 40. Expected Final Flow

```text
                 PUBLIC ARC GIS API
                         |
                         v
                    AZURE ADF
                         |
                         v
                    ADLS BRONZE
                         |
                         v
                    PYSPARK
                         |
                         v
                    SILVER DQ
                     /       \
                    /         \
             HARD FAILURE    VALID
                  |             |
                  v             v
             QUARANTINE     CITY CHECK
                                |
                         +------+------+
                         |             |
                      WARNING          OK
                         |             |
                         +------+------+
                                |
                                v
                              GOLD
                                |
                                v
                          DATA CONSUMERS
```

---

# 41. Assignment Deliverables

| Deliverable | Status |
|---|---|
| Azure ingestion | Complete |
| Bronze layer | Complete |
| Silver transformation | Complete |
| Gold product | Complete |
| Quarantine | Complete |
| Hard DQ rule | Complete |
| Warning DQ rule | Complete |
| DQ fixtures | Complete |
| Automated tests | Complete |
| Data Product Contract | Complete |
| Architecture documentation | Complete |
| README | Complete |

---

# 42. Final Notes

This repository demonstrates a complete thin Azure Medallion data product with
explicit data quality behavior and consumer documentation.

The key design principle is:

```text
Bad data should not silently become consumer data.
```

Hard quality failures are quarantined, warning records remain visible to
consumers through explicit DQ metadata, and the Gold layer contains only
publishable records.

**Contract Version:** v1.0  
**Data Product:** University Chapters  
**Architecture:** Bronze → Silver → Quarantine / Gold