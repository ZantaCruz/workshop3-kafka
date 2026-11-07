# Technical Report — Workshop 3: Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) · Data Engineering and Artificial Intelligence · Universidad Autónoma de Occidente
**Dataset:** World Happiness Report 2015–2019 · **Stack:** Python 3.12, Apache Kafka 4.4 (Docker), scikit-learn, SQLite, Streamlit

---

## Table of contents
1. Executive summary
2. Architecture
3. Part A — Integration, cleaning and model
4. Part B — Kafka pipeline and event contract
5. Part C — Database and dashboard
6. Step-by-step reproduction
7. Verification, tests and evidence
8. Limitations and future work
9. Repository structure

---

## 1. Executive summary
An end-to-end pipeline was built that (a) integrates five CSV files with different schemas into a single dataset of **782 country-year observations (164 countries)**, (b) trains and serializes a regression pipeline (`model.pkl`), (c) publishes each observation as an event to Kafka, (d) consumes the events by validating a contract, generating a real-time prediction and storing both the raw event and the result in a database, and (e) shows 4 KPIs in a dashboard that queries the database (not the CSV files).

Model result (test set, unseen countries): **MAE 0.432 · RMSE 0.562 · R² 0.784** with linear regression. As the assignment requires, the focus is on architecture, reliability and reproducibility, not on maximizing accuracy.

## 2. Architecture
```
OFFLINE
 CSV 2015–2019 ─▶ EDA + cleaning + harmonization ─▶ features ─▶ regression ─▶ models/model.pkl
                    (notebooks/eda.ipynb)          (notebooks/model_training.ipynb)

STREAMING
 data/streaming/happiness_stream.csv
        │ row by row, configurable delay
        ▼
 Producer (key = country) ─▶ Topic happiness-events (3 partitions, RF=1) ─▶ Consumer (group happiness-ml-team)
                                                                                │
   1. INSERT into raw_happiness_events (original message, status RECEIVED)      │
   2. Validate contract       ─ fails ─▶ INVALID_SCHEMA / INVALID_VALUES (stored, skipped, no crash)
   3. Features in training order (feature_names_in_ of the Pipeline)
   4. model.predict           ─ fails ─▶ PREDICTION_ERROR
   5. dim_country + dim_date + fact_predictions, status VALID (atomic transaction)
   6. manual offset commit (after persisting)
                                                                                ▼
                                                  SQLite (storage/happiness.db) ─▶ Streamlit dashboard
```
**Design decisions**
| Decision | Reason |
|---|---|
| Business logic in `kafka/processing.py`, separate from Kafka | It can be tested without a broker (`tests/test_pipeline.py`) and the consumer stays thin. |
| Manual offset commit after writing to the DB | *At-least-once* semantics: if the process dies before saving, the message is reprocessed instead of lost. |
| `UNIQUE(topic, partition, offset)` in the raw table | A replay does not duplicate rows (message-level idempotence). |
| The raw event is inserted **before** validation | Traceability/audit: everything that arrives is recorded, even broken JSON. |
| A defective event never raises an exception | The assignment requires invalid records to be stored, skipped from prediction and not to crash the pipeline. |
| SQLite | Allowed by the assignment, no server needed; the schema is portable to PostgreSQL. |

---

## 3. Part A — Integration, cleaning and model

### 3.1 Profiling (EDA, `notebooks/eda.ipynb`)
| Year | Rows | Nulls | Duplicate rows | Duplicate countries |
|---|---|---|---|---|
| 2015 | 158 | 0 | 0 | 0 |
| 2016 | 157 | 0 | 0 | 0 |
| 2017 | 155 | 0 | 0 | 0 |
| 2018 | 156 | 1 (`Perceptions of corruption`) | 0 | 0 |
| 2019 | 156 | 0 | 0 | 0 |

**Schema inconsistencies found**
- *Country column:* `Country` (2015–2017) vs `Country or region` (2018–2019).
- *Variable names:* e.g. `Economy (GDP per Capita)` / `Economy..GDP.per.Capita.` / `GDP per capita`; `Family` / `Social support`; `Health (Life Expectancy)` / `Healthy life expectancy`; `Freedom` / `Freedom to make life choices`; `Trust (Government Corruption)` / `Perceptions of corruption`.
- *Columns present only in some years:* `Region` (2015–16), `Standard Error` (2015), confidence intervals (2016), `Whisker.*` (2017), `Dystopia Residual` (2015–17); the rank column has three different names.
- *Types / format:* the 2017 file was exported from R (quoted headers with dots, ~15 decimals).
- *Different country names for the same entity:* Macedonia / North Macedonia, North Cyprus / Northern Cyprus, Somaliland region / Region, Hong Kong S.A.R., China / Hong Kong, Taiwan Province of China / Taiwan, Trinidad and Tobago / Trinidad & Tobago.

**Outliers (IQR 1.5×):** `family` 10, `generosity` 17, `corruption` 68; none in `gdp`, `health`, `freedom` or the score. There are 5 zeros in gdp/family/health/freedom/generosity and 6 in corruption.

### 3.2 Cleaning decisions and justification
| Problem | Decision | Justification |
|---|---|---|
| Different column names | Explicit mapping to `country, year, gdp, family, health, freedom, generosity, corruption, happiness_score` | A single analytical schema, the same one used by the event contract. |
| Inconsistent types | Numerics to `float64`, `year` to `int64` (with `errors="raise"` to catch hidden text) | Guarantees training and streaming see the same types. |
| Different country names | Normalization dictionary (6 cases) | Otherwise one country would count as two, breaking grouping and the split. |
| 1 null in `corruption` (United Arab Emirates, 2018) | Impute with the **same country's mean** from other years (fallback: global median) | Corruption perception is country-specific and stable over time; dropping the row would lose a whole observation for one missing field. |
| Countries with <5 years of data | Kept | They are real entries/exits of the survey (Angola, Belize, Sudan, Puerto Rico…), not naming errors. |
| Outliers and zeros | **Kept** | They are real source values (countries with very low indicators); removing them would bias the model and the stream must be able to see them. They are controlled with ranges in the contract. |
| `Region`, SE, CI, Whisker, Dystopia, Rank | Dropped from the unified schema | They do not exist in every year and several are derived from the target (see 3.3). |
| Duplicates | None found; country-year uniqueness is verified | Merge quality control. |

Final unified schema (`data/processed/happiness_unified.csv`, 782 rows): `country` (string), `year` (int), `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption` (float), `happiness_score` (float, target).

### 3.3 Feature engineering
- **Features (6):** `gdp, family, health, freedom, generosity, corruption`. Correlation with the score: gdp 0.79, health 0.74, family 0.65, freedom 0.55, corruption 0.40, generosity 0.14.
- **Excluded and why**
  - `happiness_score`: it is the target (it arrives in the event as `actual_happiness_score`, for evaluation only).
  - `Dystopia Residual`, `Rank`, `Standard Error`, CI, `Whisker`: derived from the score → **target leakage**; they also do not exist in every year or in the event.
  - `region`: only exists in 2015–2016.
  - `country`: high cardinality; used as the Kafka *key*, not as a feature.
  - `year`: identifies the observation, it does not explain happiness.
- **Categorical data:** the features contain no categorical variables, so no encoding is required.
- **Scaling:** `StandardScaler` **inside** the `Pipeline`, so the consumer applies exactly the preprocessing learned during training.
- **70/30 split grouped by country (`GroupShuffleSplit`, seed 42):** 542 training rows and 240 test rows, 0 countries in common. A country appears up to 5 times with nearly identical scores; a row-level split would leak information from train to test and inflate the metrics.

### 3.4 Model and evaluation
| Model | Test MAE | Test RMSE | Test R² | Train R² |
|---|---|---|---|---|
| **Linear Regression (chosen)** | **0.432** | **0.562** | **0.784** | 0.752 |
| Decision Tree (depth 5) | 0.543 | 0.683 | 0.681 | 0.846 |
| Random Forest (200) | 0.480 | 0.612 | 0.743 | 0.974 |

Linear regression is the simplest **and** the best on test; Random Forest overfits (R² 0.974 on train vs 0.743 on test). Standardized coefficients: gdp 0.453, freedom 0.241, family 0.239, health 0.205, corruption 0.074, generosity 0.063 (intercept 5.386). Artifacts: `models/model.pkl` (full Pipeline) and `models/metrics.json`. The notebook reloads the pickle and checks that it reproduces the predictions.

---

## 4. Part B — Kafka pipeline and event contract

### 4.1 Producer (`kafka/producer.py`)
- Reads `data/streaming/happiness_stream.csv` (prepared dataset, shuffled order) **row by row**.
- Serializes JSON in the exact format required by the assignment, `key = country`, topic `happiness-events`.
- Creates the topic (3 partitions, replication factor 1) if it does not exist, using `AdminClient`.
- `event_time` = the instant the producer emits the event; `event_id` = `E0001`, `E0002`… (4 digits because there are 782 events).
- Parameters: `--delay` (seconds between events), `--subset all|test|train`, `--limit N`, `--bootstrap`, `--inject-invalid` (interleaves 6 defective demo events), idempotent producer (`enable.idempotence`).

### 4.2 Event contract
```json
{"event_id":"E0001","country":"Colombia","year":2019,"gdp":1.2,"family":0.8,"health":0.9,
 "freedom":0.6,"generosity":0.3,"corruption":0.1,"actual_happiness_score":6.2,"event_time":"2026-10-30T15:30:00"}
```
| Field | Rule | Status on failure |
|---|---|---|
| `event_id` | non-empty string, unique among VALID events | empty/type → `INVALID_SCHEMA`; repeated → `INVALID_VALUES` |
| `country` | non-empty string (also the Kafka key) | `INVALID_SCHEMA` |
| `year` | integer (not bool/float), 2000–2100 | type → `INVALID_SCHEMA`; range → `INVALID_VALUES` |
| `gdp, family, health, freedom, generosity, corruption` | numeric (not bool/str), finite, within a plausible range | type/None/missing → `INVALID_SCHEMA`; NaN or out of range → `INVALID_VALUES` |
| `actual_happiness_score` | numeric 0–10; **evaluation only, never a feature** | same as above |
| `event_time` | valid ISO-8601 | empty string → `INVALID_SCHEMA`; not parseable → `INVALID_VALUES` |
| Model features | all present (read from `model.feature_names_in_`) | `INVALID_SCHEMA` |
| Prediction | finite and without exception | `PREDICTION_ERROR` |

Ranges: gdp [0, 3], family [0, 2.5], health [0, 1.5], freedom/generosity/corruption [0, 1], score [0, 10] (generous relative to what was observed in 2015–2019). Extra fields are ignored; broken JSON, payloads that are not objects and empty messages are stored in raw and marked without breaking the consumer.

### 4.3 Consumer (`kafka/consumer.py` + `kafka/processing.py`)
Group `happiness-ml-team`, `auto.offset.reset=earliest`, `enable.auto.commit=false`. For each message: store raw → validate → build the DataFrame with columns in training order → `model.predict` → atomic load into dimensions and fact → mark `VALID` → commit offset. The `--idle-exit N` option stops the consumer after N seconds without messages (useful for demos).

---

## 5. Part C — Database and dashboard

### 5.1 Data model (`sql/create_tables.sql`)
```
raw_happiness_events ◀── fact_predictions ──▶ dim_country
 (staging/traceability)            │
                                   └────────▶ dim_date
```
- **`raw_happiness_events`** (not a dimension): `raw_event_id` PK, `kafka_topic/partition/offset/key`, `raw_payload` (original, unmodified), `event_id`, `received_at`, `processing_status` ∈ {RECEIVED, VALID, INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR}, `status_detail` (reason). `UNIQUE(topic, partition, offset)`.
- **`dim_country`**: `country_id`, `country_name` (unique).
- **`dim_date`**: `date_id` (YYYY0101), `full_date`, `year`, `decade`. Represents the **observation year**; the streaming time lives in the fact's `event_time` / `prediction_timestamp`.
- **`fact_predictions`**: `prediction_id`, `raw_event_id` (unique FK → traceability), `country_id`, `date_id`, `actual_score`, `predicted_score`, `prediction_error` (= predicted − actual, signed), `abs_prediction_error`, `event_time`, `prediction_timestamp`, `model_name`.

Traceability: `fact_predictions → raw_happiness_events` returns the original message (including partition and offset) that produced each prediction (query included in `sql/kpis.sql`).

### 5.2 Dashboard (`dashboards/app.py`, Streamlit)
Queries the database only (`HAPPINESS_DB` lets you point to another one). Filter by observation year.
| KPI | What it shows | How to read it |
|---|---|---|
| 1. Average prediction error | MAE, RMSE and bias (pred − actual) | MAE = typical error in score points; a bias near 0 means the model does not over/underestimate on average. |
| 2. Predictions by country | Bar chart (top 20) | Streaming coverage; most countries have 5 (one per year). |
| 3. Predicted vs actual | Scatter with diagonal and color by year | Points on the diagonal = exact prediction. |
| 4. Trend over time | MAE per emission minute + actual vs predicted score by year | Model stability during the stream and across report editions. |

Also: a pipeline quality table (events by status) and the rejection reasons. Real screenshots are in `docs/evidence/` (see §7).

---

## 6. Step-by-step reproduction
Requirements: Python 3.12, Docker Desktop (port 9092 free). PowerShell commands from the project root.

```powershell
# 0. Environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# 1. (Optional: already included) Part A: run in order
#    notebooks/eda.ipynb  →  notebooks/model_training.ipynb
#    They produce: data/processed/happiness_unified.csv, models/model.pkl, models/metrics.json, data/streaming/happiness_stream.csv

# 2. Kafka
docker compose up -d
docker compose ps

# 3. Terminal 1 — consumer (creates storage/happiness.db by itself)
python kafka/consumer.py

# 4. Terminal 2 — producer (creates the topic if it does not exist)
python kafka/producer.py --delay 0.2
#   demo variant with rejections:  python kafka/producer.py --subset test --inject-invalid --delay 0.1

# 5. Terminal 3 — dashboard
streamlit run dashboards/app.py
```
**Inspection:**
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic happiness-events --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group happiness-ml-team
```
SQL: open `storage/happiness.db` with a VS Code SQLite extension and run `sql/kpis.sql`.

**Restart test (stop–produce–restart):** stop the consumer with Ctrl+C, run the producer, check the LAG with the command above, then start the consumer again: it processes the backlog and, thanks to the per-offset `UNIQUE`, does not duplicate rows.

**Notes from the real run**
- `model.pkl` was trained with scikit-learn 1.8.0; `requirements.txt` pins that version to avoid `InconsistentVersionWarning` (if another version is used, retrain with `notebooks/model_training.ipynb`).
- `event_id` is generated with a counter (`E0001`…) **per producer run**. Running the producer again on the same topic resends the same `event_id` values; the consumer rejects them as `INVALID_VALUES` (`duplicate event_id`) and does not duplicate rows in `fact_predictions`. This is the contract's uniqueness rule working; for a clean run the environment must be reset.
- An initial `UNKNOWN_TOPIC_OR_PART` warning from the consumer is normal if it starts before the producer creates the topic; it recovers on its own.
- `python tests/check_db.py` prints statuses and rejection reasons and verifies there are no duplicates in the fact table.

**Clean reset:** `docker compose down` (the topic is lost: no volume) and delete `storage/happiness.db`.

**Without Docker:** `python tests/test_pipeline.py --keep-db storage/happiness_demo.db` runs the full consumer processing with simulated messages; then `HAPPINESS_DB=storage/happiness_demo.db streamlit run dashboards/app.py`.

---

## 7. Verification, tests and evidence

### 7.1 Real run with Kafka (Docker, `apache/kafka:4.4.0`)
The complete flow was executed: consumer + producer (`--inject-invalid`) + dashboard. Result verified with `tests/check_db.py` (`docs/evidence/check_db_output.txt`):

| Status | Events | Origin |
|---|---|---|
| `VALID` | **782** | The whole dataset |
| `INVALID_SCHEMA` | 4 | Missing field, wrong type, empty country, broken JSON (demo defects) |
| `INVALID_VALUES` | 2 (+20 after the restart test) | Out-of-range value and invalid date (demo) |

`fact_predictions` has **782 rows with 782 distinct `event_id` values**; MAE 0.429, RMSE 0.549, bias −0.003. Messages were spread across the 3 partitions (p0 326, p1 234, p2 248 counting all statuses). Every rejection was stored in the raw table with its reason and the consumer never stopped.

**Dashboard (queries the DB)**

![KPIs 1–3](docs/evidence/dashboard_kpis_1_3.png)
*KPI 1: MAE 0.429 / RMSE 0.549 / bias −0.003 (the model does not over- or underestimate on average). KPI 2: every country has 5 predictions (one per year). KPI 3: points follow the diagonal; the error is larger at the extremes of the score.*

![KPI 4](docs/evidence/dashboard_kpi4.png)
*KPI 4 (right): the predicted score falls below the actual one in 2016 (≈5.12 vs 5.38) and above it from 2017 (≈5.49 vs 5.35–5.41). This is the scale change in `family` and `health` between report editions (§8). Left: MAE per event-emission minute.*

![Quality](docs/evidence/dashboard_quality.png)
*Events by status in the raw table: 782 VALID, 4 INVALID_SCHEMA and 22 INVALID_VALUES (2 from the demo + 20 duplicates from the restart test).*

### 7.2 Stop–produce–restart test (`docs/evidence/consumer_group_lag.txt`)
1. Consumer stopped with Ctrl+C; group up to date: **LAG 0** on the 3 partitions (offsets 451 / 341 / 356).
2. With the consumer off, the producer sent 20 events. Kafka retained them: LOG-END-OFFSET moved to 463 / 344 / 361 and the **total LAG was 12 + 3 + 5 = 20**.
3. When the consumer restarted (same group) it resumed from its last committed offset, processed the backlog and the **LAG returned to 0**; `CONSUMER-ID` and `HOST` now appear as active.
4. The 20 events were `E0001`–`E0020`, already loaded, so they ended as `INVALID_VALUES: duplicate event_id` (20). `fact_predictions` stayed at 782 rows: **a replay does not corrupt the fact table**.

*Note:* the offsets add up to more than 788 because the topic also retains the 360 messages from earlier test runs (already consumed before the DB was reset).

### 7.3 Automated tests without a broker (`tests/test_pipeline.py`)
Reproduces the consumer with simulated messages (partition/offset) and checks with *asserts*: 783 VALID (782 + one event with an extra field that is ignored) with the same number of rows in the fact table; 7 `INVALID_SCHEMA`; 4 `INVALID_VALUES` (range, date, duplicate `event_id`, NaN); 1 `PREDICTION_ERROR` (a model that raises an exception); a replay of the same (partition, offset) is skipped; every message lands in raw with its status. The dashboard was tested with `streamlit.testing` (no exceptions).

---

## 8. Limitations and future work
1. **Test environment:** validated on a single local broker (RF=1). It does not cover broker failures or replicated partitions.
2. **Scale change between report editions:** the mean of `family` goes from ≈0.99 (2015) and 0.79 (2016) to ≈1.19–1.21 (2017–2019), and `health` also varies. Hence the signed error by year is −0.26 in 2016 and +0.09 to +0.14 from 2017: a single model mixes slightly different scales. Possible improvement: normalize by year or include `year` as a feature.
3. **Data window:** the stream reuses the same observations the model was trained on (70% of the countries); `--subset test` emits only unseen countries for an unbiased evaluation.
4. **Kafka persistence:** the course compose file defines no volume; on `down` the topic is lost.
5. **Dashboard:** Streamlit instead of Power BI/Looker/Tableau (the assignment only suggests them); the schema is portable to PostgreSQL if one of those tools is required.
6. **Improvements:** DLQ as a separate topic, model drift metrics, PostgreSQL, time windows.

## 9. Repository structure
```
├── data/{raw,processed,streaming}/      Original CSVs, unified dataset and streaming dataset
├── notebooks/{eda,model_training}.ipynb
├── kafka/{producer,consumer,processing}.py
├── models/{model.pkl,metrics.json}
├── sql/{create_tables.sql,kpis.sql}
├── dashboards/app.py
├── docs/evidence/                       Screenshots and outputs from the real run
├── storage/                             SQLite DB (generated at runtime)
├── tests/{test_pipeline.py,check_db.py}
├── docker-compose.yml · requirements.txt · README.md
```
