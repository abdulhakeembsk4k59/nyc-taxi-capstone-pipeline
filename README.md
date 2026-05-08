# Capstone Data Engineering Pipeline

A production-style, end-to-end data engineering pipeline that ingests raw data, validates and transforms it through configurable processing stages, persists curated outputs to a relational store, and scales heavy workloads with Apache Spark. The project demonstrates modern data platform practices including modular pipeline design, schema-driven configuration, separation of concerns between batch and distributed compute layers, and a fully isolated, reproducible Python environment.

---

## Overview

This repository implements a configurable batch data pipeline designed to process structured datasets at varying scales. The system is built around three core principles:

- **Modularity** — each stage of the pipeline (ingestion, validation, transformation, persistence) is decoupled and independently testable.
- **Configuration as code** — runtime behavior is driven by external YAML configuration rather than hard-coded values, enabling reuse across datasets and environments.
- **Scalability** — lightweight tabular workloads are handled by pandas, while large-volume processing is delegated to PySpark, allowing the same architectural patterns to scale from local development to a distributed cluster.

The project serves as the capstone deliverable for a Data Engineering program and reflects industry-aligned engineering standards.

---

## Key Features

- **Configurable ETL pipeline** powered by YAML-driven parameters.
- **Hybrid compute model** combining pandas (in-memory) and PySpark (distributed) for workload-appropriate execution.
- **Relational persistence layer** through SQLAlchemy with a PostgreSQL backend (`psycopg2-binary`).
- **Columnar data interchange** via Apache Arrow / Parquet for efficient intermediate storage.
- **Unit and integration testing** through pytest with a dedicated test package.
- **Reproducible environment** using an isolated virtual environment and pinned dependencies.
- **Clean separation** of pipeline logic, distributed compute logic, and tests for maintainability.

---

## Architecture

The project follows a layered architecture that cleanly separates orchestration, transformation, and storage concerns.

```
            ┌──────────────────────┐
            │     Raw Sources      │
            │ (CSV / Parquet / DB) │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   Ingestion Layer    │  ← pipeline/
            │  (load + validate)   │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ Transformation Layer │  ← pipeline/ + spark/
            │  (clean, enrich,     │
            │   aggregate)         │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   Persistence Layer  │  ← SQLAlchemy / PostgreSQL
            │ (curated outputs +   │
            │  reporting tables)   │
            └──────────────────────┘
```

The `pipeline/` package coordinates the orchestration flow and lightweight transformations. The `spark/` package houses distributed transformations and is used when data volume exceeds single-node memory limits. Persistence is handled through SQLAlchemy, with PostgreSQL as the default target.

---

## Technology Stack

| Layer                  | Technology                              |
|------------------------|------------------------------------------|
| Language               | Python 3                                 |
| Tabular Processing     | pandas, NumPy                            |
| Distributed Processing | Apache Spark (PySpark)                   |
| Columnar Format        | Apache Arrow (PyArrow), Parquet          |
| Database ORM           | SQLAlchemy                               |
| Database Driver        | psycopg2-binary (PostgreSQL)             |
| Configuration          | PyYAML                                   |
| Testing                | pytest                                   |
| Environment            | Python `venv`                            |

---

## Project Structure

```
capstone_project/
├── pipeline/              # Core ETL logic and orchestration
│   └── __init__.py
├── spark/                 # PySpark-based distributed transformations
│   └── __init__.py
├── tests/                 # Unit and integration tests
│   └── __init__.py
├── .vscode/               # Editor configuration
├── requirements.txt       # Pinned Python dependencies
├── .gitignore             # Ignored artifacts (venv, cache, data outputs)
└── README.md              # Project documentation
```

---

## Getting Started

### Prerequisites

Before running the project, ensure the following are installed and available on your system:

- Python 3.10 or higher
- Java 8 or 11 (required by Apache Spark)
- PostgreSQL 13 or higher (if using the persistence layer)
- Git

### Installation

Clone the repository:

```bash
git clone <your-repository-url>
cd capstone_project
```

Create and activate a virtual environment:

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Configuration

Database credentials and pipeline parameters should be supplied via environment variables or a local `.env` file (which is excluded from version control). A typical configuration includes:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=capstone
DB_USER=postgres
DB_PASSWORD=your_password
```

Pipeline-level settings (input paths, schemas, transformation rules) are expected to be defined in a YAML configuration file and loaded at runtime.

---

## Usage

Once the environment is configured, the pipeline can be invoked from the project root. Typical execution flow:

```bash
# Run the full pipeline
python -m pipeline

# Run a Spark-based job
python -m spark
```

> Replace the entry-point modules with the specific scripts implemented in your `pipeline/` and `spark/` packages.

---

## Testing

The project uses **pytest** for automated testing. To execute the full test suite:

```bash
pytest
```

To run tests with verbose output and coverage:

```bash
pytest -v
```

Tests are organized under the `tests/` directory and follow the same modular structure as the source code.

---

## Data Management

Generated and intermediate data artifacts are intentionally excluded from version control to keep the repository lightweight. The following paths are ignored by default:

- `data/cleaned/` — curated output datasets
- `data/reports/` — generated reporting artifacts
- `*.parquet`, `*.csv` — large raw data files
- `logs/`, `*.log` — runtime log output
- `.env` — local secrets

---

## Roadmap

Planned and potential future enhancements:

- Workflow orchestration via Apache Airflow or Prefect
- Containerization with Docker and Docker Compose for one-command spin-up
- CI/CD integration with automated linting, testing, and deployment
- Data quality and observability layer (e.g., Great Expectations, OpenLineage)
- Cloud-native deployment templates (AWS / GCP / Azure)
- Incremental and change-data-capture (CDC) ingestion patterns

---

## Contributing

Contributions, issues, and feature requests are welcome. To contribute:

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes with clear, descriptive messages.
4. Push the branch and open a Pull Request.

Please ensure all new code is covered by tests and adheres to the existing project structure.

---

## Author

**AbdulHakeem**
Data Engineering Capstone — Module 08

For questions, feedback, or collaboration opportunities, please reach out via the project repository.
