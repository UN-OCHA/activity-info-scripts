# ActivityInfo Integration System (AIS) CLI

A specialized Command Line Interface (CLI) developed for the **United Nations Office for the Coordination of
Humanitarian Affairs (OCHA)** to streamline the management, configuration, and automation
of [ActivityInfo](https://www.activityinfo.org) databases.

## Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Setup & Installation](#-setup--installation)
- [Configuration](#-configuration)
- [Usage Guide](#-usage-guide)
    - [Database Management](#database-management)
    - [Translations](#translations)
    - [User Management](#user-management)
    - [Form & Field Configuration](#form--field-configuration)
- [Project Structure](#-project-structure)
- [License](#-license)

---

## Overview

The **AIS CLI** is designed to handle complex administrative tasks across multiple ActivityInfo databases. It provides
humanitarian data managers with tools to ensure consistency in reporting structures (disaggregations, metrics), automate
user access control, and synchronize multilingual content.

## Key Features

- **Translation Sync**: Migrate and synchronize translations between source and target databases.
- **Bulk User Management**: Add, update, or remove database users using CSV or Excel (XLSX) files.
- **Automated Form Deployment**: Programmatically create and rebuild data and reference forms based on predefined
  schemas.
- **Metric & Disaggregation Management**: Batch adjust complex form fields (Amount, Metric, Disaggregation) to maintain
  reporting standards.
- **Database Utilities**: Quick access to database listings and structural integrity checks.

## Setup & Installation

### Prerequisites

- **Python 3.11+**
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/UN-OCHA/ais.git
   cd ais
   ```

2. Install dependencies:
   ```bash
   # Using uv
   uv sync

   # Using pip
   pip install -e .
   ```

## Configuration

The tool uses environment variables for authentication. Create a `.env` file in the root directory:

```env
API_TOKEN=your_activityinfo_api_token
ACTIVITYINFO_BASE_URL=https://www.activityinfo.org/resources/
```

*Note: You can generate an API Token in your ActivityInfo profile settings.*

## Usage Guide

The CLI is built with `typer` and provides built-in help for all commands.

```bash
python main.py --help
```

### Database Management

List all databases accessible with your token:

```bash
python main.py db list
```

### Translations

Transfer translations for a specific language from one database to another:

```bash
python main.py translations transfer <source_db_id> <target_db_id> <language_code>
```

### User Management

Bulk add/update users from a file:

```bash
python main.py users add-bulk <target_db_id> <path_to_file.xlsx>
```

*Input file should contain `email`, `name`, and `role` columns.*

### Form & Field Configuration

Create or synchronize data forms in a target database:

```bash
python main.py forms create-data <target_db_id>
```

Adjust metric fields within data forms:

```bash
python main.py config metric <target_db_id>
```

---

## Project Structure

- `api/`: Low-level ActivityInfo API client and data models.
- `forms.py`: Logic for form creation and schema management.
- `users.py`: User management commands.
- `translations.py`: Translation migration logic.
- `config.py`: Field-level configuration (metrics/disaggregations).
- `db.py`: General database utility commands.
- `utils.py`: Shared utilities (logging, console output, client init).
- `activityinfo_openapi.json`: OpenAPI specification for the ActivityInfo REST API.

---

## License

This project is maintained by OCHA. License information can be found in the [LICENSE](LICENSE) file.
