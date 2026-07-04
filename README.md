# Energy Data Pipelines

## Overview

This project implements a data ingestion and monitoring solution for energy-related datasets using ETL pipelines developed in Python. It was designed to collect, validate, transform and store data from different external sources, with support for multiple execution environments and a simple web interface for pipeline control and monitoring.

The repository currently includes pipelines for two main data sources:

- **Fronius**, based on photovoltaic energy production and consumption files
- **Balcão Digital**, based on electricity consumption files provided through a shared Google Drive folder

The project was initially developed around the Fronius dataset, but later evolved into a broader multi-source ingestion system. For that reason, this repository now groups more than one pipeline under the same structure.

## Main Features

- Automated ingestion of Excel-based datasets
- Support for multiple data sources
- Data validation and normalization
- Duplicate detection before insertion
- Structured storage in database systems
- Automatic email notifications with execution summaries
- Profiling of execution times
- Flask web interface to run and monitor pipelines
- Support for local execution, Google Colab and Render deployment

## Data Sources

The project works with files obtained from two different origins:

- **Fronius files** were made available through the tutor’s GitHub repository: [datafiles](https://github.com/pedroccpimenta/datafiles.git)
- **Balcão Digital files** were made available through a shared Google Drive folder

These sources are accessed programmatically by the pipelines, depending on the selected dataset and execution environment.

## Technologies Used

The project combines data engineering, storage and web components. The main technologies used are:

- **Python** for the ETL logic
- **Pandas** for data parsing and transformation
- **Flask** for the web interface
- **CrateDB** for structured storage of historical data
- **InfluxDB** for time-series storage in the Fronius pipeline
- **TiDB** as an additional supported database in the project context
- **Google Drive API** for access to shared Balcão Digital files
- **GitHub API / requests** for access to Fronius files
- **Gmail / Resend** for automatic email notifications
- **Render** for deployment
- **Google Colab** and **VS Code** as development and execution environments

## Project Structure

A simplified view of the repository structure is shown below:

```text
.
├── scripts/        # Pipeline scripts and processing logic
├── services/       # Helper modules and service integrations
├── templates/      # HTML templates for the Flask interface
├── app.py          # Flask application entry point
├── Procfile        # Deployment configuration for Render
├── requirements.txt
├── .gitignore
└── README.md
```

### Main folders

- `scripts/` contains the ETL pipelines for each data source
- `services/` contains supporting logic such as utility functions, integrations or shared services
- `templates/` contains the HTML files used by the Flask interface

### Main files

- `app.py` starts the Flask web application and provides the control interface for pipeline execution
- `Procfile` defines how the application is started in deployment environments such as Render
- `requirements.txt` lists the Python dependencies required by the project
- `README.md` provides project documentation

## Pipeline Logic

Although each pipeline is adapted to its own dataset, the general workflow follows the same structure:

1. Access the external source and list available files
2. Filter the files to process
3. Download or read the selected files
4. Parse and normalize the data
5. Validate structure and detect possible inconsistencies
6. Check for duplicates before insertion
7. Insert new records into the target database
8. Record execution times and send an email summary

This logic makes the project easier to extend to additional data sources in the future.

## Supported Datasets

### Fronius

The Fronius pipeline processes Excel files containing photovoltaic production and consumption data. These files include timestamped measurements such as total consumption, directly consumed solar energy and energy imported from the grid.

This pipeline was designed to support time-series ingestion and may use both **CrateDB** and **InfluxDB**, depending on the target configuration.

### Balcão Digital

The Balcão Digital pipeline processes Excel files made available through a shared Google Drive folder. These files contain electricity consumption data and were integrated into the project after the initial Fronius implementation.

The pipeline supports file parsing, structure detection, timestamp construction, duplicate checking and insertion into **CrateDB**.

## Execution Environments

The project was prepared to run in different contexts:

- **Google Colab**, used during early development and testing
- **Local environment**, typically through VS Code
- **Render**, used for deployment and web access

The codebase was adapted to keep the execution logic portable across these environments, including changes in credential handling and file access.

## Notifications and Monitoring

Automatic email notifications are sent at the end of each pipeline execution. These notifications report execution status, processing times and pipeline results, helping monitor the system without directly inspecting logs.

A Flask-based web interface was also developed to make execution and monitoring easier. It allows the user to trigger pipelines and follow their behavior in a more practical way.

## Setup

### Requirements

- Python 3.x
- Access credentials for the required services
- Database configuration
- Email configuration
- Access to the external file sources

### Installation

Clone the repository:

```bash
git clone <your-repository-url>
cd <your-repository-folder>
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the Flask application locally:

```bash
python app.py
```

## Configuration Notes

This project depends on external credentials and service configuration files, which should not be committed to the repository. In the original development process, credentials were handled through environment-specific secrets for local execution, Google Colab and deployment environments.

Typical configuration includes:

- database credentials
- GitHub access token
- Google Drive service account credentials
- email service credentials
- environment variables for deployment

## Possible Improvements

- Add a dedicated configuration file with example variables
- Document each pipeline script separately
- Add sample screenshots of the web interface
- Include architecture and data flow diagrams
- Add automated tests

## Purpose

This repository was developed in the context of an internship project focused on data ingestion, monitoring and storage for energy-related datasets. The goal was not only to automate file processing, but also to build a reusable and extensible pipeline structure that can support multiple data sources and execution environments.
