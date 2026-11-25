# EUanalysis_tools

A collection of Python tools for collecting, processing and analysing data from EU institutions and related public-interest sources. The scripts are grouped by source or research task. Unless a script specifies otherwise, run commands from the repository root so that relative input and output paths resolve correctly.

## Repository structure

- `EUR-lex/` — EUR-Lex web-service client.
- `commission/consultation/` — European Commission Have Your Say consultation collection and PDF processing.
- `dsa-tdb/` — Digital Services Act Transparency Database data profiling and visualisation.
- `parliament/` — European Parliament debate and MEP voting analysis.
- `src/` — reusable API helper modules.

## Setup

Use Python 3.10 or later (the scripts have not been packaged into a single installable application). Create and activate a virtual environment, then install the dependencies required by the scripts you intend to run.

Common packages include:

```bash
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

python -m pip install pandas requests matplotlib selenium reportlab pdfplumber PyPDF2 duckdb
```

Some scripts also require a working Chrome installation and a compatible ChromeDriver (Selenium scripts). The EUR-Lex and HowTheyVote clients require internet access. Create the input/output directories referenced by a script before running it where they are not created automatically.

> **Paths:** Most scripts use paths relative to the current working directory. Run them from the repository root, and check each script's input paths and configuration before execution. Generated datasets are not necessarily committed to this repository.

## Tools

### European Parliament

#### MEP contact information

- **`parliament/mep_email_pull.py`** — collects publicly listed email addresses for Members of the European Parliament.
- Run from the repository root:
  ```bash
  python parliament/mep_email_pull.py
  ```
- Review the script's configured output path and the Parliament website structure if the scraper needs updating.

#### Debate interventions

- **`parliament/EPdebate_pull.py`** — parses European Parliament debate XML files, extracts speaker/intervention information, writes a combined CSV and creates a PDF for each intervention.
- Place the source XML files in `data/xml_debates/`. The folder-mode example at the bottom of the script is configured to use this directory.
- Run:
  ```bash
  python parliament/EPdebate_pull.py
  ```
- Edit the `process_folder(...)` call at the bottom of the script to set the XML directory, domain identifier, date string (YYMMDD), CSV output path and PDF output directory.
- The example writes the CSV to `output/ep_debate_data.csv` and PDFs to `data/clean/mep_debates/`.

#### MEP voting and defection analysis (HowTheyVote)

The voting-analysis scripts use data from [HowTheyVote](https://howtheyvote.eu/).

- **`src/howtheyvote_api.py`** — reusable API functions for listing votes (with date/facet filters), retrieving individual votes, and combining vote metadata with individual MEP votes and political-group and country statistics. It enforces a request interval and retries rate-limited requests. Import its functions into a separate analysis script or notebook; it does not run a data download automatically when executed.
  
  Example:
  ```python
  from src.howtheyvote_api import get_list, get_vote, get_votes_for_list

  # Votes on a particular date, or use date_gte/date_lte for a range
  votes = get_list(date_gte="2025-01-01", date_lte="2025-01-31")
  vote = get_vote(vote_id=12345)
  member_level_data = get_votes_for_list(
      date_gte="2025-01-01",
      date_lte="2025-01-31"
  )
  ```
  Replace the example vote ID and dates with the period you want to study. The returned objects are pandas DataFrames (or, for `get_list`/`get_vote`, the decoded API response).

- **`parliament/MEP_defectionrates_csvdata.py`** — reads HowTheyVote per-vote member CSV files from `data/individualvotes/` (matching `*_members.csv`), determines each political group's majority position for each vote, and calculates MEP defections and aggregate party defection rates. It excludes `DID_NOT_VOTE` records from the calculation.
  
  Run after placing the member-vote CSV files in the input directory:
  ```bash
  python parliament/MEP_defectionrates_csvdata.py
  ```
  Outputs: `output/mep_votes_with_party_position.csv` and `output/party_defection_rates.csv`. Ensure the `output/` directory exists.

- **`parliament/MEP_defection_graphics.py`** — creates horizontal bar charts and boxplots for party- and country-level defection rates. It reads `output/mep_votes_with_party_position.csv`, `output/party_defection_rates.csv` and `output/country_defection_rates.csv`, then saves PNG graphics under `output/graphics/`.
  
  Run:
  ```bash
  python parliament/MEP_defection_graphics.py
  ```
  This script expects all three input CSVs to exist and to contain the columns referenced in the script. The CSV-data analysis script above currently creates the first two files; provide the country-rate file (and the MEP country/party rate columns expected by the graphics script) from the relevant analysis workflow before running the graphics script.

### European Commission — Have Your Say consultations

These scripts form a workflow for collecting consultation feedback and processing attached PDF responses. They use Selenium and Chrome for browser automation.

1. **`commission/consultation/consultation_meta_pull.py`** — visits a configured Have Your Say initiative, collects feedback-page URLs and respondent information, and writes `data/consultation_responses.csv`. Edit the initiative URL in the script for the consultation you want to collect.
   
   ```bash
   python commission/consultation/consultation_meta_pull.py
   ```

2. **`commission/consultation/consultation_pdf_pull.py`** — reads `data/consultation_responses.csv`, visits each `feedback_url`, downloads available PDF attachments to `data/raw/consultation_pdfs/`, and adds `pdf_downloaded` and `pdf_filename` columns to the CSV.
   
   ```bash
   python commission/consultation/consultation_pdf_pull.py
   ```
   The input CSV must contain at least `feedback_url` and `name`.

3. **`commission/consultation/consultation_corpuscreation.py`** — extracts text from downloaded PDFs, trying pdfplumber first and PyPDF2 as a fallback. It adds a `pdf_text` column to `data/consultation_responses.csv`.
   
   ```bash
   python commission/consultation/consultation_corpuscreation.py
   ```
   It expects the PDF files in `data/consultation_pdfs/` and filenames in the CSV's `pdf_filename` column.

Check the configured PDF download and extraction paths before running the workflow: the download script currently uses `data/raw/consultation_pdfs/`, while the corpus-creation script looks in `data/consultation_pdfs/`. If you use the defaults unchanged, copy or move the downloaded PDFs to the extraction directory, or update one of the paths so they match.

### Digital Services Act — Transparency Database

Documentation for the DSA Transparency Database is available at the [DSA Transparency Database documentation](https://dsa.pages.code.europa.eu/transparency-database/dsa-tdb/index.html), and data can be explored at [transparency.dsa.ec.europa.eu](https://transparency.dsa.ec.europa.eu/).

- **`dsa-tdb/tdb_data-pull.py`** — profiles the aggregated Parquet dataset with DuckDB, recording column types, approximate distinct-value counts, null counts and sample values. It expects `data/tdb/aggregated-complete.parquet` and writes `dsa_column_profile.csv` in the working directory.
  
  Download the aggregated data first using the DSA TDB CLI. The script includes this example (adjust the dates to your required period):
  ```bash
  dsa-tdb-cli download-aggs -o data/tdb/ -i 2026-05-01 -f 2026-05-13
  python dsa-tdb/tdb_data-pull.py
  ```

- **`dsa-tdb/tdb_visualisations.py`** — queries the aggregated Parquet data with DuckDB, exports intermediate CSV tables and generates charts in `output/`. Check the input Parquet path and date filters in the script before running; ensure the required input data and `output/` directory are available.

### EUR-Lex

- **`EUR-lex/EURLEX-webservice.py`** — sends authenticated SOAP requests to the EUR-Lex web service, parses legislation metadata (including title, document date, institution, Cellar URI/ID and CELEX number), and exports the results to `data/EURLEX/eurlex_metadata.csv`.
- Before running, add your EUR-Lex web-service credentials in the `USERNAME` and `PASSWORD` configuration variables. Do not commit real credentials to the repository.
- Adjust the expert query, page range and page size in the script to suit the search. The current example query is `DTS_SUBDOM = LEGISLATION AND DC_CODED = 6088`, and the loop currently requests pages 1 and 2.
- Run:
  ```bash
  python EUR-lex/EURLEX-webservice.py
  ```
- Dependencies: `requests`, `pandas` and Python's built-in `xml.etree.ElementTree`. Create `data/EURLEX/` before running if it does not already exist.

## General notes

- Check each script's configuration, input schema and output paths before running it. Some scripts are tailored to a particular consultation, date range or dataset snapshot.
- Respect source websites' terms, rate limits and access policies. The HowTheyVote helper includes request pacing; the EUR-Lex client pauses between page requests.
- The tools are research utilities rather than a unified command-line application. Outputs and intermediate data are produced in the paths documented above.

**Disclaimer:** Some code in this repository was developed with assistance from GitHub Copilot.
