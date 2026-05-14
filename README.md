# EUanalysis_tools
This is a repository for all the data scraping tools that I have created over the years. All of them engage with the EU websites or EU institutions and are designed to gather data from them. All outputs are stored in the /output subfolder.

## Tools Overview
- **Parliament Tools**:
  - `mep_email_pull.py`: Scrapes email addresses of Members of the European Parliament (MEPs) from the European Parliament website.
  - `EPdebate_pull.py`: Extracts intervention data from European Parliament XML debate files and saves them as CSV or PDFs.

- **DSA Transparency Tools**: 

    For these tools you will need to use the DSA-TSB documentation, which you can find at https://dsa.pages.code.europa.eu/transparency-database/dsa-tdb/index.html

    The full dataset and description of the dataset can be found at https://transparency.dsa.ec.europa.eu/
    
    - `tdb_data-pull.py`: Pulls the data and prepares a profile of the dataset
    - `tdb_visualisations.py` : Creates a series of visualisations and corresponding csv files with the data.


- **Commission Tools**:
  - `consultation_meta_pull.py`: Scrapes metadata and feedback data from the European Commission's "Have Your Say" website.
  - `consultation_pdf_pull.py`: Automates the download of PDFs from consultation feedback URLs.
  - `consultation_corpuscreation.py`: Extracts text from consultation response PDFs for analysis.

- **EUR-LEX Tools**
    - `EUR-Lex Web Service API Client`: This module provides functionality to query the EUR-Lex SOAP web service to retrieve metadata about EU legislation documents. It authenticates using

    The script:
    1. Makes authenticated SOAP requests to the EUR-Lex web service
    2. Parses XML responses to extract document metadata (title, date, institution, Cellar URI, Cellar ID, and CELEX number)
    3. Iterates through multiple pages of results
    4. Exports the collected metadata to a CSV file
    
    Dependencies:
    - requests: For making HTTP POST requests to the SOAP endpoint
    - xml.etree.ElementTree: For parsing XML/SOAP responses
    - pandas: For data manipulation and CSV export

    Configuration:
    - USERNAME: EUR-Lex web service username credential
    - PASSWORD: EUR-Lex web service password credential

    Output:
      CSV file containing document metadata saved to 'data/EURLEX/eurlex_metadata.csv'

    Example Usage:
      Run this script directly to fetch EU legislation documents matching
      the expert query (DTS_SUBDOM = LEGISLATION AND DC_CODED = 6088).

    Note:
      Requires valid EUR-Lex web service credentials. Rate limiting is 
      implemented with a 1-second delay between API calls.  

Each tool is tailored to interact with specific EU websites and extract structured data for further analysis.

DISCLAIMER: much of these files were created with the help of GitHub Copilot. 