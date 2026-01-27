import os
import time
import requests
import xml.etree.ElementTree as ET
import pandas as pd

# -------------------------------
# SOAP call to EUR-Lex webservice
# -------------------------------

USERNAME = "XXXXX"
PASSWORD = "XXXXX"

def make_api_call(page_number, page_size=100):
    url = "https://eur-lex.europa.eu/EURLexWebService"
    soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:sear="http://eur-lex.europa.eu/search">
  <soap:Header>
    <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" soap:mustUnderstand="true">
      <wsse:UsernameToken xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" wsu:Id="UsernameToken-1">
        <wsse:Username>{USERNAME}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{PASSWORD}</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soap:Header>
  <soap:Body>
    <sear:searchRequest>
     <sear:expertQuery><![CDATA[DTS_SUBDOM = LEGISLATION AND DC_CODED = 6088]]></sear:expertQuery>
      <sear:page>{page_number}</sear:page>
      <sear:pageSize>{page_size}</sear:pageSize>
      <sear:searchLanguage>en</sear:searchLanguage>
    </sear:searchRequest>
  </soap:Body>
</soap:Envelope>
"""
    headers = {
        "Content-Type": "application/soap+xml; charset=utf-8",
        "Accept": "application/soap+xml",
    }
    response = requests.post(url, data=soap_envelope, headers=headers, timeout=60)
    return response

# Namespaces for parsing SOAP
ns = {
    'soap': 'http://www.w3.org/2003/05/soap-envelope',
    'sear': 'http://eur-lex.europa.eu/search'
}

# -------------------------------
# Fetch & parse metadata
# -------------------------------

all_data = []
for page in range(1, 3):  # define the page range required (get this from the EURLEX site). Start with 1 and end with final page on EURLEX + 1.
     
    print(f"Requesting page {page}...")
    response = make_api_call(page)

    if response.status_code != 200:
        print(f"Request failed for page {page} with status {response.status_code}")
        print(response.text[:1000])
        continue

    try:
        root = ET.fromstring(response.text)
        results = root.findall('.//sear:result', ns)

        if not results:
            print(f"No results found on page {page}, stopping pagination")
            break

        page_data = []
        for result in results:
            content = result.find('.//sear:content', ns)
            if content is None:
                continue

            notice = content.find('.//sear:NOTICE', ns)
            if notice is None:
                continue

            title = notice.findtext('.//sear:EXPRESSION_TITLE/sear:VALUE', default='', namespaces=ns)
            date = notice.findtext('.//sear:WORK_DATE_DOCUMENT/sear:VALUE', default='', namespaces=ns)
            institution = notice.findtext('.//sear:WORK_CREATED_BY_AGENT/sear:PREFLABEL', default='', namespaces=ns)
            uri = notice.findtext('.//sear:WORK/sear:URI/sear:VALUE', default='', namespaces=ns)
            cellar_id = notice.findtext('.//sear:WORK/sear:URI/sear:IDENTIFIER', default='', namespaces=ns)
            celex = (notice.findtext('.//sear:ID_CELEX/sear:VALUE', default='', namespaces=ns)
                     or notice.findtext('.//sear:CELEX_NUMBER/sear:VALUE', default='', namespaces=ns)
                     or '')

            page_data.append({
                'Title': title,
                'Date': date,
                'Institution': institution,
                'Cellar_URI': uri,
                'Cellar_ID': cellar_id,
                'CELEX': celex,
            })

        print(f"Found {len(page_data)} documents on page {page}")
        all_data.extend(page_data)
        time.sleep(1)  # be nice to the API

    except Exception as e:
        print(f"Error processing page {page}: {str(e)}")

df = pd.DataFrame(all_data)
df.to_csv("data/EURLEX/eurlex_metadata.csv")
print(f"Total documents fetched: {len(df)}")
