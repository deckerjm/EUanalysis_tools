import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import os
import re
import unicodedata

# Create directory if it doesn't exist
os.makedirs('data/raw', exist_ok=True)

# Fetch XML data using pandas
url = 'https://www.europarl.europa.eu/meps/en/full-list/xml'
df = pd.read_xml(url, xpath='.//mep')

def format_name_for_url(full_name):
    """
    Convert MEP name to URL format
    Examples:
    - Mika AALTOLA -> MIKA_AALTOLA
    - Maravillas ABADÍA JOVER -> MARAVILLAS_ABADIA+JOVER
    - Marie-Luce BRASIER-CLAIN -> MARIE-LUCE_BRASIER-CLAIN
    """
    if not full_name or pd.isna(full_name):
        return None
    
    # Split by space to separate first name(s) and last name(s)
    parts = full_name.strip().split()
    
    if len(parts) < 2:
        return None
    
    formatted_parts = []
    for part in parts:
        # Keep hyphens in names like Marie-Luce or BRASIER-CLAIN
        if '-' in part:
            formatted_parts.append(part.upper())
        else:
            # Remove accents
            normalized = unicodedata.normalize('NFD', part)
            without_accents = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
            formatted_parts.append(without_accents.upper())
    
    # Find the last name (typically all caps in original)
    first_name_parts = []
    last_name_parts = []
    
    for i, part in enumerate(parts):
        if part.isupper():
            # This and everything after is the last name
            last_name_parts = formatted_parts[i:]
            first_name_parts = formatted_parts[:i]
            break
    
    # If we couldn't determine, assume last part is last name
    if not last_name_parts:
        last_name_parts = [formatted_parts[-1]]
        first_name_parts = formatted_parts[:-1]
    
    # Join first names with hyphens preserved
    first_name = '-'.join(first_name_parts) if first_name_parts else ''
    
    # Join last names with + for spaces (except hyphens)
    last_name = '+'.join(last_name_parts) if len(last_name_parts) > 1 else last_name_parts[0]
    
    # Combine with underscore
    url_name = f"{first_name}_{last_name}" if first_name else last_name
    
    return url_name

# Apply the formatting function to create URL names
df['urlName'] = df['fullName'].apply(format_name_for_url)

# Function to scrape email from MEP page
def get_mep_email(mep_id, url_name):
    """
    Scrape email address from MEP's page using Selenium
    """
    if not url_name or pd.isna(url_name):
        return None
    
    url = f"https://www.europarl.europa.eu/meps/en/{mep_id}/{url_name}/home"
    
    try:
        driver.get(url)
        
        # Wait for the email link to be present
        email_link = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.link_email[href^='mailto:']"))
        )
        
        # Extract email from href attribute
        href = email_link.get_attribute('href')
        email = href.replace('mailto:', '')
        
        return email
        
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find email for {url_name} (ID: {mep_id})")
        return None
    except Exception as e:
        print(f"Error scraping {url_name} (ID: {mep_id}): {str(e)}")
        return None

# Setup Selenium before scraping
driver = webdriver.Chrome()
driver.implicitly_wait(10)

# Scrape emails for all MEPs
print("Starting email scraping...")
df['email'] = df.apply(lambda row: get_mep_email(row['id'], row['urlName']), axis=1)
print(f"Scraped emails for {df['email'].notna().sum()} out of {len(df)} MEPs")

# Close the browser
driver.quit()

# Save to CSV
df.to_csv('data/raw/meps.csv', index=False)
print(f"Saved {len(df)} MEPs to CSV")

# Setup Selenium
driver = webdriver.Chrome()
driver.implicitly_wait(10)