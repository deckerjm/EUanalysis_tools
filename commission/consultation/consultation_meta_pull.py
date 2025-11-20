from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import random
import os

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument('--start-maximized')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

# Create PDF folder and configure Chrome to download there
pdf_folder = os.path.abspath('data/raw/consultation_pdfs')
os.makedirs(pdf_folder, exist_ok=True)

# Configure Chrome download preferences
prefs = {
    "download.default_directory": pdf_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "plugins.always_open_pdf_externally": True  # Don't open PDFs in browser
}
chrome_options.add_experimental_option("prefs", prefs)

# Initialize the Chrome driver
driver = webdriver.Chrome(options=chrome_options)

# Store all scraped data
all_data = []

try:
    # Navigate to the website
    base_url = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
    driver.get(base_url)
    
    # Wait for page to load
    wait = WebDriverWait(driver, 10)
    
    page_number = 1
    
    while True:
        print(f"Scraping page {page_number}...")
        
        # Wait for content to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Random delay between 2-4 seconds to mimic human behavior
        time.sleep(random.uniform(2, 4))
        
        # Find all feedback links using the class attribute
        feedback_links = driver.find_elements(
            By.XPATH, 
            "//a[@ecllink and contains(@class, 'ecl-link') and contains(@href, '/info/law/better-regulation/have-your-say/initiatives/') and contains(@href, '/F')]"
        )
        
        print(f"Found {len(feedback_links)} feedback entries on page {page_number}")
        
        # Extract data from each feedback link
        for i, link_elem in enumerate(feedback_links):
            try:
                # Get the full href
                feedback_url = link_elem.get_attribute('href')
                
                # Get the full text (name + country)
                full_text = link_elem.text.strip()
                
                # Extract name and country
                # The structure is: "Name (Country)" or just "Name"
                if '(' in full_text and ')' in full_text:
                    # Extract country from parentheses
                    country_start = full_text.rfind('(')
                    country_end = full_text.rfind(')')
                    country = full_text[country_start+1:country_end].strip()
                    name = full_text[:country_start].strip()
                else:
                    name = full_text
                    country = ""
                
                all_data.append({
                    'name': name,
                    'country': country,
                    'feedback_url': feedback_url,
                    'page': page_number
                })
                
                print(f"Scraped {i+1}/{len(feedback_links)}: {name} ({country})")
                
            except Exception as e:
                print(f"Could not extract data for entry {i+1}: {e}")
        
        print(f"Completed page {page_number} - Total entries so far: {len(all_data)}")
        
        # Try to find and click the "Next" button
        try:
            next_button = driver.find_element(By.XPATH, "//li[contains(@class, 'ecl-pagination__item--next')]//a[@aria-label='Go to next page']")
            
            # Scroll to next button
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(1)
            
            next_button.click()
            page_number += 1
            time.sleep(random.uniform(2, 4))  # Wait for next page to load
        except Exception as e:
            print(f"No 'Next' button found - reached last page: {e}")
            break
    
    # Convert to DataFrame and save
    df = pd.DataFrame(all_data)
    df.to_csv('data/clean/consultation_responses.csv', index=False, encoding='utf-8')
    print(f"\nTotal entries scraped: {len(all_data)}")
    print(f"Data saved to consultation_responses.csv")
    

except Exception as e:
    print(f"Error during initial scraping: {e}")
    df = pd.DataFrame(all_data)
    df.to_csv('consultation_responses.csv', index=False, encoding='utf-8')

# Lists to store scraped content
feedback_texts = []
feedback_references = []
user_types = []

# Loop through each URL in the dataframe
for idx, row in df.iterrows():
    feedback_url = row['feedback_url']
    name = row['name']
    
    print(f"\nProcessing {idx+1}/{len(df)}: {name}")
    
    try:
        # Navigate to the feedback page
        driver.get(feedback_url)
        
        # Random delay between 3-5 seconds to be polite to the server
        time.sleep(random.uniform(3, 5))
        
        # Wait for page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Extract feedback reference
        feedback_reference = ""
        try:
            ref_element = driver.find_element(By.XPATH, "//dt[contains(text(), 'Feedback reference')]/following-sibling::dd[1]")
            feedback_reference = ref_element.text.strip()
            print(f"Feedback reference: {feedback_reference}")
        except Exception as e:
            print(f"Could not find feedback reference: {e}")
        
        # Extract user type
        user_type = ""
        try:
            user_type_element = driver.find_element(By.XPATH, "//dt[contains(text(), 'User type')]/following-sibling::dd[1]")
            user_type = user_type_element.text.strip()
            print(f"User type: {user_type}")
        except Exception as e:
            print(f"Could not find user type: {e}")
        
        # Scrape the feedback text
        try:
            feedback_element = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[@class='ecl-blockquote__body']//p[@class='ecl-blockquote__citation']//span")
            ))
            feedback_text = feedback_element.text.strip()
            feedback_texts.append(feedback_text)
            print(f"Scraped feedback text ({len(feedback_text)} characters)")
        except Exception as e:
            print(f"Could not find feedback text: {e}")
            feedback_texts.append("")
        
        feedback_references.append(feedback_reference)
        user_types.append(user_type)
        
        # Additional delay between processing different feedback pages
        if idx < len(df) - 1:  # Don't delay after the last one
            time.sleep(random.uniform(2, 4))
        
    except Exception as e:
        print(f"Error processing {feedback_url}: {e}")
        feedback_texts.append("")
        feedback_references.append("")
        user_types.append("")

# Add new columns to dataframe
df['feedback_reference'] = feedback_references
df['user_type'] = user_types
df['feedback_text'] = feedback_texts

# Save updated dataframe
df.to_csv('data/clean/consultation_responses.csv', index=False, encoding='utf-8')
print(f"\nComplete data saved to consultation_responses.csv")

# Close the driver
driver.quit()
