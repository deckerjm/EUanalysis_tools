import os
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

"""
Script for downloading PDFs from consultation feedback URLs.
This script automates the process of downloading PDFs from a list of feedback URLs 
provided in a CSV file. This file can be generated using the consultation_meta_pull.py 
It uses Selenium to interact with the web pages and locate 
the download buttons for the PDFs. The downloaded PDFs are saved in a specified 
directory, and the script updates the CSV file with information about the download 
status and filenames.

How to Use:
1. Ensure you have the required dependencies installed:
    - Python packages: selenium, pandas
    - Chrome browser and the appropriate ChromeDriver for your browser version.
2. Prepare the input CSV file:
    - The CSV file should be located at `data/consultation_responses.csv`.
    - It must contain at least two columns:
      - `feedback_url`: The URL of the feedback page.
      - `name`: A name or identifier for the feedback entry.
3. Run the script:
    - The script will navigate to each URL, attempt to download the PDF, and save it 
      in the `data/consultation_pdfs` directory.
    - The script will update the input CSV file with two new columns:
      - `pdf_downloaded`: A boolean indicating whether the PDF was successfully downloaded.
      - `pdf_filename`: The name of the downloaded PDF file (if applicable).
4. Output:
    - The updated CSV file will be saved at `data/consultation_responses.csv`.
    - The downloaded PDFs will be saved in the `data/consultation_pdfs` directory.
Inputs:
- `data/consultation_responses.csv`: The input CSV file containing feedback URLs and names.
Outputs:
- Updated CSV file with download status and filenames.
- Downloaded PDFs saved in the specified directory.
Notes:
- Ensure the ChromeDriver executable is in your system's PATH or specify its location 
  when initializing the WebDriver.
- The script includes random delays to mimic human behavior and avoid detection as a bot.
"""

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
    "plugins.always_open_pdf_externally": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Initialize the Chrome driver
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 10)

# Read existing CSV with feedback URLs
df = pd.read_csv('data/consultation_responses.csv')

# Lists to store PDF info
pdf_downloaded = []
pdf_filenames = []

print(f"Starting PDF downloads for {len(df)} entries...")
print(f"Download directory: {pdf_folder}\n")

# Loop through each URL in the dataframe
for idx, row in df.iterrows():
    feedback_url = row['feedback_url']
    name = row['name']
    
    print(f"\nProcessing {idx+1}/{len(df)}: {name}")
    
    pdf_found = False
    pdf_filename = None
    
    try:
        # Navigate to the feedback page
        driver.get(feedback_url)
        time.sleep(random.uniform(3, 5))
        
        # Wait for page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Try to download PDF
        try:
            # Find the download button
            download_button = driver.find_element(
                By.XPATH, "//a[@eclfiledownload and @download and contains(@class, 'ecl-file__download')]"
            )
            
            # Small delay before clicking
            time.sleep(random.uniform(1, 2))
            
            # Scroll to download button
            driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
            time.sleep(0.5)
            
            # JavaScript click
            print(f"Download button found, attempting click...")
            driver.execute_script("arguments[0].click();", download_button)
            
            # Get initial list of files before download
            files_before = set(os.listdir(pdf_folder))
            
            # Wait for download to complete
            download_wait_time = 20
            start_time = time.time()
            download_complete = False
            
            print(f"Waiting for download to complete...")
            
            while time.time() - start_time < download_wait_time:
                try:
                    files_now = set(os.listdir(pdf_folder))
                    crdownload_files = [f for f in files_now if f.endswith('.crdownload')]
                    
                    if not crdownload_files:
                        new_files = files_now - files_before
                        pdf_files = [f for f in new_files if f.endswith('.pdf')]
                        if pdf_files:
                            download_complete = True
                            pdf_filename = pdf_files[0]
                            pdf_found = True
                            print(f"✓ Download completed: {pdf_filename}")
                            break
                    else:
                        print(f"  Download in progress... ({len(crdownload_files)} partial file(s))")
                    
                    time.sleep(1)
                except Exception as e:
                    print(f"  Error checking download: {e}")
                    time.sleep(1)
            
            if not download_complete:
                print(f"✗ Warning: Download did not complete within {download_wait_time}s")
                pdf_found = False
                
        except Exception as e:
            print(f"✗ No PDF available: {e}")
        
        pdf_downloaded.append(pdf_found)
        pdf_filenames.append(pdf_filename)
        
        # Delay between pages
        if idx < len(df) - 1:
            time.sleep(random.uniform(2, 4))
        
    except Exception as e:
        print(f"✗ Error processing page: {e}")
        pdf_downloaded.append(False)
        pdf_filenames.append(None)

# Update dataframe with PDF info
df['pdf_downloaded'] = pdf_downloaded
df['pdf_filename'] = pdf_filenames

# Save updated dataframe
df.to_csv('data/consultation_responses.csv', index=False, encoding='utf-8')

# Print summary
print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")
print(f"Total entries: {len(df)}")
print(f"PDFs downloaded: {df['pdf_downloaded'].sum()}")
print(f"PDFs missing: {(~df['pdf_downloaded']).sum()}")
print(f"\nData saved to: data/consultation_responses.csv")
print(f"PDFs saved in: {pdf_folder}")

# Close the driver
driver.quit()
