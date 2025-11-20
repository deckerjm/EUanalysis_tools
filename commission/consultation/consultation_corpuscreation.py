import pandas as pd
import os
import PyPDF2
import pdfplumber
from pathlib import Path

"""
The script is designed to extract text from PDF files associated with consultation responses.

The script processes a collection of PDF files and extracts their text content for analysis. 
It is designed to work with a metadata CSV file that contains information about the PDF files, 
including their filenames and associated metadata.

Inputs:
1. A metadata CSV file named 'data/consultation_responses.csv' with the following columns:
    - 'name': The name or identifier of the entry.
    - 'pdf_filename': The filename of the corresponding PDF file (relative to the 'data/consultation_pdfs' folder).

2. A folder named 'data/consultation_pdfs' containing the PDF files to be processed.

How to Use:
1. Ensure the metadata CSV file and the folder containing the PDF files are in the correct locations.
2. Run the script using Python.
3. The script will process each entry in the CSV file, attempt to extract text from the corresponding PDF file, 
    and store the extracted text in a new column ('pdf_text') in the CSV file.

Outputs:
1. The updated metadata CSV file ('data/consultation_responses.csv') with an additional column:
    - 'pdf_text': The extracted text content of the PDF file (or an empty string if extraction failed).

2. A summary of the processing results is printed to the console, including:
    - The total number of entries processed.
    - The number of PDFs with successfully extracted text.
    - The number of PDFs where text extraction failed.
"""

# Read the metadata CSV
df = pd.read_csv('data/consultation_responses.csv')

# Path to PDF folder
pdf_folder = 'data/consultation_pdfs'

# List to store extracted PDF text
pdf_texts = []

print(f"Processing {len(df)} entries...\n")

# Loop through each row in the dataframe
for idx, row in df.iterrows():
    pdf_filename = row.get('pdf_filename', None)
    
    print(f"Processing {idx+1}/{len(df)}: {row['name']}")
    
    if pd.isna(pdf_filename) or pdf_filename is None:
        print(f"  ✗ No PDF filename available")
        pdf_texts.append("")
        continue
    
    # Construct full path to PDF
    pdf_path = os.path.join(pdf_folder, pdf_filename)
    
    if not os.path.exists(pdf_path):
        print(f"  ✗ PDF file not found: {pdf_filename}")
        pdf_texts.append("")
        continue
    
    # Try to extract text from PDF
    extracted_text = ""
    
    # Method 1: Try pdfplumber (better for complex PDFs)
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text_parts = []
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    text_parts.append(text)
            extracted_text = "\n\n".join(text_parts)
            
            if extracted_text.strip():
                print(f"  ✓ Extracted {len(extracted_text)} characters using pdfplumber")
            else:
                raise Exception("No text extracted with pdfplumber")
                
    except Exception as e:
        print(f"  ⚠ pdfplumber failed: {e}")
        
        # Method 2: Fallback to PyPDF2
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text_parts = []
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
                extracted_text = "\n\n".join(text_parts)
                
                if extracted_text.strip():
                    print(f"  ✓ Extracted {len(extracted_text)} characters using PyPDF2")
                else:
                    print(f"  ✗ No text could be extracted from PDF")
                    
        except Exception as e2:
            print(f"  ✗ PyPDF2 also failed: {e2}")
    
    pdf_texts.append(extracted_text)

# Add PDF text to dataframe
df['pdf_text'] = pdf_texts

# Save the updated dataframe
output_path = 'data/consultation_responses.csv'
df.to_csv(output_path, index=False, encoding='utf-8')

# Print summary
print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")
print(f"Total entries: {len(df)}")
print(f"PDFs with text extracted: {(df['pdf_text'] != '').sum()}")
print(f"PDFs without text: {(df['pdf_text'] == '').sum()}")
print(f"\nData saved to: {output_path}")
