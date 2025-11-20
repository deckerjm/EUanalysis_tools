import pandas as pd
import os
import PyPDF2
import pdfplumber
from pathlib import Path

# Read the metadata CSV
df = pd.read_csv('data/clean/consultation_responses.csv')

# Path to PDF folder
pdf_folder = 'data/raw/consultation_pdfs'

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
output_path = 'data/clean/consultation_responses.csv'
df.to_csv(output_path, index=False, encoding='utf-8')

# Print summary
print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")
print(f"Total entries: {len(df)}")
print(f"PDFs with text extracted: {(df['pdf_text'] != '').sum()}")
print(f"PDFs without text: {(df['pdf_text'] == '').sum()}")
print(f"\nData saved to: {output_path}")