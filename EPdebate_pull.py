import requests
import csv
from datetime import datetime

import xml.etree.ElementTree as ET
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
import os

def extract_ep_debate_data(xml_url, domain, date_str, start_number, output_csv='data/clean/ep_debate_data.csv'):
    """
    Extract MEP intervention data from European Parliament XML debate files.
    
    Args:
        xml_url: URL to the XML file
        domain: Domain identifier for URN
        date_str: Date in format YYMMDD (e.g., '201219' for 19 Dec 2020)
        start_number: Starting number for consecutive URN numbering
        output_csv: Output CSV filename
    """
    # Fetch XML content
    response = requests.get(xml_url)
    response.raise_for_status()
    response.encoding = 'utf-8'  # Add this line to force UTF-8 encoding
    
    # Parse XML - use response.text instead of response.content
    root = ET.fromstring(response.text.encode('utf-8'))
    
    # Prepare data for CSV
    interventions_data = []
    current_number = start_number
    
    # Find all INTERVENTION elements
    for intervention in root.findall('.//INTERVENTION'):
        # Extract metadata from ORATEUR element
        orateur = intervention.find('ORATEUR')
        
        if orateur is not None:
            pp = orateur.get('PP', '')
            lg = orateur.get('LG', '')
            mepid = orateur.get('MEPID', '')
            lib = orateur.get('LIB', '')
            speaker_type = orateur.get('SPEAKER_TYPE', '')
            
            # Split LIB into first and last name
            if '|' in lib:
                parts = lib.split('|')
                first_name = parts[0].strip()
                last_name = parts[1].strip()
            else:
                first_name = lib
                last_name = ''
        else:
            pp = lg = mepid = lib = speaker_type = first_name = last_name = ''
        
        # Extract all text from PARA elements
        para_texts = []
        for para in intervention.findall('.//PARA'):
            # Get all text content, including from child elements
            text_content = ''.join(para.itertext()).strip()
            if text_content:
                para_texts.append(text_content)
        
        # Combine all paragraphs into single text
        full_text = '\n\n'.join(para_texts)
        
        # Construct URN
        urn = f'deb-{domain}-{date_str}-{current_number}'
        
        # Append to data list
        interventions_data.append({
            'URN': urn,
            'PP': pp,
            'LG': lg,
            'MEPID': mepid,
            'First_Name': first_name,
            'Last_Name': last_name,
            'SPEAKER_TYPE': speaker_type,
            'Text': full_text
        })
        
        current_number += 1
    
    # Write to CSV
    if interventions_data:
        fieldnames = ['URN', 'PP', 'LG', 'MEPID', 'First_Name', 'Last_Name', 'SPEAKER_TYPE', 'Text']
        
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(interventions_data)
        
        print(f"Successfully extracted {len(interventions_data)} interventions to {output_csv}")
    else:
        print("No interventions found in the XML")

    return interventions_data

def save_interventions_as_pdfs(interventions_data, output_folder='data/clean/mep_debates'):
    """
    Save each intervention text as a separate PDF file named by URN.
    
    Args:
        interventions_data: List of intervention dictionaries
        output_folder: Folder to save PDF files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Setup paragraph styles
    styles = getSampleStyleSheet()
    normal_style = styles['Normal']
    
    # Create custom style for header
    header_style = ParagraphStyle(
        'Header',
        parent=styles['Heading2'],
        fontSize=12,
        textColor='black',
        spaceAfter=12
    )
    
    print(f"\nSaving {len(interventions_data)} interventions as PDFs...")
    
    for i, intervention in enumerate(interventions_data):
        urn = intervention['URN']
        text = intervention['Text']
        first_name = intervention.get('First_Name', '')
        last_name = intervention.get('Last_Name', '')
        
        # Extract domain from URN (format: deb-DOMAIN-date-number)
        domain = urn.split('-')[1] if len(urn.split('-')) > 1 else ''
        
        # Create PDF filename
        pdf_filename = os.path.join(output_folder, f"{urn}.pdf")
        
        try:
            # Create PDF document
            doc = SimpleDocTemplate(pdf_filename, pagesize=A4)
            story = []
            
            # Add domain header
            if domain:
                domain_para = Paragraph(f"<b>Domain:</b> {domain}", header_style)
                story.append(domain_para)
            
            # Add speaker name header
            speaker_name = f"{first_name} {last_name}".strip()
            if speaker_name:
                speaker_para = Paragraph(f"<b>Speaker:</b> {speaker_name}", header_style)
                story.append(speaker_para)
            
            # Add spacing
            story.append(Spacer(1, 0.2 * inch))
            
            # Add text as paragraph
            if text:
                para = Paragraph(text.replace('\n', '<br/>'), normal_style)
                story.append(para)
            else:
                para = Paragraph("[No text content]", normal_style)
                story.append(para)
            
            # Build PDF
            doc.build(story)
            
            if (i + 1) % 10 == 0:  # Progress update every 10 files
                print(f"  Saved {i + 1}/{len(interventions_data)} PDFs...")
                
        except Exception as e:
            print(f"  Error saving {urn}.pdf: {e}")
    
    print(f"✓ Successfully saved {len(interventions_data)} PDFs to {output_folder}")

# Example usage
if __name__ == "__main__":
    xml_url = "https://www.europarl.europa.eu/doceo/document/CRE-9-2022-07-04-ITM-015_EN.xml"
    domain = "DSA"  # User defined domain
    date_str = "251120"  # 19 January 2022 in YYMMDD format
    start_number = 1  # User defined starting number
    
    interventions_data = extract_ep_debate_data(xml_url, domain, date_str, start_number)
    save_interventions_as_pdfs(interventions_data)