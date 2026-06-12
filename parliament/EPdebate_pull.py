import csv
import re
import xml.etree.ElementTree as ET
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
import os


def parse_filename_metadata(filename):
    """
    Extract debate_date and ep_agenda_item from filename.
    Expects format like: CRE-9-2022-07-04-ITM-015_EN.xml
    Returns (ep_agenda_item, debate_date) where debate_date is DD.MM.YYYY
    """
    basename = os.path.basename(filename)
    basename = basename.replace('.xml', '').replace('_EN', '')

    ep_agenda_item = basename

    parts = basename.split('-')
    try:
        year = parts[2]
        month = parts[3]
        day = parts[4]
        debate_date = f"{day}.{month}.{year}"
    except IndexError:
        debate_date = ''

    return ep_agenda_item, debate_date


def extract_ep_debate_data(xml_path, domain, date_str, start_number):
    """
    Extract MEP intervention data from a local European Parliament XML debate file.
    Returns a list of intervention dicts — does NOT write CSV (handled by caller).

    Args:
        xml_path: Path to the local XML file
        domain: Domain identifier for URN
        date_str: Date in format YYMMDD (e.g., '220704' for 04 Jul 2022)
        start_number: Starting number for consecutive URN numbering
    """
    ep_agenda_item, debate_date = parse_filename_metadata(xml_path)

    # Read as binary to detect encoding from XML declaration
    with open(xml_path, 'rb') as f:
        raw = f.read()

    if not raw.strip():
        print(f"  ⚠ Skipping {os.path.basename(xml_path)} — file is empty")
        return []

    # Detect encoding from XML declaration, fallback to utf-8
    match = re.search(rb'encoding=["\']([^"\']+)["\']', raw[:200])
    encoding = match.group(1).decode('ascii') if match else 'utf-8'

    # Decode with detected encoding, then normalise to clean UTF-8
    content = raw.decode(encoding, errors='replace')
    content = content.encode('utf-8', errors='replace').decode('utf-8')

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  ⚠ Skipping {os.path.basename(xml_path)} — XML parse error: {e}")
        return []

    interventions_data = []
    current_number = start_number

    for intervention in root.findall('.//INTERVENTION'):
        orateur = intervention.find('ORATEUR')

        if orateur is not None:
            pp = orateur.get('PP', '')
            lg = orateur.get('LG', '')
            mepid = orateur.get('MEPID', '')
            lib = orateur.get('LIB', '')
            speaker_type = orateur.get('SPEAKER_TYPE', '')

            if '|' in lib:
                parts = lib.split('|')
                first_name = parts[0].strip()
                last_name = parts[1].strip()
            else:
                first_name = lib
                last_name = ''
        else:
            pp = lg = mepid = lib = speaker_type = first_name = last_name = ''

        para_texts = []
        for para in intervention.findall('.//PARA'):
            text_content = ''.join(para.itertext()).strip()
            if text_content:
                para_texts.append(text_content)

        full_text = '\n\n'.join(para_texts)

        urn = f'deb-{domain}-{date_str}-{current_number}'

        interventions_data.append({
            'URN': urn,
            'ep_agenda_item': ep_agenda_item,
            'debate_date': debate_date,
            'PP': pp,
            'LG': lg,
            'MEPID': mepid,
            'First_Name': first_name,
            'Last_Name': last_name,
            'SPEAKER_TYPE': speaker_type,
            'Text': full_text
        })

        current_number += 1

    print(f"  → Extracted {len(interventions_data)} interventions from {os.path.basename(xml_path)}")
    return interventions_data


def write_csv(interventions_data, output_csv):
    """Write a list of intervention dicts to a single combined CSV file."""
    fieldnames = ['URN', 'ep_agenda_item', 'debate_date', 'PP', 'LG', 'MEPID',
                  'First_Name', 'Last_Name', 'SPEAKER_TYPE', 'Text']

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(interventions_data)

    print(f"✓ CSV written: {output_csv} ({len(interventions_data)} rows)")


def save_interventions_as_pdfs(interventions_data, output_folder='data/clean/mep_debates'):
    """
    Save each intervention text as a separate PDF file named by URN.

    Args:
        interventions_data: List of intervention dictionaries
        output_folder: Folder to save PDF files
    """
    os.makedirs(output_folder, exist_ok=True)

    styles = getSampleStyleSheet()
    normal_style = styles['Normal']

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
        ep_agenda_item = intervention.get('ep_agenda_item', '')
        debate_date = intervention.get('debate_date', '')

        domain = urn.split('-')[1] if len(urn.split('-')) > 1 else ''

        pdf_filename = os.path.join(output_folder, f"{urn}.pdf")

        try:
            doc = SimpleDocTemplate(pdf_filename, pagesize=A4)
            story = []

            if ep_agenda_item:
                story.append(Paragraph(f"<b>Agenda Item:</b> {ep_agenda_item}", header_style))
            if debate_date:
                story.append(Paragraph(f"<b>Debate Date:</b> {debate_date}", header_style))
            if domain:
                story.append(Paragraph(f"<b>Domain:</b> {domain}", header_style))

            speaker_name = f"{first_name} {last_name}".strip()
            if speaker_name:
                story.append(Paragraph(f"<b>Speaker:</b> {speaker_name}", header_style))

            story.append(Spacer(1, 0.2 * inch))

            if text:
                story.append(Paragraph(text.replace('\n', '<br/>'), normal_style))
            else:
                story.append(Paragraph("[No text content]", normal_style))

            doc.build(story)

            if (i + 1) % 10 == 0:
                print(f"  Saved {i + 1}/{len(interventions_data)} PDFs...")

        except Exception as e:
            print(f"  Error saving {urn}.pdf: {e}")

    print(f"✓ PDFs saved to {output_folder}")


def process_folder(xml_folder, domain, date_str,
                   output_csv='data/ep_debate_data.csv',
                   output_folder='data/clean/mep_debates'):
    """
    Process all XML files in a folder, write one combined CSV, and save PDFs.

    Args:
        xml_folder: Path to folder containing XML files
        domain: Domain identifier for URN
        date_str: Date string for URN (YYMMDD)
        output_csv: Output CSV path for combined results
        output_folder: Output folder for PDFs
    """
    xml_files = sorted([
        os.path.join(xml_folder, f)
        for f in os.listdir(xml_folder)
        if f.endswith('.xml')
    ])

    if not xml_files:
        print(f"No XML files found in {xml_folder}")
        return

    print(f"Found {len(xml_files)} XML file(s) to process...\n")

    all_interventions = []
    current_number = 1

    for xml_path in xml_files:
        print(f"Processing: {os.path.basename(xml_path)}")
        interventions = extract_ep_debate_data(
            xml_path, domain, date_str,
            start_number=current_number
        )
        all_interventions.extend(interventions)
        current_number += len(interventions)

    # Write single combined CSV after all files are processed
    write_csv(all_interventions, output_csv)

    save_interventions_as_pdfs(all_interventions, output_folder)

    print(f"\n✓ Done. Total interventions processed: {len(all_interventions)}")


# Example usage
if __name__ == "__main__":
    # --- Single file mode ---
    # interventions = extract_ep_debate_data(
    #     xml_path="data/xml_debates/CRE-9-2022-07-04-ITM-015_EN.xml",
    #     domain="DSA",
    #     date_str="220704",
    #     start_number=1
    # )
    # write_csv(interventions, "data/ep_debate_data.csv")
    # save_interventions_as_pdfs(interventions)

    # --- Folder mode ---
    process_folder(
        xml_folder="data/xml_debates",
        domain="AI",
        date_str="260612",
        output_csv="output/ep_debate_data.csv",
        output_folder="data/clean/mep_debates"
    )