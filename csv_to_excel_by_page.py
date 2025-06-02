import os
import pandas as pd
import glob
import re
import logging

# Required for writing to .xlsx: pip install openpyxl
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_filename_for_page_sheet(filepath):
    """
    Parses the filename to extract trimester identifier (e.g., "1T22") and page number.
    Example filename: ./batch_processing_output/csv_1T22_p46/1T22_p46_all_tables.csv
    Returns a dictionary with 'trimester_id', 'page', and 'filepath', or None.
    """
    filename = os.path.basename(filepath)
    # Regex to capture the trimester part (e.g., 1T22) and page number
    match = re.match(r'([1-4]T\d{2,})_p(\d+)_all_tables\.csv', filename)
    if match:
        trimester_id = match.group(1)  # e.g., "1T22"
        page_number = int(match.group(2))
        return {
            'trimester_id': trimester_id,
            'page': page_number,
            'filepath': filepath
        }
    logger.warning(f"Could not parse filename for page sheet: {filename}")
    return None

def create_excel_by_page(base_dir="batch_processing_output", output_excel_filename="trimester_page_reports.xlsx"):
    """
    Finds all '_all_tables.csv' files, sorts them by trimester and page,
    and saves each CSV's data to a separate sheet in an Excel file,
    named like 'TrimesterID_pPageNumber'.
    """
    search_pattern = os.path.join(base_dir, 'csv_*/*_all_tables.csv')
    logger.info(f"Searching for CSV files with pattern: {search_pattern}")
    all_csv_files = glob.glob(search_pattern, recursive=True)

    if not all_csv_files:
        logger.warning(f"No '_all_tables.csv' files found in {base_dir}. Exiting.")
        return

    logger.info(f"Found {len(all_csv_files)} potential CSV files to process.")

    parsed_file_details = []
    for f_path in all_csv_files:
        parsed = parse_filename_for_page_sheet(f_path)
        if parsed:
            parsed_file_details.append(parsed)

    if not parsed_file_details:
        logger.warning("No files could be successfully parsed. Exiting.")
        return

    # Sort files: 1st by trimester_id (chronologically), 2nd by page number
    def sort_key_page_sheet(file_detail_item):
        trimester_id_key = file_detail_item['trimester_id']
        page_num = file_detail_item['page']
        match_trimester = re.match(r'([1-4])T(\d{2,})', trimester_id_key)
        if match_trimester:
            quarter = int(match_trimester.group(1))
            year = int(match_trimester.group(2))
            return (year, quarter, page_num)
        return (9999, 9, page_num) # Fallback for sorting if trimester parsing fails

    parsed_file_details.sort(key=sort_key_page_sheet)
    
    output_excel_path = os.path.join(os.getcwd(), output_excel_filename)
    try:
        with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
            logger.info(f"Processing CSVs and writing to Excel sheets in {output_excel_path}...")
            
            for file_detail in parsed_file_details:
                trimester_id = file_detail['trimester_id']
                page = file_detail['page']
                filepath = file_detail['filepath']
                
                sheet_name_base = f"{trimester_id}_p{page}"
                # Ensure sheet name is valid (Excel has a 31 char limit)
                sheet_name = sheet_name_base
                if len(sheet_name) > 31:
                    sheet_name = sheet_name[:31]
                    logger.warning(f"Sheet name '{sheet_name_base}' truncated to '{sheet_name}' due to length limit.")
                
                try:
                    df = pd.read_csv(filepath)
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"Successfully wrote data from {filepath} to sheet '{sheet_name}'.")
                    else:
                        # Optionally, create an empty sheet or skip
                        # For now, let's create an empty sheet to indicate the file was processed
                        pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"CSV file {filepath} was empty. Created empty sheet '{sheet_name}'.")
                except pd.errors.EmptyDataError:
                    pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"CSV file {filepath} caused EmptyDataError. Created empty sheet '{sheet_name}'.")
                except Exception as e:
                    logger.error(f"Error reading or writing CSV {filepath} to sheet '{sheet_name}': {e}")
                    # Optionally, create a sheet with an error message
                    error_df = pd.DataFrame([{'error': str(e)}])
                    error_sheet_name = f"ERR_{sheet_name}"[:31]
                    error_df.to_excel(writer, sheet_name=error_sheet_name, index=False)

        logger.info(f"Successfully created Excel file: {output_excel_path}")

    except ImportError:
        logger.error("The 'openpyxl' library is required to write Excel files. Please install it using: pip install openpyxl")
    except Exception as e:
        logger.error(f"An error occurred while creating the Excel file: {e}")

if __name__ == '__main__':
    input_base_directory = "batch_processing_output"
    
    if not os.path.isdir(input_base_directory):
        logger.error(f"Error: Input directory '{input_base_directory}' not found.")
        logger.error("Please ensure the 'docling_excel_extractor.py' script has run and its output directory is accessible.")
    else:
        create_excel_by_page(base_dir=input_base_directory, output_excel_filename="trimester_page_reports.xlsx") 