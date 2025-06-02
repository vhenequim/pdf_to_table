import os
import pandas as pd
import glob
import re
import logging
from collections import defaultdict

# Required for writing to .xlsx: pip install openpyxl
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_filename_for_trimester_sheet(filepath):
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
    logger.warning(f"Could not parse filename for trimester sheet: {filename}")
    return None

def create_excel_by_trimester(base_dir="batch_processing_output", output_excel_filename="trimester_reports.xlsx"):
    """
    Finds all '_all_tables.csv' files, groups them by trimester,
    concatenates data from multiple pages within a trimester vertically,
    and saves each trimester's data to a separate sheet in an Excel file.
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
        parsed = parse_filename_for_trimester_sheet(f_path)
        if parsed:
            parsed_file_details.append(parsed)

    if not parsed_file_details:
        logger.warning("No files could be successfully parsed. Exiting.")
        return

    # Group files by trimester_id
    trimester_grouped_files = defaultdict(list)
    for detail in parsed_file_details:
        trimester_grouped_files[detail['trimester_id']].append(detail)

    # Sort trimesters for consistent sheet order (e.g., 1T22, 2T22, ..., 4T23, 1T24)
    # Extract year and quarter number for sorting
    def sort_key_trimester(trimester_id_key):
        match = re.match(r'([1-4])T(\d{2,})', trimester_id_key)
        if match:
            quarter = int(match.group(1))
            year = int(match.group(2))
            return (year, quarter)
        return (9999, 9) # Should not happen if parsing is correct

    sorted_trimester_ids = sorted(trimester_grouped_files.keys(), key=sort_key_trimester)
    
    output_excel_path = os.path.join(os.getcwd(), output_excel_filename)
    try:
        with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
            logger.info(f"Processing trimesters and writing to Excel sheets in {output_excel_path}...")
            for trimester_id in sorted_trimester_ids:
                files_for_trimester = trimester_grouped_files[trimester_id]
                # Sort files within this trimester by page number
                files_for_trimester.sort(key=lambda x: x['page'])

                logger.info(f"Processing trimester: {trimester_id} with {len(files_for_trimester)} page(s).")
                
                trimester_dataframes = []
                for file_detail in files_for_trimester:
                    try:
                        df = pd.read_csv(file_detail['filepath'])
                        if not df.empty:
                            trimester_dataframes.append(df)
                            logger.debug(f"  Read and added {file_detail['filepath']} (Page {file_detail['page']}) for sheet {trimester_id}")
                        else:
                            logger.info(f"  Skipping empty CSV: {file_detail['filepath']} for sheet {trimester_id}")
                    except pd.errors.EmptyDataError:
                        logger.info(f"  Skipping empty CSV (EmptyDataError): {file_detail['filepath']} for sheet {trimester_id}")
                    except Exception as e:
                        logger.error(f"  Error reading CSV {file_detail['filepath']} for sheet {trimester_id}: {e}")
                
                if trimester_dataframes:
                    # Concatenate all dataframes for this trimester vertically
                    combined_trimester_df = pd.concat(trimester_dataframes, ignore_index=True)
                    
                    # Ensure sheet name is valid (Excel has a 31 char limit)
                    sheet_name = trimester_id
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                        logger.warning(f"Sheet name '{trimester_id}' truncated to '{sheet_name}' due to length limit.")
                    
                    combined_trimester_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Successfully wrote data for trimester {trimester_id} to sheet '{sheet_name}'. Total rows: {len(combined_trimester_df)}")
                else:
                    logger.warning(f"No data found or read for trimester {trimester_id}. Sheet will not be created.")
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
        create_excel_by_trimester(base_dir=input_base_directory, output_excel_filename="trimester_reports.xlsx") 