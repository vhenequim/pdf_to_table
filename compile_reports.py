import os
import pandas as pd
import glob
import re
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_filename(filepath):
    """
    Parses the filename to extract year, trimester, page number, and original basename.
    Example filename: ./batch_processing_output/csv_1T22_p46/1T22_p46_all_tables.csv
    """
    filename = os.path.basename(filepath)
    # Regex to capture basename (like 1T22), and page number (like 46)
    # It looks for a structure like BASENAME_pPAGE_all_tables.csv
    match = re.match(r'([1-4]T\d{2,})_p(\d+)_all_tables\.csv', filename)
    if match:
        basename_part = match.group(1) # e.g., "1T22"
        page_number = int(match.group(2)) # e.g., 46

        # Extract trimester and year from basename_part
        trimester_match = re.match(r'([1-4]T)(\d{2,})', basename_part)
        if trimester_match:
            trimester_str = trimester_match.group(1) # "1T", "2T", etc.
            year_str = trimester_match.group(2) # "22", "23", etc.
            
            trimester_map = {'1T': 1, '2T': 2, '3T': 3, '4T': 4}
            trimester_num = trimester_map.get(trimester_str)
            year_num = int(year_str)
            
            if trimester_num is not None:
                return {
                    'filepath': filepath,
                    'year': year_num,
                    'trimester': trimester_num,
                    'page': page_number,
                    'original_basename': basename_part
                }
    logger.warning(f"Could not parse filename for sorting: {filename}")
    return None

def compile_csv_reports(base_dir="batch_processing_output", output_filename="compiled_financial_reports.csv"):
    """
    Compiles all '_all_tables.csv' files from subdirectories of base_dir
    into a single side-by-side CSV file.
    """
    search_pattern = os.path.join(base_dir, 'csv_*/*_all_tables.csv')
    logger.info(f"Searching for CSV files with pattern: {search_pattern}")
    
    all_csv_files = glob.glob(search_pattern, recursive=True)
    
    if not all_csv_files:
        logger.warning(f"No '_all_tables.csv' files found in {base_dir}. Exiting.")
        return

    logger.info(f"Found {len(all_csv_files)} potential CSV files to process.")

    parsed_files = [p for p in (parse_filename(f) for f in all_csv_files) if p]

    if not parsed_files:
        logger.warning("No files could be successfully parsed for sorting. Exiting.")
        return

    # Sort files: 1st by year, 2nd by trimester, 3rd by page number
    parsed_files.sort(key=lambda x: (x['year'], x['trimester'], x['page']))

    logger.info("Sorted files to process:")
    for pf in parsed_files:
        logger.info(f"  - {pf['filepath']} (Year: {pf['year']}, Trimester: {pf['trimester']}, Page: {pf['page']})")

    dataframes_details = [] # To store df, and original column info
    max_rows = 0

    for file_info in parsed_files:
        df = pd.DataFrame()
        num_original_cols = 1 # Default
        try:
            df = pd.read_csv(file_info['filepath'])
            if df.empty:
                # Try to get column count from headers for truly empty files (0 rows but headers exist)
                try:
                    header_df = pd.read_csv(file_info['filepath'], nrows=0)
                    num_original_cols = len(header_df.columns) if len(header_df.columns) > 0 else 1
                except Exception:
                    num_original_cols = 1 # Default if headers can't be read
            else:
                num_original_cols = len(df.columns)
            
            if not df.empty:
                 max_rows = max(max_rows, len(df.index))
            logger.info(f"Read {file_info['filepath']} - Rows: {len(df.index)}, Cols: {num_original_cols}")
        except pd.errors.EmptyDataError:
            logger.warning(f"Skipping empty CSV file (EmptyDataError): {file_info['filepath']}")
            num_original_cols = 1 # Default for completely empty files
        except Exception as e:
            logger.error(f"Error reading CSV file {file_info['filepath']}: {e}")
            num_original_cols = 1 # Default for error cases
        dataframes_details.append({'df': df, 'num_cols': num_original_cols, 'file_info': file_info})

    if not dataframes_details:
        logger.warning("No dataframes were successfully read or prepared. Exiting.")
        return
    
    if max_rows == 0:
        logger.warning("All CSV files were empty or unreadable. Creating an empty compiled report.")
        # If all files are empty, create an empty output file
        pd.DataFrame().to_csv(os.path.join(os.getcwd(), output_filename), index=False)
        return

    padded_dataframes = []
    for item in dataframes_details:
        df = item['df']
        file_info = item['file_info']
        num_cols_original = item['num_cols']
        file_prefix = f"{file_info['original_basename']}_p{file_info['page']}"

        if df.empty:
            # Create a DataFrame of empty strings for placeholder
            columns = [f"{file_prefix}_col{j}" for j in range(num_cols_original)]
            padded_df = pd.DataFrame('', index=range(max_rows), columns=columns)
        else:
            current_df = df.copy()
            # Pad rows if necessary
            if len(current_df.index) < max_rows:
                padding = pd.DataFrame(index=range(max_rows - len(current_df.index)), columns=current_df.columns)
                current_df = pd.concat([current_df, padding], ignore_index=True)
            elif len(current_df.index) > max_rows: # Should not happen if max_rows is calculated correctly
                current_df = current_df.iloc[:max_rows]
            
            current_df.columns = [f"{file_prefix}_{str(col)}" for col in current_df.columns]
            padded_df = current_df
        
        padded_dataframes.append(padded_df.fillna(''))

    if not padded_dataframes:
        logger.error("No dataframes available for final compilation after padding. Exiting.")
        return

    logger.info(f"All DataFrames prepared with {max_rows} rows. Concatenating side-by-side.")
    
    try:
        final_df = pd.concat(padded_dataframes, axis=1)
    except Exception as e: # Should be less likely now with consistent column naming
        logger.error(f"Critical error during final concatenation: {e}. This might indicate an unhandled edge case.")
        # As a last resort, try to create a file with what we have, or an error message
        error_df = pd.DataFrame([{'error_message': f"Concatenation failed: {e}"}])
        output_path = os.path.join(os.getcwd(), "error_in_compilation_" + output_filename)
        error_df.to_csv(output_path, index=False)
        logger.info(f"Saved error information to: {output_path}")
        return

    output_path = os.path.join(os.getcwd(), output_filename)
    try:
        final_df.to_csv(output_path, index=False, na_rep='')
        logger.info(f"Successfully compiled all reports into: {output_path}")
    except Exception as e:
        logger.error(f"Failed to save the final compiled CSV to {output_path}: {e}")

if __name__ == '__main__':
    # Ensure the base directory for inputs is clear.
    # This assumes 'batch_processing_output' is in the same directory as this script.
    # If it's elsewhere, you might need to adjust this path.
    input_base_directory = "batch_processing_output"
    
    # Check if the input directory exists
    if not os.path.isdir(input_base_directory):
        logger.error(f"Error: Input directory '{input_base_directory}' not found.")
        logger.error("Please ensure the 'docling_excel_extractor.py' script has been run and its output directory is accessible.")
        logger.error("You might need to create this directory or specify the correct path if it's located elsewhere.")
    else:
        compile_csv_reports(base_dir=input_base_directory, output_filename="compiled_financial_reports.csv") 