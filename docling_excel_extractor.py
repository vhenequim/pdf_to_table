import os
import pandas as pd # Keep for potential future use, but not for this text export
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.datamodel.base_models import InputFormat
import logging
import tempfile
from pdf2image import convert_from_path # Added for pdf2image
from pdf2image.exceptions import (\
    PDFInfoNotInstalledError,\
    PDFPageCountError,\
    PDFSyntaxError,\
    PDFPopplerTimeoutError\
)
import markdown # For converting Markdown to HTML
import pathlib # For robust path operations

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def convert_markdown_to_csv(markdown_file_path: str, output_csv_dir: str, pdf_basename: str, page_number: int):
    """
    Reads a Markdown file, extracts tables, saves them as individual CSV files,
    and also saves a single CSV file concatenating all tables with blank line separators.

    Args:
        markdown_file_path (str): Path to the input Markdown file.
        output_csv_dir (str): Directory to save the output CSV files.
        pdf_basename (str): Base name of the PDF file (without extension), used for naming the concatenated CSV.
        page_number (int): Page number from the PDF, used for naming the concatenated CSV.
    """
    try:
        if not os.path.exists(markdown_file_path):
            logger.error(f"Markdown file not found: {markdown_file_path}")
            return False # Indicate failure

        with open(markdown_file_path, 'r', encoding='utf-8') as f:
            md_content = f.read()

        html_content = markdown.markdown(md_content, extensions=['markdown.extensions.tables'])
        dfs = pd.read_html(html_content)

        if not dfs:
            logger.warning(f"No tables found in the Markdown file: {markdown_file_path}")
            return True # No tables is not an error, but nothing was processed

        os.makedirs(output_csv_dir, exist_ok=True)

        logger.info(f"Found {len(dfs)} tables in {markdown_file_path}. Processing for individual CSV export and concatenation.")
        
        cleaned_and_saved_tables = [] # List to store DataFrames that are valid and saved

        for i, df in enumerate(dfs):
            df_copy = df.copy() # Work on a copy for cleaning
            # Clean up DataFrame: remove rows/cols that are entirely NaN
            df_copy.dropna(how='all', axis=0, inplace=True) # Drop rows where all elements are NaN
            df_copy.dropna(how='all', axis=1, inplace=True) # Drop columns where all elements are NaN
            df_copy.reset_index(drop=True, inplace=True)
            
            if df_copy.empty:
                logger.info(f"Table {i+1} from {markdown_file_path} is empty after cleaning, skipping.")
                continue

            # Save individual table
            individual_csv_path = os.path.join(output_csv_dir, f"table_{i+1}.csv")
            df_copy.to_csv(individual_csv_path, index=False)
            logger.info(f"Saved table {i+1} from {markdown_file_path} to {individual_csv_path}")
            
            cleaned_and_saved_tables.append(df_copy)

        if not cleaned_and_saved_tables:
            logger.info(f"No non-empty tables were available for concatenation from {markdown_file_path}.")
            return True

        # Concatenate tables with a blank line separator
        if len(cleaned_and_saved_tables) == 1:
            final_df_to_save = cleaned_and_saved_tables[0]
            logger.info(f"Only one table found in {markdown_file_path}; this will be saved as the 'all_tables' CSV.")
        else:
            logger.info(f"Concatenating {len(cleaned_and_saved_tables)} tables from {markdown_file_path} with blank line separators.")
            tables_to_concat_with_separators = []
            for k, table_df in enumerate(cleaned_and_saved_tables):
                tables_to_concat_with_separators.append(table_df)
                if k < len(cleaned_and_saved_tables) - 1: # If not the last table
                    tables_to_concat_with_separators.append(pd.DataFrame([{}])) # Adds a blank row for pd.concat
            final_df_to_save = pd.concat(tables_to_concat_with_separators, ignore_index=True)
        
        concatenated_csv_name = f"{pdf_basename}_p{page_number}_all_tables.csv"
        concatenated_csv_full_path = os.path.join(output_csv_dir, concatenated_csv_name)
        final_df_to_save.to_csv(concatenated_csv_full_path, index=False)
        logger.info(f"All tables from {markdown_file_path} (PDF: {pdf_basename}, Page: {page_number}) concatenated and saved to {concatenated_csv_full_path}")

        return True
    except ImportError as ie:
        logger.error(f"Required library not found for Markdown/HTML parsing: {ie}")
        logger.error("Please ensure 'markdown' and 'lxml' are installed (e.g., pip install markdown lxml)")
        return False
    except Exception as e:
        logger.error(f"Error converting Markdown tables to CSV for {markdown_file_path}: {e}")
        return False

def extract_text_and_tables_via_ocr_with_docling(
    pdf_path: str, 
    page_number_to_extract: int, 
    output_markdown_path: str, 
    output_csv_dir: str, # Added CSV output directory
    poppler_path_param: str = None
):
    """
    Converts a PDF page to image, OCRs with Docling, saves text as Markdown,
    then converts tables from Markdown to CSV files.

    Args:
        pdf_path (str): Path to PDF.
        page_number_to_extract (int): 1-indexed page number.
        output_markdown_path (str): Path to save extracted Markdown text.
        output_csv_dir (str): Directory to save output CSV files from Markdown tables.
        poppler_path_param (str, optional): Path to Poppler bin.
    """
    if not os.path.exists(pdf_path):
        logger.error(f"Error: PDF file not found at {pdf_path}")
        return

    temp_image_path = None
    docling_doc = None
    markdown_content_generated = False # Flag to check if markdown was made
    try:
        logger.info(f"Processing PDF: '{os.path.basename(pdf_path)}', Page: {page_number_to_extract}")
        logger.info(f"Converting page {page_number_to_extract} to image...")
        images = convert_from_path(
            pdf_path,
            dpi=300,
            first_page=page_number_to_extract,
            last_page=page_number_to_extract,
            poppler_path=poppler_path_param,
            fmt='png'
        )

        if not images:
            logger.error(f"pdf2image: No images returned for page {page_number_to_extract} from {pdf_path}.")
            return

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
            temp_image_path = tmp_file.name
        images[0].save(temp_image_path, 'PNG')
        logger.info(f"Page {page_number_to_extract} converted to temp image: {temp_image_path}")

        logger.info(f"Initializing Docling for temp image: {temp_image_path}...")
        pipeline_options = PdfPipelineOptions(
            force_ocr=True,
            do_table_structure=True, # Keep true, might help structure text even if not making tables
            table_structure_options_kwargs={
                "mode": TableFormerMode.ACCURATE,
                "do_cell_matching": False 
            }
        )
        doc_converter = DocumentConverter(
            format_options={
                InputFormat.IMAGE: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
        
        logger.info(f"Starting image conversion with Docling for '{temp_image_path}'...")
        conversion_result = doc_converter.convert(temp_image_path)
        docling_doc = conversion_result.document
        logger.info("Docling image conversion complete.")

        if docling_doc:
            # Ensure output directory for markdown exists
            os.makedirs(os.path.dirname(output_markdown_path), exist_ok=True)
            logger.info(f"Exporting OCRed text to Markdown: {output_markdown_path}")
            markdown_text = docling_doc.export_to_markdown()
            
            with open(output_markdown_path, 'w', encoding='utf-8') as f:
                f.write(markdown_text)
            logger.info(f"Successfully saved OCRed text to {output_markdown_path}")
            markdown_content_generated = True
        else:
            logger.warning(f"Docling did not produce a document for {pdf_path} page {page_number_to_extract}; cannot extract text or tables.")
            return

    except (PDFInfoNotInstalledError, PDFPageCountError, PDFSyntaxError, PDFPopplerTimeoutError) as e_pdf2image:
        logger.error(f"pdf2image error for {pdf_path}: {e_pdf2image}. Ensure Poppler is installed and in PATH or path is set.")
        return
    except ImportError as e_import:
        logger.error(f"ImportError for {pdf_path}: {e_import}. Ensure all libraries are installed.")
        return
    except Exception as e:
        logger.error(f"Error during OCR/Docling processing for {pdf_path} page {page_number_to_extract}: {e}")
        return
    finally:
        if temp_image_path and os.path.exists(temp_image_path):
            try:
                os.remove(temp_image_path)
                logger.info(f"Temporary image file {temp_image_path} removed.")
            except Exception as e_remove:
                logger.error(f"Failed to remove temp image: {e_remove}")

    # --- Convert Markdown tables to CSV ---
    if markdown_content_generated:
        logger.info(f"Proceeding to convert tables from {output_markdown_path} to CSVs in {output_csv_dir}.")
        pdf_basename_for_csv = pathlib.Path(pdf_path).stem
        success = convert_markdown_to_csv(output_markdown_path, output_csv_dir, pdf_basename_for_csv, page_number_to_extract)
        if success:
            logger.info(f"Successfully converted tables to CSVs for {os.path.basename(pdf_path)}, page {page_number_to_extract}.")
        else:
            logger.error(f"Failed to convert tables to CSVs for {os.path.basename(pdf_path)}, page {page_number_to_extract}.")
    else:
        logger.warning(f"Markdown content was not generated for {pdf_path} page {page_number_to_extract}, skipping CSV conversion.")

if __name__ == '__main__':
    # --- User Configuration for Batch Processing ---
    # Dictionary: { "pdf_filename_or_path": [page_number_to_extract1, page_number_to_extract2, ...] }
    pdfs_to_process = {
        "1T25.pdf": [39, 40], # Your existing example
        "1T24.pdf": [39, 40],
        "2T24.pdf": [38, 39],
        "3T24.pdf": [43, 44],
        "4T24.pdf": [61, 62, 63, 64],
        "1T23.pdf": [38, 39],
        "2T23.pdf": [35, 36],
        "3T23.pdf": [35, 36],
        "4T23.pdf": [56, 57, 58, 59],
        "1T22.pdf": [46, 47, 48, 49],
        "2T22.pdf": [66, 67, 68, 69],
        "3T22.pdf": [47, 48, 49, 50], # Example: process pages 47, 48, 49, and 50
        "4T22.pdf": [66, 67, 68, 69]  # Example: process pages 66, 67, 68, and 69
        # Add more PDFs and their page numbers here, for example:
        # "another_report.pdf": [5, 6, 10],
        # "financial_summary_2023.pdf": [12], # Single page still in a list
        # "C:\\full\\path\\to\\datasheet.pdf": [2, 3]
    }

    # IMPORTANT: If Poppler is not in your system PATH, provide the path to its 'bin' directory here.
    # For example: POPPLER_PATH = r"C:\path\to\poppler-xxx\bin"
    POPPLER_PATH = None # Set to your Poppler path if needed, e.g., r"C:\poppler-23.11.0\Library\bin"
    # --- End User Configuration ---

    current_dir = os.getcwd()
    base_output_dir = os.path.join(current_dir, "batch_processing_output")
    os.makedirs(base_output_dir, exist_ok=True)

    logger.info(f"Starting batch PDF-to-Image -> OCR (Markdown) -> Tables (CSV) process...")
    logger.info(f"Output will be saved in subdirectories under: {base_output_dir}")

    for pdf_input_path_or_name, pages_to_extract in pdfs_to_process.items():
        # Resolve PDF path (can be absolute or relative to script dir)
        if not os.path.isabs(pdf_input_path_or_name):
            pdf_full_path = os.path.join(current_dir, pdf_input_path_or_name)
        else:
            pdf_full_path = pdf_input_path_or_name

        if not os.path.exists(pdf_full_path):
            logger.error(f"PDF file not found: {pdf_full_path}. Skipping all pages for this entry: {pdf_input_path_or_name}.")
            continue

        pdf_basename = pathlib.Path(pdf_full_path).stem # Gets filename without extension
        logger.info(f"--- Starting processing for PDF: {pdf_input_path_or_name} ---")

        for page_to_extract in pages_to_extract:
            logger.info(f"    Processing Page: {page_to_extract} of {pdf_input_path_or_name}")
            
            # Define specific output paths for this PDF and page
            markdown_output_filename = f"ocr_md_{pdf_basename}_p{page_to_extract}.md"
            csv_output_subdir_name = f"csv_{pdf_basename}_p{page_to_extract}"
            
            markdown_output_path = os.path.join(base_output_dir, markdown_output_filename)
            csv_output_dir_path = os.path.join(base_output_dir, csv_output_subdir_name)

            extract_text_and_tables_via_ocr_with_docling(
                pdf_full_path, 
                page_to_extract, 
                markdown_output_path, 
                csv_output_dir_path, 
                poppler_path_param=POPPLER_PATH
            )
            logger.info(f"    Finished processing Page: {page_to_extract} of {pdf_input_path_or_name}")
        logger.info(f"--- Finished all pages for PDF: {pdf_input_path_or_name} ---")
    logger.info("Batch process finished.") 