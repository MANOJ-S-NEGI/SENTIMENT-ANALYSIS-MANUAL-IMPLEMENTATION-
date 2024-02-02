from Extraction_component_dir.UrlRequest import DataExtraction
from Variable_artifact_dir.Artifact import UNRESPONSIVE_CSV_PATH
from Variable_artifact_dir.Artifact import EXTRACTED_CSV_PATH
from Log_connection_dir.LogConnection import logging
import pandas as pd
from datetime import datetime


class TextExtraction:
    def __init__(self, input_file_path):
        self.path = input_file_path

    def extract_text_function(self):
        start_time = datetime.now()  # Assuming you have a start_time
        try:
            logging.info(f"calling Csv file")
            input_file = pd.read_csv(self.path)

            # Create a new DataFrame to store the extracted data
            result_df = pd.DataFrame(columns=['URL_ID', 'URL', 'TITLE', 'URL_DATA'])

            # Loop through each row in the DataFrame
            for index, rows in input_file.iterrows():
                url = rows.URL
                data_extraction = DataExtraction(
                    title_class_alternate="tdb-title-text",
                    content_class_alternate="tdb-block-inner td-fix-index",
                    title_class="entry-title",
                    content_class="td-post-content tagdiv-type",
                    url=url)

                # Check if DataExtraction was successful
                if data_extraction.Data_extract():
                    artical_title, url_content = data_extraction.Data_extract()

                    # Assigning to new columns in DataFrame using .loc
                    result_df.loc[index, 'URL_ID'] = rows.URL_ID
                    result_df.loc[index, 'URL'] = rows.URL
                    result_df.loc[index, 'TITLE'] = artical_title[0]
                    result_df.loc[index, 'URL_DATA'] = url_content[0]

            result_df.to_csv(EXTRACTED_CSV_PATH, index=False)  # saving result into csv
            unresponsive_url = pd.read_csv(EXTRACTED_CSV_PATH)  # calling saved csv
            nan_rows = unresponsive_url[unresponsive_url['URL_DATA'].isna()]  # checking if any unresponsive link
            nan_rows = nan_rows.url
            nan_rows.to_csv(UNRESPONSIVE_CSV_PATH, index=False)  # saving link unresponsive or 404
            logging.info(f"Execution time of extraction {datetime.now() - start_time}")

        except Exception as e:
            logging.info(f"Error in module Extraction pipeline :{e}")


