from Extraction_component_dir.ExtractionPipeline import TextExtraction
from Variable_artifact_dir.Artifact import EXTRACTED_CSV_PATH
from Variable_artifact_dir.Artifact import INPUT_CSV_PATH
from Variable_artifact_dir.Artifact import STOPWORDS_PATH
from Log_connection_dir.LogConnection import logging
from Components.ingestion import Ingestion


class ComponentPipeline:
    def __init__(self):
        self.extracted_csv = EXTRACTED_CSV_PATH
        self.stopwords = STOPWORDS_PATH
        self.text_extraction = TextExtraction(input_file_path=INPUT_CSV_PATH)

    def module_connection(self):
        try:
            self.text_extraction.extract_text_function()  # extraction started
            Ingestion(extracted_csv_path=self.extracted_csv, st_words_path=self.stopwords).sentiment_analysis()

        except Exception as e:
            logging.info(f"{e}")


if __name__ == '__main__':
    ComponentPipeline().module_connection()
