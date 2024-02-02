from Log_connection_dir.LogConnection import logging
from Components.support_module import stopword_function
from Components.support_module import pos_neg_words
from Variable_artifact_dir.Artifact import POSITIVE_WORDS
from Variable_artifact_dir.Artifact import NEGATIVE_WORDS
from Variable_artifact_dir.Artifact import SENTIMENT_ANALYSIS_CSV_PATH
import pandas as pd
import pyphen
import re
import string
import nltk

# calling pos_neg function
negative_words = pos_neg_words(NEGATIVE_WORDS)
positive_words = pos_neg_words(POSITIVE_WORDS)

NLTK_PATH = 'punkt/english.pickle'  # Relative path
nltk.data.path.append('.')  # code is in the same directory as 'punkt'

# Initialize Pyphen
diction = pyphen.Pyphen(lang='en_US')

# Load the 'punkt' data directly using nltk.data.load()
tokenizer = nltk.data.load(NLTK_PATH)


class Ingestion:
    def __init__(self,
                 extracted_csv_path,
                 st_words_path,
                 ):
        self.extracted_csv = extracted_csv_path
        self.stop_words = stopword_function(stopwordpath=st_words_path)

    def dataframe_function(self):
        try:
            logging.info(f"reading extracted file")
            frame = pd.read_csv(self.extracted_csv)
            # file = frame.dropna().reset_index(drop=True)  # invoke if wants to remove all null entry [i.e. site 404 error site not reached]
            logging.info(f"extracted file invoked : successful")
            return frame

        except Exception as e:
            logging.info(f"error in dataframe_function ingestion module of component dir: {e}")

    def clean_text(self):
        try:
            logging.info(f"initializing clean_text function")
            extracted_file = self.dataframe_function()
            stopwords = self.stop_words
            filtered_sent = []
            filtered_token = []
            abbreviation_dict = {
                'US': 'United States',
                'BTW': 'between'
            }
            logging.info(f"stopwords function called as list")
            for texts in extracted_file['URL_DATA'].apply(str):
                filtered_words = []  # list for filtered words in lowercase

                # Replace full forms with abbreviations
                for key, value in abbreviation_dict.items():
                    if key in texts:
                        texts = texts.replace(key, value)

                # removing punctuations
                for punctuations in string.punctuation:
                    if punctuations in texts:
                        texts = texts.replace(punctuations, '')

                # splitting the words
                article_total_words = texts.split()
                for word in article_total_words:
                    lowercase_word = word.lower()
                    if lowercase_word not in stopwords:
                        filtered_words.append(lowercase_word)
                        filtered_token.append(filtered_words)
                filtered_text = ' '.join(filtered_words)
                filtered_sent.append(filtered_text)
            logging.info(f"filtered text and sentence :successful")
            return filtered_sent, filtered_token

        except Exception as e:
            logging.info(f"error in clean_text ingestion module of component dir: {e}")

    def sentiment_analysis(self):
        try:
            logging.info(f"initializing sentiment_analysis function")
            filtered_sent, filtered_token = self.clean_text()
            extracted_file = self.dataframe_function()

            positive_score = []
            negative_score = []
            polarity_score = []
            subjectivity_score = []
            neutral_score = []
            avg_words_per_sentence = []
            complex_word_percent = []
            fog_index = []
            avg_number_words = []
            complex_word_count = []
            word_count = []
            syllable_count = []
            average_word_length = []
            total_personal_pronoun = []

            for text in filtered_sent:

                total_character_count = 0
                counter_pos = 0
                counter_neg = 0
                syllable_words = []
                exception_syllable_words = []
                split_sentence_total_words = []

                sentences = tokenizer.tokenize(text)
                for sentence in sentences:
                    split_sent = sentence.split()
                    split_sentence_total_words.append(len(split_sent))

                    for words in split_sent:
                        if words in positive_words:
                            counter_pos = counter_pos + 1

                        if words in negative_words:
                            counter_neg = counter_neg + 1

                        sylla_word = (diction.inserted(words).split('-'))  # Count syllables in a word
                        sylla_len = len(sylla_word)
                        if sylla_len > 2:
                            syllable_words.append(words)

                        if sylla_word:
                            if not (words.endswith("es") or words.endswith("ed")):  # handle syllable with exception
                                exception_syllable_words.append(words)

                pol_score = (counter_pos - counter_neg) / ((counter_pos + counter_neg) + 0.000001)  # Range is from -1 to +1
                sub_score = (counter_pos + counter_neg) / (len(filtered_token) + 0.000001)  # Range is from 0 to +1
                neut_score = 1 - abs(pol_score)

                # readability analysis
                avg_words_per_sent = sum(split_sentence_total_words) / len(sentences)
                total_complex_word = (len(syllable_words)) / sum(split_sentence_total_words)
                fog_idx = 0.4 * (total_complex_word + avg_words_per_sent)

                # appending positive score and negative score parameters
                positive_score.append(counter_pos)
                negative_score.append(counter_neg)
                polarity_score.append(pol_score)
                subjectivity_score.append(sub_score)
                neutral_score.append(neut_score)
                avg_words_per_sentence.append(avg_words_per_sent)
                complex_word_percent.append(total_complex_word)
                fog_index.append(fog_idx)
                avg_number_words.append(avg_words_per_sent)
                complex_word_count.append(len(syllable_words))
                word_count.append(sum(split_sentence_total_words))
                syllable_count.append(len(exception_syllable_words))

                # total number of words in article
                article_total_words = text.split()
                article_total_words = len(article_total_words)
                for letter in text:
                    total_character_count += len(letter)

                article_avg_word_len = total_character_count / article_total_words
                average_word_length.append(article_avg_word_len)

                # Define a regular expression pattern for personal pronouns
                pronoun_pattern = re.compile(r'\b(?:I|we|my|ours|us)\b', re.IGNORECASE)

                # Find all matches in the text
                matches = pronoun_pattern.findall(text)
                total_personal_pronoun.append(len(matches))

            result = {
                "POSITIVE SCORE": positive_score,
                "NEGATIVE SCORE": negative_score,
                "POLARITY SCORE": polarity_score,
                "SUBJECTIVITY SCORE": subjectivity_score,
                "AVG SENTENCE LENGTH": neutral_score,
                "PERCENTAGE OF COMPLEX WORDS": complex_word_percent,
                "FOG INDEX": fog_index,
                "AVG NUMBER OF WORDS PER SENTENCE": avg_number_words,
                "COMPLEX WORD COUNT": complex_word_count,
                "WORD COUNT": word_count,
                "SYLLABLE PER WORD": syllable_count,
                "PERSONAL PRONOUNS": total_personal_pronoun,
                "AVG WORD LENGTH": average_word_length}

            # Merging data frame to produce with desired columns
            frame_result = pd.DataFrame(result)
            extracted_frame = extracted_file[["URL_ID", "URL"]]
            output_dataframe = pd.concat([extracted_frame, frame_result], axis=1)
            logging.info(f"dataframe merged: successful")

            # saving output dataframe to csv dir
            output_dataframe.to_csv(SENTIMENT_ANALYSIS_CSV_PATH, index=False)

        except Exception as e:
            logging.info(f"error in sentiment_analysis ingestion module of component dir: {e}")
