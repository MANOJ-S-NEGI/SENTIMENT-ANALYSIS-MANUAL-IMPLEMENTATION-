# Importing libraries
import requests
from bs4 import BeautifulSoup
import numpy as np
from Log_connection_dir.LogConnection import logging


class DataExtraction:
    def __init__(self,
                 content_class,
                 title_class,
                 title_class_alternate,
                 content_class_alternate,
                 url):

        self.content_class = content_class
        self.title_class = title_class
        self.title_class_alternate = title_class_alternate
        self.content_class_alternate = content_class_alternate
        self.url = url

    def Data_extract(self):
        try:
            article_title = []
            article_content = []
            response = requests.get(self.url)

            # Checking if the request was successful (status code 200)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text,
                                     'html.parser')
                title = soup.find("h1",
                                  class_=self.title_class)  # Extract article title

                # If not found, try the title_class_2
                if not title:
                    title = soup.find('h1',
                                      class_=self.title_class_alternate)  # Replace with the actual class of the alternative div
                if title:
                    title = title.get_text(separator=' ', strip=True)
                    article_title.append(title)
                else:
                    article_title.append(np.nan)

                # HTML element to extract content from div class_1
                data = soup.find("div",
                                 class_=self.content_class)

                # If not found, try the second div
                if not data:
                    data = soup.find('div',
                                     class_=self.content_class_alternate)  # Replace with the actual class of the alternative div

                # If data found: Find and remove the <pre-class='wp-block-preformatted'> from data
                if data:
                    pre_tag = data.find('pre',
                                        class_='wp-block-preformatted')
                    if pre_tag:  # if it exists
                        pre_tag.decompose()

                    figure_tags = data.find_all('figure')  # Find all <figure> tags
                    for figure_tag in figure_tags:
                        figure_tag.decompose()  # Remove each <figure> tag

                    # Print only the text content separated by space and removing leading and trailing spaces from data div
                    modified_text = data.get_text(separator=' ',
                                                  strip=True)
                    article_content.append(modified_text)
                    logging.info(f"Data extraction successful for link<{self.url}>")
                else:
                    article_content.append(np.nan)
            else:
                article_title.append(np.nan)
                article_content.append(np.nan)
                logging.info(f"Failed to retrieve the webpage for URL: {self.url}")
            return article_title, article_content

        except requests.exceptions.RequestException as e:
            logging.info(f"error UrlRequest Module: {e}")
