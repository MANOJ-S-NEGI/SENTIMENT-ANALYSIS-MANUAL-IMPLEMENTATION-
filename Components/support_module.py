import os


def stopword_function(stopwordpath):
    stop_words = []
    files = os.listdir(stopwordpath)
    for file in files:
        filename = os.path.join(stopwordpath, file)  # Join the directory path with the filename
        with open(filename, 'r', encoding='latin-1') as words:
            for word in words.readlines():
                strip_word = word.strip()
                stop_words.append(strip_word)
    return stop_words


# Step 1.2: Creating a dictionary of Positive and Negative words
def pos_neg_words(pn_filepath):
    words = []
    with open(pn_filepath, 'r', encoding='latin-1') as file:
        for f in file.readlines():
            word = f.strip()
            words.append(word)
    return words

