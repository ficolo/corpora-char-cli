from collections import Counter
import textract
from corporachar.utils.db_manager import DBConnect
from joblib import Parallel, delayed
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from os import walk, path
import click


def word_count(pdf_file_path):
    if pdf_file_path.endswith('pdf') or pdf_file_path.endswith('PDF'):
        text = textract.process(pdf_file_path, method="pdfminer")
    elif pdf_file_path.endswith('html') or pdf_file_path.endswith('htm'):
        text = textract.process(pdf_file_path, method="beautifulsoup4")
    elif pdf_file_path.endswith('txt'):
        with open(pdf_file_path, 'r') as file:
            text = file.read()
    if text.isspace():
            log = {
                'file_name': pdf_file_path.encode('utf-8'),
                'error': 'Failed PDF to text transformation in recommendation process',
                'exception': '',
                'data': ''
            }
            db = DBConnect()
            db.insert_log(log)
            return []
    text = unicode(text, 'utf-8')
    words = word_tokenize(text.upper())
    c = Counter()
    c.update(words)
    return c


def word_count_dir(dir_path):
    file_names = []
    db = DBConnect()
    dictionary = db.select_tags()
    for root, dirs, files in walk(dir_path):
        for name in files:
            if 'pdf' in name or 'htm' in name or 'txt' in name:
                file_names.append(path.join(root, name))
            else:
                continue
    click.secho("Counting words for {} PDF documents.".format(len(file_names))
                , fg='blue')
    n_jobs = 20
    dir_counters = Parallel(n_jobs=n_jobs)(delayed(word_count)(file_name) for file_name in file_names)
    total = sum(dir_counters, Counter())
    for word in dictionary:
        if word in total:
            db.insert_word({'word': word, 'frequency': total[word]})
    return total
