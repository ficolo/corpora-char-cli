from collections import Counter
import textract
from corporachar.utils.db_manager import DBConnect
from joblib import Parallel, delayed
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from os import walk, path
import click


def word_count(pdf_file_path, dictionary):
    text = textract.process(pdf_file_path, method="pdfminer")
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
    words = word_tokenize(text)
    text = ' '.join([word.upper() for word in words if word.upper() in dictionary])
    c = Counter()
    c.update(text.strip().split(' '))
    return c


def word_count_dir(dir_path):
    file_names = []
    db = DBConnect()
    for root, dirs, files in walk(dir_path):
        for name in files:
            if 'pdf' in name:
                file_names.append(path.join(root, name))
            else:
                continue
    click.secho("Counting words for {} PDF documents.".format(len(file_names))
                , fg='blue')
    n_jobs = 20
    dir_counters = Parallel(n_jobs=n_jobs)(delayed(word_count)(file_name) for file_name in file_names)
    total = sum(dir_counters, Counter())
    print "hi"
    for key in total:
        db.insert_word({'word': key, 'frequency': total[key]})
    return total
