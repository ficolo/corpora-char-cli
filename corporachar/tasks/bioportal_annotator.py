#-*- coding: utf-8 -*-
""""""
from corporachar import settings
from corporachar.utils.db_manager import DBConnect
import json
import requests
from os import walk, path
import textract
from joblib import Parallel, delayed
import click
from unidecode import unidecode
import itertools


def init():
    db = DBConnect()
    db.init_model()


def get_recommendations_file(pdf_file_path):
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
        abstract_index = text.find('abstract')
        abstract_index += text.find('ABSTRACT')
        abstract_index += text.find('Abstract')
        abstract_index = 0 if abstract_index < 0 else abstract_index
        text = unidecode(text.decode('utf8'))
        text = ' '.join(text.split())
        text = text[abstract_index:abstract_index+500] if len(text) > 500 else text
        post_data = dict(apikey=settings.BIOPORTAL_API_KEY, input=text, include='ontologies',
                         display_links='false', output_type='2', display_context='false',
                         wc='0.15', ws='1.0', wa='1.0', wd='0.5')
        try:
            response = requests.post(settings.RECOMMENDER_URL, post_data)
            json_results = json.loads(response.text)
            best_ontology_set = json_results[0]['ontologies'] if len(json_results) > 0 else []
            return [{'acronym': ontology['acronym'], 'id': ontology['@id']} for ontology in best_ontology_set]
        except (ValueError, IndexError, KeyError) as e:
            log = {
                'file_name': '',
                'error': 'Bad response from Bioportal Recommender:',
                'exception': str(e),
                'data': ''
            }
            db = DBConnect()
            db.insert_log(log)
            return []


def annotate_doc(pdf_file_path, ontologies):
    if pdf_file_path.endswith('pdf') or pdf_file_path.endswith('PDF'):
        text = textract.process(pdf_file_path, method="pdfminer")
    elif pdf_file_path.endswith('html') or pdf_file_path.endswith('htm'):
        text = textract.process(pdf_file_path, method="beautifulsoup4")
    elif pdf_file_path.endswith('txt'):
            with open(pdf_file_path, 'r') as file:
                text = file.read()
    db = DBConnect()
    if text.isspace():
        log = {
            'file_name': pdf_file_path.encode('utf-8'),
            'error': 'Failed PDF to text transformation in annotation process',
            'exception': '',
            'data': ''
        }
        db.insert_log(log)
        return
    ontologies = ",".join(ontologies)
    annotations = []
    text = unidecode(text.decode('utf8'))
    text = ' '.join(text.split())
    # post_data = dict(apikey=settings.BIOPORTAL_API_KEY, text=text,
    #                  display_links='true', display_context='false', minimum_match_length='3',
    #                  exclude_numbers='true', longest_only='true', ontologies=ontologies, exclude_synonyms='true')
    post_data = dict(apikey=settings.BIOPORTAL_API_KEY, text=text,
                     display_links='true', display_context='false', minimum_match_length='3',
                     exclude_numbers='true', longest_only='true', ontologies=ontologies, exclude_synonyms='true')
    try:
        response = requests.post(settings.ANNOTATOR_URL, post_data)
        json_results = json.loads(response.text)
        for result in json_results:
            for annotation in result['annotations']:
                context_begin = annotation['from']  if annotation['from'] - 40 < 1 else annotation['from'] - 40
                context_end = annotation['to'] if annotation['to'] + 40 > len(text) else annotation['to'] + 40
                record = {
                    'file_name': pdf_file_path.encode('utf-8'),
                    'bio_class_id': result['annotatedClass']['@id'],
                    'bio_ontology_id': result['annotatedClass']['links']['ontology'],
                    'text': u'' + annotation['text'].encode('utf-8'),
                    'match_type': annotation['matchType'],
                    'context': u''+text[context_begin:context_end]
                }
                annotations.append(record)
        db.insert_annotations(annotations)
        return
    except (ValueError, IndexError, KeyError) as e:
        print e
        log = {
            'file_name': pdf_file_path.encode('utf-8'),
            'error': 'Bad response from Bioportal Annotator',
            'exception': str(e),
            'data': ''
        }
        db.insert_log(log)
        return


def annotate_dir(dir_path, ontologies):
    file_names = []
    db = DBConnect()
    for root, dirs, files in walk(dir_path):
        for name in files:
            if 'pdf' in name or 'htm' in name or 'txt' in name:
                file_names.append(path.join(root, name))
            else:
                continue

    click.secho("Getting Bioportal annotations for {} PDF documents with {} ontologies.".format(len(file_names), len(ontologies))
                , fg='blue')
    n_jobs = 20
    dir_annotations = Parallel(n_jobs=n_jobs)(delayed(annotate_doc)(file_name, ontologies) for file_name in file_names)
    return dir_annotations


def get_recommendations_dir(dir_path):
    file_names = []
    ontologies = {}
    db = DBConnect()

    for root, dirs, files in walk(dir_path):
        for name in files:
            if 'pdf' in name or 'htm' in name or 'txt' in name:
                file_names.append(path.join(root, name))
            else:
                continue

    click.secho("Getting Bioportal recommendations for {} PDF documents.".format(len(file_names)), fg='blue')
    n_jobs = 20
    file_recommendations = Parallel(n_jobs=n_jobs)(delayed(get_recommendations_file)(file_name)
                                                        for file_name in file_names)
    for recommendation in file_recommendations:
        for ontology in recommendation:
                if ontology['acronym'] in ontologies:
                    ontologies[ontology['acronym']]['frequency'] += 1
                else:
                    ontologies[ontology['acronym']] = {'frequency': 1, 'id': ontology['id']}
    db.insert_ontologies(ontologies)
    return list(ontologies.keys())
