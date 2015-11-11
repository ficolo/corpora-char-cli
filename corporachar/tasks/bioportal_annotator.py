#-*- coding: utf-8 -*-
""""""
from corporachar import settings
from corporachar.utils.db_manager import DBConnect
import json
import requests
from os import walk, path
import textract
from joblib import Parallel, delayed
import multiprocessing
import click


num_cores = multiprocessing.cpu_count()


def init():
    db = DBConnect()
    db.init_model()


def get_recommendations_file(pdf_file_path):
        text = textract.process(pdf_file_path, method="pdfminer")
        if text.isspace() or '(cid:' in text:
            log = {
                'file_name': pdf_file_path.encode('utf-8'),
                'error': 'Failed PDF to text transformation in recommendation process',
                'exception': ''
            }
            db = DBConnect()
            db.insert_log(log)
            return []
        abstract_index = text.find('abstract')
        abstract_index += text.find('ABSTRACT')
        abstract_index += text.find('Abstract')
        abstract_index = 0 if abstract_index < 0 else abstract_index
        text = text[abstract_index:abstract_index+500] if len(text) > 500 else text
        post_data = dict(apikey=settings.BIOPORTAL_API_KEY, input=text, include='ontologies',
                         display_links='false', output_type='2', display_context='false')
        try:
            response = requests.post(settings.RECOMMENDER_URL, post_data)
            json_results = json.loads(response.text)
            best_ontology_set = json_results[0]['ontologies']
            return [{'acronym': ontology['acronym'], 'id': ontology['@id']} for ontology in best_ontology_set]
        except (ValueError, IndexError, KeyError) as e:
            log = {
                'file_name': pdf_file_path.encode('utf-8'),
                'error': 'Bad response from Bioportal Recommender:{}'.format(response.text),
                'exception': str(e)
            }
            db = DBConnect()
            db.insert_log(log)
            return []


def annotate_doc(pdf_file_path, ontologies):
    text = textract.process(pdf_file_path, method="pdfminer")
    db = DBConnect()
    if text.isspace() or '(cid:' in text:
        log = {
            'file_name': pdf_file_path.encode('utf-8'),
            'error': 'Failed PDF to text transformation in annotation process',
            'exception': ''
        }
        db.insert_log(log)
        return
    ontologies = ",".join(ontologies)
    annotations = {}
    post_data = dict(apikey=settings.BIOPORTAL_API_KEY, text=text,
                     display_links='true', display_context='false', minimum_match_length='3',
                     exclude_numbers='true', longest_only='true', ontologies=ontologies, exclude_synonyms='true')
    try:
        response = requests.post(settings.ANNOTATOR_URL, post_data)
        json_results = json.loads(response.text)
        for result in json_results:
            for annotation in result['annotations']:
                if result['annotatedClass']['@id'] in annotations:
                    annotations[result['annotatedClass']['@id']]['frequency'] += 1
                else:
                    record = {
                        'file_name': pdf_file_path.encode('utf-8'),
                        'bio_class_id': result['annotatedClass']['@id'],
                        'bio_ontology_id': result['annotatedClass']['links']['ontology'],
                        'text': annotation['text'].encode('utf-8'),
                        'match_type': annotation['matchType'],
                        'frequency': 1
                    }
                    annotations[result['annotatedClass']['@id']] = record
        db.insert_annotations(annotations)
    except (ValueError, IndexError, KeyError) as e:
        log = {
            'file_name': pdf_file_path.encode('utf-8'),
            'error': 'Bad response from Bioportal Annotator',
            'exception': str(e)}
        db.insert_log(log)
        return


def annotate_dir(dir_path):
    file_names = []
    db = DBConnect()
    ontologies = db.select_ontologies()

    for root, dirs, files in walk(dir_path):
        for name in files:
            if 'pdf' in name:
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
            if 'pdf' in name:
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
