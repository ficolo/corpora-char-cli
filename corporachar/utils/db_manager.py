#-*- coding: utf-8 -*-

import pymysql
import sys
from corporachar import settings

CREATE_PROCESS_LOG = """
    CREATE TABLE IF NOT EXISTS Process_Log(id_log MEDIUMINT NOT NULL AUTO_INCREMENT, file_name TEXT,
     error TEXT, exception TEXT, data TEXT, PRIMARY KEY (id_log))
    """
CREATE_ANNOTATIONS_STMT = """
    CREATE TABLE IF NOT EXISTS Annotations(id_annotation MEDIUMINT NOT NULL AUTO_INCREMENT, file_name TEXT,
    bio_class_id TEXT, bio_ontology_id TEXT, text TEXT, match_type TEXT, context TEXT, frequency INT, PRIMARY KEY (id_annotation))
    """
CREATE_ONTOLOGIES_STMT = """
    CREATE TABLE IF NOT EXISTS Ontologies(id_ontology MEDIUMINT NOT NULL AUTO_INCREMENT, bio_ontology_id TEXT,
    bio_ontology_acronym TEXT, frequency INT, PRIMARY KEY (id_ontology))
    """
CREATE_WORDS_STMT = """
    CREATE TABLE IF NOT EXISTS Words(id_word MEDIUMINT NOT NULL AUTO_INCREMENT, word TEXT,
    frequency INT, PRIMARY KEY (id_word))
"""
INSERT_LOG = """
    INSERT INTO Process_Log(file_name, error, exception, data) VALUES ('{}', '{}', '{}', "{}")
    """
INSERT_ONTOLOGY = """
    INSERT INTO Ontologies(bio_ontology_id, bio_ontology_acronym, frequency) VALUES ('{}','{}', {})
    """
INSERT_ANNOTATION = """
    INSERT INTO Annotations(file_name, bio_class_id, bio_ontology_id, text, match_type, frequency)
    VALUES ('{}', '{}', '{}', "{}", "{}", '{}')
    """
INSERT_WORD = """
    INSERT INTO Words(word, frequency)
    VALUES ('{}', '{}')
    """


class DBConnect:
    con = None

    def __init__(self):
        self.con = pymysql.connect(
            host=settings.DB_HOST,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            db=settings.DB_NAME,
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor)

    def init_model(self):
        try:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS Process_Log")
            cur.execute("DROP TABLE IF EXISTS Annotations")
            cur.execute("DROP TABLE IF EXISTS Ontologies")
            cur.execute("DROP TABLE IF EXISTS Words")
            cur.execute(CREATE_PROCESS_LOG)
            cur.execute(CREATE_ANNOTATIONS_STMT)
            cur.execute(CREATE_ONTOLOGIES_STMT)
            cur.execute(CREATE_WORDS_STMT)
        except pymysql.Error as e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)

    def insert_ontologies(self, ontologies):
        try:
            cur = self.con.cursor()
            for acronym in ontologies:
                insert = INSERT_ONTOLOGY.format(ontologies[acronym]['id'], acronym, ontologies[acronym]['frequency'])
                cur.execute(insert)
            self.con.commit()
        except pymysql.Error as e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)

    def select_ontologies(self):
        try:
            cur = self.con.cursor()
            select = "SELECT bio_ontology_acronym FROM Ontologies"
            cur.execute(select)
            rows = cur.fetchall()
            ontologies =[]
            for row in rows:
                ontologies.append(row['bio_ontology_acronym'])
            return ontologies
        except pymysql.Error, e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)

    def insert_annotations(self, annotations):
        try:
            cur = self.con.cursor()
            for class_id in annotations:
                insert = INSERT_ANNOTATION.format(
                    annotations[class_id]['file_name'],
                    annotations[class_id]['bio_class_id'].replace("'", '%27'),
                    annotations[class_id]['bio_ontology_id'],
                    self.con.escape_string(annotations[class_id]['text'].replace("'", '`')),
                    annotations[class_id]['match_type'],
                    annotations[class_id]['context'],
                    annotations[class_id]['frequency'])
                cur.execute(insert)
            self.con.commit()
        except (pymysql.Error, UnicodeEncodeError) as e:
            print "Error %s:" % e.args[0]
            print e
            print insert
            sys.exit(1)

    def insert_log(self, log):
        try:
            cur = self.con.cursor()
            insert = INSERT_LOG.format(log['file_name'], log['error'], log['exception'], log['data'])
            cur.execute(insert)
            self.con.commit()
        except (pymysql.Error, UnicodeEncodeError) as e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)

    def insert_word(self, word):
        try:
            cur = self.con.cursor()
            insert = INSERT_WORD.format(word['word'], word['frequency'])
            cur.execute(insert)
            self.con.commit()
        except (pymysql.Error, UnicodeEncodeError) as e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)