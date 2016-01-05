#-*- coding: utf-8 -*-

import pymysql
import sys
from corporachar import settings
import time


CREATE_FILES_STMT = """
    CREATE TABLE IF NOT EXISTS Files(id_file MEDIUMINT NOT NULL AUTO_INCREMENT, file_name VARCHAR(255),
    UNIQUE (file_name),  PRIMARY KEY (id_file))
    """
CREATE_CONCEPTS_STMT = """
    CREATE TABLE IF NOT EXISTS Concepts(id_concept MEDIUMINT NOT NULL AUTO_INCREMENT, bio_class_id VARCHAR(255),
    UNIQUE (bio_class_id) ,  PRIMARY KEY (id_concept))
    """
CREATE_ONTOLOGIES_STMT = """
    CREATE TABLE IF NOT EXISTS Ontologies(id_ontology MEDIUMINT NOT NULL AUTO_INCREMENT, bio_ontology_id VARCHAR(255),
    bio_ontology_acronym VARCHAR(255),  UNIQUE (bio_ontology_id, bio_ontology_acronym), PRIMARY KEY (id_ontology))
    """
CREATE_WORDS_STMT = """
    CREATE TABLE IF NOT EXISTS Words(id_word MEDIUMINT NOT NULL AUTO_INCREMENT, word VARCHAR(255),
    UNIQUE (word), PRIMARY KEY (id_word))
    """
CREATE_PROCESS_LOG = """
    CREATE TABLE IF NOT EXISTS Process_Log(id_log MEDIUMINT NOT NULL AUTO_INCREMENT, file_name VARCHAR(255),
     error TEXT, exception TEXT, data TEXT, PRIMARY KEY (id_log))
    """
CREATE_MATCH_TYPES_STMT = """
    CREATE TABLE IF NOT EXISTS MatchTypes(id_match_type MEDIUMINT NOT NULL AUTO_INCREMENT, match_type VARCHAR(255),
    PRIMARY KEY (id_match_type), UNIQUE(match_type))
    """
CREATE_CONTEXTS_STMT = """
    CREATE TABLE IF NOT EXISTS Contexts(id_context MEDIUMINT NOT NULL AUTO_INCREMENT, context VARCHAR(255), PRIMARY KEY (id_context))
    """
CREATE_ANNOTATIONS_STMT = """
    CREATE TABLE IF NOT EXISTS Annotations(id_annotation MEDIUMINT NOT NULL AUTO_INCREMENT, id_file MEDIUMINT,
    id_concept MEDIUMINT, id_ontology MEDIUMINT, id_word MEDIUMINT, id_match_type MEDIUMINT, id_context MEDIUMINT, PRIMARY KEY (id_annotation),
    FOREIGN KEY (id_file)
        REFERENCES Files(id_file)
        ON DELETE CASCADE,
    FOREIGN KEY (id_concept)
        REFERENCES Concepts(id_concept)
        ON DELETE CASCADE,
    FOREIGN KEY (id_ontology)
        REFERENCES Ontologies(id_ontology)
        ON DELETE CASCADE,
    FOREIGN KEY (id_word)
        REFERENCES Words(id_word)
        ON DELETE CASCADE,
    FOREIGN KEY (id_match_type)
        REFERENCES MatchTypes(id_match_type)
        ON DELETE CASCADE,
    FOREIGN KEY (id_context)
        REFERENCES Contexts(id_context)
        ON DELETE CASCADE)
    """
CREATE_ONTOLOGIES_FQ_STMT = """
    CREATE TABLE IF NOT EXISTS Ontologies_fq(id_ontology MEDIUMINT, frequency INT, FOREIGN KEY (id_ontology)
        REFERENCES Ontologies(id_ontology)
        ON DELETE CASCADE)
    """
CREATE_WORDS_FQ_STMT = """
    CREATE TABLE IF NOT EXISTS Words_fq(id_word MEDIUMINT, frequency INT, FOREIGN KEY (id_word)
        REFERENCES Words(id_word)
        ON DELETE CASCADE)
"""
INSERT_LOG = """
    INSERT INTO Process_Log(file_name, error, exception, data) VALUES ('{}', '{}', "{}", "{}")
    """
INSERT_ONTOLOGY = """
    INSERT INTO Ontologies(bio_ontology_id, bio_ontology_acronym) VALUES ('{}', '{}')
    ON DUPLICATE KEY UPDATE id_ontology = LAST_INSERT_ID(id_ontology)
    """
INSERT_ONTOLOGY_FQ = """
    INSERT INTO Ontologies_fq(id_ontology, frequency) VALUES ({}, {})
    """
INSERT_FILE = """
    INSERT INTO Files(file_name) VALUES ('{}') ON DUPLICATE KEY UPDATE id_file = LAST_INSERT_ID(id_file)
"""
INSERT_CONCEPT = """
    INSERT INTO Concepts(bio_class_id) VALUES ('{}') ON DUPLICATE KEY UPDATE id_concept = LAST_INSERT_ID(id_concept)
"""
INSERT_WORD = """
    INSERT INTO Words(word) VALUES ('{}') ON DUPLICATE KEY UPDATE id_word = LAST_INSERT_ID(id_word)
"""
INSERT_WORD_FQ = """
    INSERT INTO Words_fq(id_word, frequency) VALUES ({}, {})
"""
INSERT_MATCHTYPE = """
    INSERT INTO MatchTypes(match_type) VALUES ('{}') ON DUPLICATE KEY UPDATE id_match_type = LAST_INSERT_ID(id_match_type)
"""
INSERT_CONTEXT = """
    INSERT INTO Contexts(context) VALUES ('{}') ON DUPLICATE KEY UPDATE id_context = LAST_INSERT_ID(id_context)
"""
INSERT_ANNOTATION = """
    INSERT INTO Annotations(id_file, id_concept, id_ontology, id_word, id_match_type, id_context)
    VALUES ({}, {}, {}, {}, {}, {})
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
            cur.execute("DROP TABLE IF EXISTS Annotations")
            cur.execute("DROP TABLE IF EXISTS Ontologies_fq")
            cur.execute("DROP TABLE IF EXISTS Words_fq")
            cur.execute("DROP TABLE IF EXISTS Process_Log")
            cur.execute("DROP TABLE IF EXISTS Ontologies")
            cur.execute("DROP TABLE IF EXISTS Words")
            cur.execute("DROP TABLE IF EXISTS Files")
            cur.execute("DROP TABLE IF EXISTS Concepts")
            cur.execute("DROP TABLE IF EXISTS MatchTypes")
            cur.execute("DROP TABLE IF EXISTS Contexts")
            cur.execute(CREATE_PROCESS_LOG)
            cur.execute(CREATE_ONTOLOGIES_STMT)
            cur.execute(CREATE_ONTOLOGIES_FQ_STMT)
            cur.execute(CREATE_FILES_STMT)
            cur.execute(CREATE_WORDS_STMT)
            cur.execute(CREATE_WORDS_FQ_STMT)
            cur.execute(CREATE_CONTEXTS_STMT)
            cur.execute(CREATE_CONCEPTS_STMT)
            cur.execute(CREATE_MATCH_TYPES_STMT)
            cur.execute(CREATE_ANNOTATIONS_STMT)
        except pymysql.Error as e:
            print "Error %s:" % e.args[0]
            print e
            print CREATE_ANNOTATIONS_STMT
            sys.exit(1)

    def insert_ontologies(self, ontologies):
        try:
            cur = self.con.cursor()
            for acronym in ontologies:
                insert_onto = INSERT_ONTOLOGY.format(ontologies[acronym]['id'], acronym)
                cur.execute(insert_onto)
                id_onto = cur.lastrowid
                insert_fq = INSERT_ONTOLOGY_FQ.format(id_onto, ontologies[acronym]['frequency'])
                cur.execute(insert_fq)
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
            for annotation in annotations:
                insert_file = INSERT_FILE.format(annotation['file_name'])
                cur.execute(insert_file)
                file_id = cur.lastrowid

                insert_concept = INSERT_CONCEPT.format(annotation['bio_class_id'].replace("'", '%27'))
                cur.execute(insert_concept)
                concept_id = cur.lastrowid

                begin_acrom = annotation['bio_ontology_id'].rfind('/') + 1
                acrom = annotation['bio_ontology_id'][begin_acrom:]
                insert_ontology = INSERT_ONTOLOGY.format(annotation['bio_ontology_id'], acrom)
                cur.execute(insert_ontology)
                ontology_id = cur.lastrowid

                insert_word = INSERT_WORD.format(self.con.escape_string(annotation['text'].replace("'", '`')))
                cur.execute(insert_word)
                word_id = cur.lastrowid

                insert_match = INSERT_MATCHTYPE.format(annotation['match_type'])
                cur.execute(insert_match)
                match_id = cur.lastrowid

                insert_context = INSERT_CONTEXT.format(self.con.escape_string(annotation['context'].replace("'", '`')))
                cur.execute(insert_context)
                context_id = cur.lastrowid

                insert = INSERT_ANNOTATION.format(
                    file_id,
                    concept_id,
                    ontology_id,
                    word_id,
                    match_id,
                    context_id)
                cur.execute(insert)
            self.con.commit()
        except (pymysql.Error, UnicodeEncodeError) as e:
            if e.args[0] == 1213 or e.args[0] == 1205:
                self.con.rollback()
                time.sleep(0.02)
                self.insert_annotations(annotations)
            else:
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
            print insert
            sys.exit(1)

    def insert_word(self, word):
        try:
            cur = self.con.cursor()
            insert_word = INSERT_WORD.format(word['word'])
            cur.execute(insert_word)
            word_id = cur.lastrowid
            insert = INSERT_WORD_FQ.format(word_id, word['frequency'])
            cur.execute(insert)
            self.con.commit()
        except (pymysql.Error, UnicodeEncodeError) as e:
            print "Error %s:" % e.args[0]
            print e
            print insert
            sys.exit(1)

    def select_tags(self):
        try:
            cur = self.con.cursor()
            select = "SELECT word FROM Words"
            cur.execute(select)
            rows = cur.fetchall()
            tags =[]
            for row in rows:
                tags.append(row['word'])
            return tags
        except pymysql.Error, e:
            print "Error %s:" % e.args[0]
            print e
            sys.exit(1)