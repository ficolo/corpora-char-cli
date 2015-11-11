#-*- coding: utf-8 -*-
import csv


def export_recommendations_to_csv(dictionary, output):
    with open(output + 'recommendations_stats.csv', 'wb') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, ['id', 'acronym', 'frequency'])
        w.writeheader()
        for key in dictionary:
            w.writerow({'id': dictionary[key]['id'], 'acronym': key, 'frequency': dictionary[key]['frequency']})
