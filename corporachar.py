#-*- coding: utf-8 -*-
import click
import time
from corporachar.tasks import bioportal_annotator as annotator
from corporachar.tasks import text_mining as minner
from corporachar.settings import local


WELCOME_MSJ = "Corpora Char v0.0: Command line interface to characterize document copora."


@click.command()
@click.option('--input', default='./', help='Path to the corpus directory. Default = ./')
@click.option('--output', default='./', help='Path to the output directory. Default = ./')
@click.option('--no_recommender', default=False, help='Flag to not use the BioPortal recommender. Default = False')
@click.option('--use_umls', default=False, help='Flag to use only the UMLS ontologies. Default = False')
def cli(input, output, no_recommender, use_umls):
    click.secho(WELCOME_MSJ, fg='yellow', bold=True)
    start_time = time.time()
    input = input if input.endswith('/') else input + '/'
    output = output if output.endswith('/') else output + '/'
    annotator.init()
    ontologies = []
    if not no_recommender and not use_umls:
        ontologies = annotator.get_recommendations_dir(input)
    elif use_umls:
        ontologies = local.UMLS
    annotator.annotate_dir(input, ontologies)
    minner.word_count_dir(input)
    elapsed_time = time.time() - start_time
    click.secho("Elapsed time {}.".format(elapsed_time), fg='green')
    
if __name__ == '__main__':
    cli()
