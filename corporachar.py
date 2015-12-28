import click
import time
from corporachar.tasks import bioportal_annotator as annotator
from corporachar.tasks import text_mining as minner


WELCOME_MSJ = "Corpora Char v0.0: Command line interface to characterize document copora."


@click.command()
@click.option('--input', default='./', help='Path to the corpus directory. Default = ./')
@click.option('--output', default='./', help='Path to the output directory. Default = ./')
def cli(input, output):
    click.secho(WELCOME_MSJ, fg='yellow', bold=True)
    start_time = time.time()
    input = input if input.endswith('/') else input + '/'
    output = output if output.endswith('/') else output + '/'
    annotator.init()
    annotator.get_recommendations_dir(input)
    annotator.annotate_dir(input)
    #minner.word_count_dir(input, ['DNA'])
    elapsed_time = time.time() - start_time
    click.secho("Elapsed time {}.".format(elapsed_time), fg='green')
    
if __name__ == '__main__':
    cli()
