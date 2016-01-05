# corpora-char-cli
Command line interface for document corpora analysis and characterization

##Installation

```{r, engine='bash', count_lines}
git clone https://github.com/ficolo/corpora-char-cli.git
pip install -r requirements.txt
```


##Running the Command Line Interface
Before running the python script you have to create a local.py file (under ./settings) adding a valid Bioportal API_KEY and the database configuration.
```{r, engine='bash', count_lines}
Usage: corporachar.py [OPTIONS]

Options:
  --input TEXT           Path to the corpus directory. Default = ./
  --no_recommender TEXT  Flag to not use the BioPortal recommender. Default = False
  --use_umls TEXT        Flag to use only the UMLS ontologies. Default = False
  --help                 Show this message and exit.
  ```

