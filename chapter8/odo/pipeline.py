# create a command-line runnable pipeline
from extract import extract_data
from transform import transform_data
import load

import yaml

# import pipeline configuration
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

import bonobo


def extract():
    # Your code for extracting data goes here

    yield data


def transform(data):
    # Your code for transforming data goes here

    yield transformed_data


def load(transformed_data):
    # Your code for loading data goes here

    pass


def get_graph(**options):
    graph = bonobo.Graph()

    graph.add_chain(extract, transform, load)

    return graph


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options))