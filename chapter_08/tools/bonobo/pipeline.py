# Import modules
import bonobo
from chapter_08.etl.extract import extract_data
from chapter_08.etl.transform import transform_data
from chapter_08.tools.bonobo.load import load_data

# Define the Bonobo pipeline
def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract_data, transform_data, load_data)
    return graph

# Define the main function to run the Bonobo pipeline
def main():
    # Set the options for the Bonobo pipeline
    options = {
        'services': [],
        'plugins': [],
        'log_level': 'INFO',
        'log_handlers': [bonobo.logging.StreamHandler()],
        'use_colors': True,
        'graph': get_graph()
    }
    # Run the Bonobo pipeline
    bonobo.run(**options)

if __name__ == '__main__':
    main()
