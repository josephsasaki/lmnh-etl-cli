'''Module from which entire pipeline is run'''

from argparse import ArgumentParser
from models.pipeline import Pipeline


def get_args():
    '''Get the arguments passed into the CLI'''
    parser = ArgumentParser()
    parser.add_argument("-l", action="store_true")
    retrieved_args = parser.parse_args()
    return retrieved_args


if __name__ == "__main__":
    args = get_args()
    pipeline = Pipeline(args)
    pipeline.run()
