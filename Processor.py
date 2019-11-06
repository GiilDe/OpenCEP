from PatternQuery import PatternQuery
from typing import List


class EvaluationModel:
    def handle_event(self, event):
        pass


class GraphBasedProcessing(EvaluationModel):
    """
    This class receives a graph that represents the pattern the user is looking for.
    It iterates the events one by one and tries to build the graph that it received.
    It saves its partial graphs explicitly in memory.
    """
    def __init__(self, graph_initializer, pattern_query: PatternQuery):
        """
        Parameters
        ----------
        graph_initializer: receives PatternQuery and returns a graph that represents the pattern query
        """

    def handle_event(self, event):


class Processor:
    """

    """
    def __init__(self, data_file_path: str, ):


    def query(self, pattern_query: PatternQuery, evaluation_model: EvaluationModel) -> List:
