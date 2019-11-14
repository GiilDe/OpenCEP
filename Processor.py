from PatternQuery import PatternQuery
from typing import List


class EvaluationModel:
    def handle_event(self, event):
        pass

    def set_pattern_query(self, pattern_query: PatternQuery):
        pass


class Event:
    def __init__(self, attribute_names: List[str], values: List):
        self.attributes = dict(zip(attribute_names, values))

    def __getattribute__(self, item):
        return self.attributes[item]


class Processor:
    """
    This class represents the main complex event processor
    """
    def __init__(self, data_file_path: str, attribute_names: List[str]):
        self.data_file_path = data_file_path
        self.attribute_names = attribute_names

    def query(self, pattern_query: PatternQuery, evaluation_model: EvaluationModel) -> List:
        """

        :param pattern_query:
        :param evaluation_model:
        :return:
        """
        def convert_value(value: str):
            def isfloat(val: str):
                try:
                    float(val)
                    return True
                except ValueError:
                    return False

            if str.isdigit(value):
                return int(value)
            if isfloat(value):
                return float(value)
            return value

        evaluation_model.set_pattern_query(pattern_query)
        data_stream = open(self.data_file_path, 'r')
        next_line = data_stream.readline()
        results = []
        while next_line:
            values = next_line.split(',')
            for i, value in enumerate(values):
                values[i] = convert_value(value)
            new_event = Event(self.attribute_names, values)
            res = evaluation_model.handle_event(new_event)
            if res is not None:
                results.append(res)
            next_line = data_stream.readline()

        return results if len(results) != 0 else None