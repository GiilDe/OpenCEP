import ProcessingUtilities
from typing import List
from file_sort import sort_file
import copy

metastock7_attributes, metastock7_time_index = ['symbol', 'date', 'open of the day', 'high of the day',
                                               'low of the day', 'close of the day', 'volumes'], 1


class Processor:
    """
    This class represents the main complex event processor
    """
    sorted_prefix = 'sorted_'

    def get_event_from_line(self, line):
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

        values = line[:-1].split(',')
        for i, value in enumerate(values):
            values[i] = convert_value(value)
        new_event = ProcessingUtilities.Event(self.attribute_names, values, self.time_name)
        return new_event

    def __init__(self, data_file_path: str, attribute_names: List[str], time_attribute_index: int, sorted_by_time=True):
        self.data_file_path = data_file_path
        self.attribute_names = attribute_names
        self.time_name = attribute_names[time_attribute_index]
        if not sorted_by_time:
            self.data_file_path = Processor.sorted_prefix + self.data_file_path
            sort_file(time_attribute_index, data_file_path, self.data_file_path)

    def query(self, pattern_query: ProcessingUtilities.PatternQuery,
              evaluation_model: ProcessingUtilities.EvaluationModel, output_file=None):
        """
        Method to handle
        :param pattern_query:
        :param evaluation_model:
        :return:
        """
        evaluation_model.set_pattern_query(pattern_query)
        data_stream = open(self.data_file_path, 'r')
        for line in data_stream:
            event = self.get_event_from_line(line)
            evaluation_model.handle_event(event)
        data_stream.close()
        results = evaluation_model.get_results()
        if output_file is None:
            return results
        else:
            output = open(output_file, 'w')
            for result in results:
                output.write(str(result))
            output.close()
