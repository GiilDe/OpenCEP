import ProcessingUtilities
import typing
from file_sort import sort_file


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
        new_event = ProcessingUtilities.Event(self.attribute_names, values, self.time_name, self.type_name)
        return new_event

    def __init__(self, data_file_path: str, attribute_names: typing.List[str], time_attribute_index: int,
                 type_attribute_index: int, sorted_by_time=True):
        self.data_file_path = data_file_path
        self.attribute_names = attribute_names
        self.time_name = attribute_names[time_attribute_index]
        self.type_name = attribute_names[type_attribute_index]
        if not sorted_by_time:
            self.data_file_path = Processor.sorted_prefix + self.data_file_path
            sort_file(time_attribute_index, data_file_path, self.data_file_path)

    def query(self, pattern_queries: typing.List[ProcessingUtilities.PatternQuery],
              evaluation_model: ProcessingUtilities.EvaluationModel,
              input_interface: ProcessingUtilities.InputInterface = ProcessingUtilities.TrivialInputInterface(),
              output_interface: ProcessingUtilities.OutputInterface = ProcessingUtilities.TrivialOutputInterface()):
        """

        :param pattern_queries:
        :param evaluation_model:
        :param input_interface:
        :param output_interface:
        :return:
        """
        clean_pattern_queries = input_interface.get_clean_pattern_queries(pattern_queries)
        evaluation_model.set_pattern_queries(clean_pattern_queries)
        data_stream = open(self.data_file_path, 'r')
        for line in data_stream:
            event = self.get_event_from_line(line)
            evaluation_model.handle_event(event)
        data_stream.close()
        results = evaluation_model.get_results()
        return output_interface.output_results(results)
