from . import processing_utilities
import typing
from .file_sort import sort_file


class Processor:
    """
    This class represents the main complex event processor
    """
    sorted_prefix = 'sorted_'

    def __init__(self, data_file_path: str, attribute_names: typing.List[str], time_attribute_index: int,
                 type_attribute_index: int, sorted_by_time=True):
        """
        initializes all the needed parameters and sorts the input file according to time (if needed)
        :param data_file_path: input file path
        :param attribute_names: attribute names of the events in the data file
        :param time_attribute_index: the index of the time attribute in attribute_names
        :param type_attribute_index: the index of the event type (name) attribute in attribute_names
        :param sorted_by_time: whether or not the input file is sorted by the time attribute
        """
        self.data_file_path = data_file_path
        self.attribute_names = attribute_names
        self.time_name = attribute_names[time_attribute_index]
        self.type_name = attribute_names[type_attribute_index]
        if not sorted_by_time:
            self.data_file_path = Processor.sorted_prefix + self.data_file_path
            sort_file(time_attribute_index, data_file_path, self.data_file_path)

    def get_event_from_line(self, line):
        """
        parses a line from the event input file into an event class
        :param line: the line from the input file representing to current event
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
        values = line[:-1].split(',')
        for i, value in enumerate(values):
            values[i] = convert_value(value)
        new_event = processing_utilities.Event(self.attribute_names, values, self.time_name, self.type_name)
        return new_event

    def query(self, pattern_queries: typing.List[processing_utilities.PatternQuery],
              evaluation_model: processing_utilities.EvaluationModel,
              input_interface: processing_utilities.InputInterface = processing_utilities.TrivialInputInterface(),
              output_interfaces: typing.List[processing_utilities.OutputInterface]=None):
        """
        creates the evaluation model based on the give queries and the corresponding output interfaces, and parses event
        lines from the event files and passes them as event objects to the evaluation model
        :param pattern_queries: the pattern queries to query by
        :param evaluation_model: the evaluation model to use
        :param input_interface:
        :param output_interfaces:
        :return:
        """
        if output_interfaces is None:
            output_interfaces = [processing_utilities.TrivialOutputInterface()] * len((pattern_queries))
        clean_pattern_queries = input_interface.get_clean_pattern_queries(pattern_queries)
        evaluation_model.set_pattern_queries(clean_pattern_queries, output_interfaces)
        data_stream = open(self.data_file_path, 'r')
        counter = 0
        for line in data_stream:
            event = self.get_event_from_line(line)
            evaluation_model.handle_event(event, counter)
            counter += 1
        data_stream.close()
        results = evaluation_model.get_results()
        return results

