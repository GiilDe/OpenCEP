import typing
import itertools


class Event:
    """
    This class represents a specific event from the stream
    """
    def __init__(self, attribute_names: typing.List[str], values: typing.List, time_name, type_name):
        """
        :param attribute_names: the names of the attributes (those names should match the attribute names
        in the conditions functions)
        :param values: the attributes values
        :param time_name: the name of the time attribute
        :param type_name: the name of the type attribute
        """
        self.attributes = dict(zip(attribute_names, values))
        self.time_name = time_name
        self.type_name = type_name

    def __getattr__(self, item):
        """
        "Simulate" a normal class
        :param item: attribute name to return
        :return: the attribute value
        """
        if item == 'start_time' or item == 'end_time':
            return self.get_time()
        return self.attributes[item]

    def get_time(self):
        return self.attributes[self.time_name]

    def get_type(self):
        return self.attributes[self.type_name]

    @staticmethod
    def same_events(event1, event2) -> bool:
        return event1.attributes == event2.attributes

    def __len__(self):
        """
        for debugging reasons
        :return:
        """
        return len(self.attributes.keys())

    def __str__(self):
        join = ','.join(str(value) for value in self.attributes.values())
        return join

    def set_time_to_counter(self, counter):
        """
        used if pattern uses fixed window instead of time limit
        :param counter:
        """
        self.attributes[self.time_name] = counter


class EventTypeOrPatternAndIdentifier:
    """
    Simple tuple class that glues and event type and its identifier. Used in event pattern.
    """
    def __init__(self, event_type_or_pattern, identifier):
        self.event_type_or_pattern = event_type_or_pattern
        self.identifier = identifier


class EventPattern:
    """
    This class represents an operator application on some event types.
    for example if A, B, C are event types that this class might represent SEQ(A, B, C)
    the elements in event_types can be event types or EventPattern so we can represent recursive operator
    application (such as SEQ(A, B, AND(C, D))). Note that the order of event in operands for a Seq operator is important
    """
    def __init__(self, event_types_or_patterns: typing.List, operator):
        """
        :param event_types_or_patterns: List[Union[TypeEventAndIdentifier, EventPattern]]
        :param operator: operator associated with this event pattern
        """
        self.operator = operator
        self.event_types_or_patterns = event_types_or_patterns


class PartialResult:
    """
    This class holds a partial match.
    """
    def __init__(self, identifier_to_partial_result: typing.Dict, operator_type=None, identifier=None):
        """
        :param identifier_to_partial_result: a dictionary that holds all the partial results that this partial results
        is composed of. It holds a mapping from identifier to partial results.
        :param operator_type: the operator type that created this partial results
        :param identifier: the identifier associated with this partial match
        """
        self.identifier_to_partial_result = identifier_to_partial_result
        self.operator_type = operator_type
        self.identifier = identifier
        self.start_time = min(self.identifier_to_partial_result.values(),
                              key=lambda partial_result_or_event: partial_result_or_event.start_time).start_time
        self.end_time = max(self.identifier_to_partial_result.values(),
                            key=lambda partial_result_or_event: partial_result_or_event.end_time).end_time

    def is_event_wrapper(self) -> bool:
        """
        :return: True if this partial result holds a single event (the basis of the following recursive functions),
        False otherwise
        """
        return len(self.identifier_to_partial_result) == 1

    @staticmethod
    def init_with_partial_results(partial_results, operator_type=None, identifier=None):
        """
        composing a new partial results from partial results
        :param partial_results: results to compose
        :param operator_type: operator type that is composing this partial result
        :param identifier: relevant identifier
        :return: new partial results
        """
        identifier_to_partial_result = {}
        for partial_result in partial_results:
            if partial_result.operator_type is None and not partial_result.is_event_wrapper:
                identifier_to_partial_result.update(partial_result.identifier_to_partial_result)
            else:
                identifier_to_partial_result.update({partial_result.identifier: partial_result})

        return PartialResult(identifier_to_partial_result, operator_type, identifier)

    def completely_unpack(self) -> typing.Dict:
        """
        recursive function that traversed all inner partial results and unpacks all the identifier to partial results
        dictionaries that this partial results holds.
        :return: a dict that holds mapping from identifier to event for all the events that are
        (directly or in directly) in this partial result
        """
        if self.is_event_wrapper():
            return self.identifier_to_partial_result
        result = dict()
        for partial_result in self.identifier_to_partial_result.values():
            result.update(partial_result.completely_unpack())
        return result

    def unpack(self):
        """
        :return: similar to above, but returns a dict holding from identifier to partial results for all the partial results
        that are (directly or in directly) in this partial result
        """
        if self.is_event_wrapper() or self.operator_type is not None:
            return {self.identifier: self}
        result = dict()
        for partial_result in self.identifier_to_partial_result.values():
            result.update(partial_result.unpack())
        return result


class MemoryModel:
    """
    Abstract class that is responsible for managing the saving and loading of (partial) matches
    """
    def add_partial_result(self, partial_result: PartialResult):
        pass

    def __iter__(self):
        pass

    def get_relevant_results(self, current_time, time_limit, **kwargs):
        """
        :param current_time: time of the current processed event
        :param time_limit: the time limit associated with the query
        :param kwargs:
        :return: all the partial matches that are still in the time limit
        """
        pass

    def pop_results(self):
        """
        :return: all current partial matches
        """
        pass


class ListWrapper(MemoryModel):
    """
    A simple memory model implemented as a list
    """
    def __init__(self):
        self.l = []

    def __iter__(self):
        return iter(self.l)

    def add_partial_result(self, partial_result: PartialResult):
        self.l.append(partial_result)

    def get_relevant_results(self, current_time, time_limit, **kwargs):
        is_sorted = kwargs['is_sorted']
        if is_sorted:
            i = len(self.l) - 1 if len(self.l) > 0 else 0
            while i > 0 and current_time - self.l[i].start_time <= time_limit:
                i -= 1
            del self.l[:i]
        else:
            relevant_events = [result for result in self.l if current_time - result.start_time <= time_limit]
            self.l = relevant_events
        return self.l

    def pop_results(self):
        temp = self.l
        self.l = []
        return temp


class Condition:
    """
    this class represents a predicate (for example for events A, B: A.x > B.x)
    """
    def __init__(self, condition_apply_function: typing.Callable, event_identifiers: typing.List):
        """
        :param condition_apply_function: a boolean function that gets the relevant event and applies the condition
        :param event_identifiers: the identifiers of the events in the PatternQuery event_types list
            to be checked by this condition. Note that event_indices needs to be ordered in the order arguments should
            be passed to the condition_apply_function!
        """
        self.condition_apply_function = condition_apply_function
        self.event_identifiers = event_identifiers

    def check_condition(self, partial_result: PartialResult) -> bool:
        """
        :param partial_result: partial_result.events is the list of relevant events in the order they
        need to be called in the condition_apply_function
        :return: true if the condition holds for the relevant events
        """
        events = partial_result.completely_unpack()
        relevant_events = [events[identifier] for identifier in self.event_identifiers]
        return self.condition_apply_function(*relevant_events)


class PatternQuery:
    """
    This is an abstract class representing a possible input for a possible interface. In order to create new ways of
    input to the system override this class with new class and override Interface class with a new class that can
    process the new PatternQuery class
    """
    pass


class CleanPatternQuery(PatternQuery):
    """
    A class representing a "clean" pattern query meaning the pattern query input after being processed by the
    interface class (the inner algorithms only know this class).
    """
    def __init__(self, event_pattern: EventPattern, conditions: typing.List[Condition], time_limit,
                 use_const_window=False):
        self.event_pattern = event_pattern
        self.conditions = conditions
        self.time_limit = time_limit
        self.use_const_window = use_const_window


class StringPatternQuery(PatternQuery):
    """
    This class represents a pattern query, its input is a string in the following form:
    WHERE
    """
    # def __init__(self, pattern_query: str):


class Operator:
    """
    Abstract class representing an operator
    """
    def get_new_results(self, children_buffers: typing.List[MemoryModel],
                        new_result: PartialResult, identifier) -> typing.List[PartialResult]:
        """
        :param children_buffers: list where each cell is a list that holds partial matches
        :param new_result: the new partial results to be composed to existing partial results
        :param identifier: the relevant identifier
        :return: new results composed of the new result and a partial results from each child buffer
        """
        pass

    @staticmethod
    def get_all_possible_combinations(children_buffers: typing.List[MemoryModel],
                                      new_result: PartialResult):
        """
        :param children_buffers: list where each cell is a list that holds partial matches.
        :param new_result: the new partial results to be composed to existing partial results
        :return: all possible combinations of composing a partial result from each buffer and the new results
        """
        children_buffers.append([new_result])
        return itertools.product(*children_buffers)

    @staticmethod
    def get_events_from_partial_results(partial_results):
        result = {}
        for partial_result in partial_results:
            result.update(partial_result.unpack())
        return result

    @staticmethod
    def contains_same_event_multiple_times(partial_results) -> bool:
        """
        :param partial_results:
        :return: True checks if the partial results contains the same event multiple times, False otherwise
        """
        s = set()
        for partial_result in partial_results:
            events = partial_result.completely_unpack()
            for event in events.values():
                value_representation = tuple(event.attributes.values())
                if value_representation in s:
                    return True
                s.add(value_representation)
        return False


class Seq(Operator):
    """
    Class representing a seq operator
    """
    def __init__(self, identifiers_order):
        """
        :param identifiers_order: an iterable defining the order of the identifiers in the seq
        """
        self.identifiers_order = identifiers_order

    @staticmethod
    def get_sorted_by_identifier_order(partial_results_dict, identifiers_order):
        """
        :param partial_results_dict: a dictionary mapping from identifier to partial result
        :param identifiers_order: a list of identifiers that defines the desired order (for example: ['A', 'B', 'C'])
        :return: a list of partial results ordered by the given ordering
        """
        events_ordered = []
        for identifier in identifiers_order:
            events_ordered.append(partial_results_dict[identifier])
        return events_ordered

    def get_new_results(self, children_buffers: typing.List[MemoryModel],
                        new_result: PartialResult, identifier) -> typing.List[PartialResult]:
        """
        :param children_buffers: list where each cell is a list that holds partial matches
        :param new_result: the new partial results to be composed to existing partial results
        :param identifier: the relevant identifier
        :return: returns every ordered combination that does not contain the same event twice
        """
        result = []
        for partial_results in self.get_all_possible_combinations(children_buffers, new_result):
            partial_results_dict = self.get_events_from_partial_results(partial_results)
            if not self.contains_same_event_multiple_times(partial_results_dict.values()):
                partial_results_ordered = self.get_sorted_by_identifier_order(partial_results_dict,
                                                                              self.identifiers_order)
                if all(partial_results_ordered[i].end_time <= partial_results_ordered[i + 1].start_time
                       for i in range(len(partial_results_ordered) - 1)):
                    result.append(PartialResult.init_with_partial_results(partial_results, Seq, identifier))
        return result


class And(Operator):
    """
    Class representing an and operator
    """
    def __init__(self, *args):
        pass

    def get_new_results(self, children_buffers: typing.List[MemoryModel],
                        new_result: PartialResult, identifier) -> typing.List[PartialResult]:
        """
        :param children_buffers: list where each cell is a list that holds partial matches
        :param new_result: the new partial results to be composed to existing partial results
        :param identifier: the relevant identifier
        :return: returns every combination that does not contain the same event twice
        """
        return [PartialResult.init_with_partial_results(partial_results, And, identifier) for partial_results in
                self.get_all_possible_combinations(children_buffers, new_result) if not
                self.contains_same_event_multiple_times(self.get_events_from_partial_results(partial_results).values())]


class InputInterface:
    """
    This is an abstract class to generalize the possible ways of processing various types of PatterQuery
    """
    def get_clean_pattern_queries(self, pattern_queries: typing.Iterable[PatternQuery]) \
            -> typing.Iterable[CleanPatternQuery]:
        pass


class TrivialInputInterface(InputInterface):
    """
    This interface does nothing as it already receives a CleanPatternQuery
    """
    def get_clean_pattern_queries(self, pattern_queries: typing.Iterable[CleanPatternQuery]) \
            -> typing.Iterable[CleanPatternQuery]:
        return pattern_queries


class StringInputInterface(InputInterface):
    """
    This interface gets a StringPatternQuery.
    """


class OutputInterface:
    """
    This is an abstract class to generalize the various ways of outputing the results to the user
    """
    def output_results(self, results):
        pass

    @staticmethod
    def output_while_running() -> bool:
        """
        :return: True if this output interface supports outputting part of the results while stream is still processed
        (better for performance where possible)
        """
        pass


class TrivialOutputInterface(OutputInterface):
    """
    Trivial output interface that simply returns the results it gets
    """
    def output_results(self, results):
        return results

    @staticmethod
    def output_while_running() -> bool:
        return False


class FileOutputInterface(OutputInterface):
    """
    An OutputInterface that outputs the results to a file
    """
    def __init__(self, output_file: str):
        """
        :param output_file: file path to output results to
        """
        self.output_file = output_file
        self.first_call = True

    def output_results(self, query_result):
        def result_to_str(_result: typing.List[Event]):
            s = " ###result### \n"
            for event in _result:
                s += str(event) + "\n"
            s += " ### "
            return s

        output = open(self.output_file, 'a') if not self.first_call else open(self.output_file, 'w+')
        self.first_call = False
        for result in query_result:
            output.write(result_to_str(result))
        output.close()
        return query_result

    @staticmethod
    def output_while_running() -> bool:
        return True


class EvaluationModel:
    """
    An abstract class responsible of processing events
    """
    def handle_event(self, event, event_counter):
        pass

    def set_pattern_queries(self, pattern_queries: typing.Iterable[CleanPatternQuery],
                            output_interfaces: typing.List[OutputInterface]):
        """
        called before starting to process the stream to initialize the model
        :param pattern_queries:
        :param output_interfaces:
        :return:
        """
        pass

    def get_results(self) -> typing.List:
        pass
