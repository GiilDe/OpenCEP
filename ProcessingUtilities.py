from typing import List, Tuple, Callable, Union
from itertools import chain

_time_limit = None


class Event:
    def __init__(self, attribute_names: List[str], values: List, time_name: str):
        self.attributes = dict(zip(attribute_names, values))
        self.time_name = time_name

    def __getattr__(self, item):
        """
        "Simulate" a normal class
        :param item: attriute name to return
        :return:
        """
        return self.attributes[item]

    def get_time(self):
        return self.attributes[self.time_name]

    def __str__(self):
        join = ','.join(str(value) for value in self.attributes.values())
        return join + "\n"


class EventPattern:
    """
    This class represents an operator application on some event types.
    for example if A, B, C are event types than this class might represent SEQ(A, B, C)
    the elements in event_types can be event types or OperatorApplication so we can represent recursive operator
    application (such as SEQ(A, B, AND(C, D))). Note that the order of event in operands for a Seq operator is important
    """
    def __init__(self, operands_quantity: int, operator):
        """
        :param operands_quantity:
        :param operator:
        """
        self.operator = operator
        self.operands_quantity = operands_quantity


class PartialResult:
    """
    just a possible implementation, not finished
    """
    def __init__(self, events: Union[List[Event], Event]):
        self.events = [events] if type(events) == Event else events
        self.start_time = min(self.events, key=lambda event: event.get_time()).get_time()
        self.end_time = max(self.events, key=lambda event: event.get_time()).get_time()

    def __getitem__(self, i):
        return self.events[i]

    def __iter__(self):
        return self.events

    @staticmethod
    def init_with_partial_results(partial_results):
        new_events_lists = [partial_result.events for partial_result in partial_results]
        new_events = list(chain(*new_events_lists))
        return PartialResult(new_events)

    def __str__(self):
        s = " ###result### \n"
        for event in self.events:
            s += str(event)
        s += " ### "
        return s


class Condition:
    """
    this class represents a predicate (for example for events A, B: A.x > B.x)
    """
    def __init__(self, condition_apply_function: Callable, event_indices: List[int]):
        """
        :param condition_apply_function: a boolean function that gets the relevant event and applies the condition
        :param event_indices: the indices of the events in the PatternQuery event_types list
            to be checked by this condition. Note that event_indices needs to be ordered in the order arguments should
            be passed to the condition_apply_function!
        """
        self.condition_apply_function = condition_apply_function
        self.event_indices = event_indices

    def check_condition(self, partial_result: PartialResult) -> bool:
        """
        :param partial_result: partial_result.events is the list of relevant events in the order they
        need to be called in the condition_apply_function
        :return: true if the condition holds for the relevant events
        """
        if len(partial_result.events) == 1:
            return self.condition_apply_function(partial_result.events[0])
        relevant_events = [partial_result.events[i] for i in self.event_indices]
        return self.condition_apply_function(*relevant_events)


class PatternQuery:
    def __init__(self, event_pattern: EventPattern, conditions: List[Condition], time_limit):
        self.event_pattern = event_pattern
        self.conditions = conditions
        global _time_limit
        _time_limit = time_limit


class EvaluationModel:
    def handle_event(self, event):
        pass

    def set_pattern_query(self, pattern_query: PatternQuery):
        pass

    def get_results(self) -> List:
        pass


class Operator:
    def check_operator(self, partial_results: Tuple[PartialResult]) -> bool:
        pass


class Seq(Operator):
    def check_operator(self, partial_results: Tuple[PartialResult]) -> bool:
        return all(partial_results[i].end_time <= partial_results[i+1].start_time for i in range(len(partial_results)-1))


class StriclyMonotoneSeq(Operator):
    def check_operator(self, partial_results: Tuple[PartialResult]) -> bool:
        return all(partial_results[i].end_time < partial_results[i+1].start_time for i in range(len(partial_results)-1))


class And(Operator):
    def check_operator(self, partial_results: Tuple[PartialResult]) -> bool:
        return True