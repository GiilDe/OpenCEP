from typing import List, Callable, Union, Tuple
from Processor import Event
from PatternQueryGraph import PartialResult


_time_limit = None


class Operator:
    def check_operator(self, partial_results: Tuple[PartialResult]) -> bool:
        pass


class Seq:
    @staticmethod
    def check_operator(partial_results: Tuple[PartialResult]) -> bool:
        return all(partial_results[i].end_time < partial_results[i+1].start_time for i in range(len(partial_results)-1))


class And:
    @staticmethod
    def check_operator(partial_results: Tuple[PartialResult]) -> bool:
        return True


class EventPattern:
    """
    This class represents an operator application on some event types.
    for example if A, B, C are event types than this class might represent SEQ(A, B, C)
    the elements in event_types can be event types or OperatorApplication so we can represent recursive operator
    application (such as SEQ(A, B, AND(C, D))). Note that the order of event in operands for a Seq operator is important
    """
    def __init__(self, operands: List, operator: Operator):
        """
        :param operands:
        :param operator:
        """
        self.operator = operator
        self.operands = operands


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

    def check_condition(self, events: Union[Event, PartialResult]) -> bool:
        """
        :param events: the list of relevant events in the order they need to be called in the condition_apply_function
        :return: true if the condition holds for the relevant events
        """
        if type(events) == Event:
            return self.condition_apply_function(events)
        relevant_events = [events[i] for i in self.event_indices]
        return self.condition_apply_function(*relevant_events)


class PatternQuery:
    def __init__(self, event_pattern: EventPattern, conditions: List[Condition], time_limit):
        self.event_pattern = event_pattern
        self.conditions = conditions
        global _time_limit
        _time_limit = time_limit
