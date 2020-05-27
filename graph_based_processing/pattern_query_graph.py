from typing import List, Union
from .. import processing_utilities


class IntWrapper:
    val: int = 0

steps_counter = IntWrapper()

class Node:
    """
    Abstract class, predecessor of ConditionNode and EventNode
    """
    def __init__(self, memory_model: processing_utilities.MemoryModel, time_limit,
                 conditions: List[processing_utilities.Condition] = None, parent=None):
        """
        :param memory_model: memory mode to save the partial results.
        :param time_limit: the time limit associated with the query
        :param conditions: the conditions associated with the query
        :param parent: predecessor node
        """
        if conditions is None:
            self.conditions = []
        self.conditions = conditions if conditions else []
        self.parent = parent
        self.partial_results_buffer = memory_model
        self.time_limit = time_limit

    def add_condition(self, condition: processing_utilities.Condition):
        self.conditions.append(condition)

    def set_parent(self, parent):
        self.parent = parent

    def get_relevant_results(self, current_time):
        """
        :param current_time: time of the current processed event
        :return: all the partial matches that are still in the time limit
        """
        return self.partial_results_buffer.get_relevant_results(current_time, self.time_limit,
                                                                is_sorted=type(self) == EventNode)

    def get_results(self):
        """
        :return: all saved (partial, if node is not root node) matches
        """
        return [list(partial_result.completely_unpack().values()) for partial_result in self.partial_results_buffer]

    def clear(self):
        self.partial_results_buffer.clear()

class ConditionNode(Node):
    """
    Represents an inner node in the graph that holds an operator and a condition list and partial results
    """
    def __init__(self, memory_model: processing_utilities.MemoryModel, operator: processing_utilities.Operator,
                 time_limit, children: List[Node] = None, identifier=None,
                 conditions: List[processing_utilities.Condition] = None, parent=None):
        """
        :param memory_model: memory mode to save the partial results.
        :param time_limit: the time limit associated with the query
        :param conditions: the conditions associated with the query
        :param operator: operator associated with this node
        :param parent: predecessor node
        """
        super().__init__(memory_model, time_limit, conditions, parent)
        self.children = children
        self.operator = operator
        self.identifier = identifier

    def set_output_interface(self, output_interface: processing_utilities.OutputInterface =
    processing_utilities.TrivialOutputInterface()):
        self.output_interface = output_interface

    def _check_conditions(self, partial_result: Union[processing_utilities.PartialResult, processing_utilities.Event])\
            -> bool:
        """
        :param partial_result: partial_result to check
        :return: True if partial result is within the time limit and all conditions hold, False otherwise
        """
        steps_counter.val += 1
        time_limit = partial_result.end_time - partial_result.start_time <= self.time_limit
        if time_limit:
            steps_counter.val += len(self.conditions)
        return time_limit and \
               all(condition.check_condition(partial_result) for condition in self.conditions)

    def try_add_partial_result(self, partial_result: processing_utilities.PartialResult, diffuser_child: Node):
        """
        Anytime a node succeeds in building a partial results he "diffuses" it to his predecessor to try
        and further build it
        :param partial_result: the new partial results
        :param diffuser_child: the child node that that the new partial results was built in
        :return: self
        """
        current_time = partial_result.start_time

        children_buffers = [child.get_relevant_results(current_time) for child in self.children
                            if child != diffuser_child]
        for new_result in self.operator.get_new_results(children_buffers, partial_result, self.identifier):
            if self._check_conditions(new_result):
                new_result.operator_type = type(self.operator)
                self.partial_results_buffer.add_partial_result(new_result)
                if self.parent:
                    self.parent.try_add_partial_result(new_result, self)

        if self.is_root() and self.output_interface is not None and \
                self.output_interface.output_while_running():
            results = self.partial_results_buffer.pop_results()
            self.output_interface.output_results\
                ([list(partial_result.completely_unpack().values()) for partial_result in results])
        return self

    def set_children(self, children: List[Node]):
        self.children = children

    def is_root(self):
        return self.parent is None


class EventNode(Node):
    """
    Represents a leaf node in the graph.
    """
    def __init__(self, memory_model: processing_utilities.MemoryModel, time_limit,
                 event_type_and_identifier: processing_utilities.EventTypeOrPatternAndIdentifier,
                 parent: ConditionNode = None, conditions: List[processing_utilities.Condition] = None):
        """
        :param memory_model: memory mode to save the partial results.
        :param time_limit: the time limit associated with the query
        :param conditions: the conditions associated with the query
        :param parent: predecessor node
        :param event_type_and_identifier: event type associated with this lead node and its matching identifier
        (in patterns with multiple events of the same type identifiers need to be used)
        """
        super().__init__(memory_model, time_limit, conditions, parent)
        self.event_type = event_type_and_identifier.event_type_or_pattern
        self.event_identifier = event_type_and_identifier.identifier

    def _check_conditions(self, partial_result: Union[processing_utilities.PartialResult, processing_utilities.Event])\
            -> bool:
        steps_counter.val += len(self.conditions)
        return all(condition.check_condition(partial_result) for condition in self.conditions)

    def try_add_partial_result(self, event: processing_utilities.Event):
        """
        adds a partial result if it's from the right type
        :param event: an event corresponding to this leaf node
        :return: self
        """
        steps_counter.val += 1
        if self.event_type == event.get_type():
            partial_result = processing_utilities.PartialResult({self.event_identifier: event},
                                                                identifier=self.event_identifier)
            if self._check_conditions(partial_result):
                self.partial_results_buffer.add_partial_result(partial_result)
                self.parent.try_add_partial_result(partial_result, self)
        return self


class PatternQueryGraph:
    """
    a graph that represents a pattern query without operator nesting
    """
    def __init__(self, root_node: ConditionNode, event_nodes: List[EventNode], inner_nodes: List[ConditionNode],
                 use_const_window=False):
        """
        :param root_node: graph's root node
        :param event_nodes: graph's event nodes
        :param use_const_window: if should use a constant length of events rather than a time limit (if so will treat
        the time limit paramter as the window size)
        """
        self.root_node = root_node
        self.event_nodes = event_nodes
        self.inner_nodes = inner_nodes
        self.use_const_window = use_const_window
        steps_counter.val = 0
        self.steps_counter = steps_counter
        
    def clear(self):
        for node in self.event_nodes + self.inner_nodes:
            node.clear()


