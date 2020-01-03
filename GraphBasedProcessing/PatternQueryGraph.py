from typing import List, Union
import ProcessingUtilities


class Node:
    """
    abstract class, predecessor of ConditionNode and EventNode
    """
    def __init__(self, memory_model: ProcessingUtilities.MemoryModel, time_limit,
                 conditions: List[ProcessingUtilities.Condition] = None, parent=None):
        if conditions is None:
            self.conditions = []
        self.conditions = conditions if conditions else []
        self.parent = parent
        self.partial_results_buffer = memory_model
        self.time_limit = time_limit

    def add_condition(self, condition: ProcessingUtilities.Condition):
        self.conditions.append(condition)

    def set_parent(self, parent):
        self.parent = parent

    def get_relevant_results(self, current_time):
        return self.partial_results_buffer.get_relevant_results(current_time, self.time_limit,
                                                                is_sorted=type(self) == EventNode)

    def get_results(self):
        return [list(partial_result.completely_unpack().values()) for partial_result in self.partial_results_buffer]


class ConditionNode(Node):
    """
    represents an inner node in the graph that holds an operator and a condition list and partial results
    """
    def __init__(self, memory_model: ProcessingUtilities.MemoryModel, operator: ProcessingUtilities.Operator,
                 time_limit, children: List[Node] = None, identifier=None,
                 conditions: List[ProcessingUtilities.Condition] = None, parent=None):
        """
        :param children:
        :param conditions:
        :param operator:
        :param parent:
        """
        super().__init__(memory_model, conditions, parent)
        self.children = children
        self.operator = operator
        self.identifier = identifier
        self.time_limit = time_limit

    def set_output_interface(self, output_interface: ProcessingUtilities.OutputInterface =
    ProcessingUtilities.TrivialOutputInterface()):
        self.output_interface = output_interface

    def _check_conditions(self, partial_result: Union[ProcessingUtilities.PartialResult, ProcessingUtilities.Event])\
            -> bool:
        return partial_result.end_time - partial_result.start_time <= self.time_limit and \
               all(condition.check_condition(partial_result) for condition in self.conditions)

    def try_add_partial_result(self, partial_result: ProcessingUtilities.PartialResult, diffuser_child: Node):
        """
        :param partial_result:
        :param diffuser_child:
        :return:
        """
        current_time = partial_result.start_time
        if self.is_root() and self.output_interface.output_while_running():
            results = self.partial_results_buffer.pop_results()
            self.output_interface.output_results\
                ([list(partial_result.completely_unpack().values()) for partial_result in results])

        children_buffers = [child.get_relevant_results(current_time) for child in self.children
                            if child != diffuser_child]
        for new_result in self.operator.get_new_results(children_buffers, partial_result, self.identifier):
            if self._check_conditions(new_result):
                new_result.operator_type_of_node = type(self.operator)
                self.partial_results_buffer.add_partial_result(new_result)
                if self.parent:
                    self.parent.try_add_partial_result(new_result, self)
        return self

    def set_children(self, children: List[Node]):
        self.children = children

    def is_root(self):
        return self.parent is None


class EventNode(Node):
    """
    represents a leaf node in the graph that holds events
    """
    def __init__(self, memory_model: ProcessingUtilities.MemoryModel, time_limit,
                 event_type_and_identifier: ProcessingUtilities.EventTypeOrPatternAndIdentifier,
                 parent: ConditionNode = None, conditions: List[ProcessingUtilities.Condition] = None):
        super().__init__(memory_model, time_limit, conditions, parent)
        self.event_type = event_type_and_identifier.event_type_or_pattern
        self.event_identifier = event_type_and_identifier.identifier

    def _check_conditions(self, partial_result: Union[ProcessingUtilities.PartialResult, ProcessingUtilities.Event])\
            -> bool:
        return all(condition.check_condition(partial_result) for condition in self.conditions)

    def try_add_partial_result(self, event: ProcessingUtilities.Event):
        """
        adds a partial result
        :param event: an event corresponding to this leaf node
        :return: self
        """
        if self.event_type == event.get_type():
            partial_result = ProcessingUtilities.PartialResult({self.event_identifier: event},
                                                               identifier=self.event_identifier)
            if self._check_conditions(partial_result):
                self.partial_results_buffer.add_partial_result(partial_result)
                self.parent.try_add_partial_result(partial_result, self)
        return self


class PatternQueryGraph:
    """
    a graph that represents a pattern query without operator nesting
    """
    def __init__(self, root_node: ConditionNode, event_nodes: List[EventNode], use_const_window=False):
        self.root_node = root_node
        self.event_nodes = event_nodes
        self.use_const_window = use_const_window


