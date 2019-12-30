import ProcessingUtilities
from GraphBasedProcessing import PatternQueryGraph
import typing


class GraphInitializer:
    def get_graph(self, pattern_query: ProcessingUtilities.CleanPatternQuery) -> PatternQueryGraph.PatternQueryGraph:
        pass


class TestingTree(GraphInitializer):
    def get_graph(self, pattern_queries: ProcessingUtilities.CleanPatternQuery) -> PatternQueryGraph.PatternQueryGraph:

        def conditions1(A, B):
            return 10*A.volumes > B.volumes

        def conditions2(A, B):
            return A.volumes < B.volumes

        stocks = ['AAME', 'ZHNE', 'AAME', 'MCRS']
        stock_types_with_identifiers = \
            [ProcessingUtilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stocks)]
        root_node = PatternQueryGraph.ConditionNode(ProcessingUtilities.ListWrapper(), ProcessingUtilities.Seq([-1, -3]),
                                                    pattern_queries.time_limit, identifier=-2,
                                                    conditions=[ProcessingUtilities.Condition(conditions1, [0, 3])])
        left_son = PatternQueryGraph.ConditionNode(ProcessingUtilities.ListWrapper(), ProcessingUtilities.And(),
                                                    pattern_queries.time_limit, identifier=-1,
                                                    conditions=[ProcessingUtilities.Condition(conditions2, [0, 1])])
        right_son = PatternQueryGraph.ConditionNode(ProcessingUtilities.ListWrapper(), ProcessingUtilities.And(),
                                                   pattern_queries.time_limit, identifier=-3,
                                                   conditions=[])
        events = [PatternQueryGraph.EventNode(ProcessingUtilities.ListWrapper(), stock_and_id)
                  for stock_and_id in stock_types_with_identifiers]
        left_son.set_children([events[0], events[1]])
        right_son.set_children([events[2], events[3]])
        root_node.set_children([left_son, right_son])
        left_son.set_parent(root_node)
        right_son.set_parent(root_node)
        events[0].set_parent(left_son)
        events[1].set_parent(left_son)
        events[2].set_parent(right_son)
        events[3].set_parent(right_son)
        graph = PatternQueryGraph.PatternQueryGraph(root_node, events)
        return graph


class LeftDeepTreeInitializer(GraphInitializer):
    """
    This class receives a PatternQuery and generates a trivial left deep tree representing the pattern query.
    This class cannot implement operator nesting.
    """
    def get_graph(self, pattern_query: ProcessingUtilities.CleanPatternQuery) -> PatternQueryGraph.PatternQueryGraph:
        """
        assumes no operator nesting
        :param pattern_query:
        :return: PatternQueryGraph.PatternQueryGraph that is a left deep tree representing the pattern query
        """
        def get_params_to_operator_construction():
            """
            this function is responsible to output to the operator builder the correct params that he need according to
            its type
            :param i:
            :param operator_type:
            :return:
            """
            if operator_type == ProcessingUtilities.Seq or operator_type == ProcessingUtilities.And:
                return [initial_condition_node_identifier + 1, i]

        operator = pattern_query.event_pattern.operator
        operator_type = type(operator)
        event_dict = {event_and_identifier.identifier: event_and_identifier
                      for event_and_identifier in pattern_query.event_pattern.event_types_or_patterns}
        events = pattern_query.event_pattern.event_types_or_patterns if type(operator) != ProcessingUtilities.Seq \
            else ProcessingUtilities.Seq.get_sorted_by_identifier_order(event_dict, operator.identifiers_order)
        initial_condition_node_identifier = -1
        events_num = len(events)
        conditions = pattern_query.conditions
        inner_nodes = []
        old_parent = PatternQueryGraph.EventNode(ProcessingUtilities.ListWrapper(), events[0])
        leaves = [old_parent]
        seen_events = {events[0].identifier}
        if events_num > 1:
            for i in range(1, events_num):
                right_child = PatternQueryGraph.EventNode(ProcessingUtilities.ListWrapper(), events[i])
                identifier = events[i].identifier
                seen_events.add(identifier)
                leaves.append(right_child)
                new_parent = PatternQueryGraph.ConditionNode(ProcessingUtilities.ListWrapper(),
                                                             operator_type(get_params_to_operator_construction()),
                                                             pattern_query.time_limit, [old_parent, right_child],
                                                             initial_condition_node_identifier)
                new_conditions = []
                for condition in conditions:
                    condition_identifiers = set(condition.event_identifiers)
                    if condition_identifiers.issubset(seen_events):
                        new_parent.add_condition(condition)
                    else:
                        new_conditions.append(condition)
                conditions = new_conditions
                old_parent.set_parent(new_parent)
                right_child.set_parent(new_parent)
                old_parent = new_parent
                inner_nodes.append(new_parent)
                initial_condition_node_identifier -= 1

        root_node = new_parent if events_num > 1 else old_parent
        pattern_query_graph = PatternQueryGraph.PatternQueryGraph(root_node, leaves)
        return pattern_query_graph


class NaiveMultipleTreesGraphBasedProcessing(ProcessingUtilities.EvaluationModel):
    """
    This class initializes a graph for each input query.
    It iterates the events one by one and tries to build each graph that it received.
    It saves its partial graphs explicitly in memory.
    """
    def __init__(self, graph_initializer: GraphInitializer):
        """
        :param graph_initializer: receives PatternQuery and returns a graph that represents the pattern query
        """
        self.graph_initializer = graph_initializer
        self.graphs = []

    def set_pattern_queries(self, pattern_queries: typing.Iterable[ProcessingUtilities.CleanPatternQuery]):
        self.graphs = [self.graph_initializer.get_graph(pattern_query) for pattern_query in pattern_queries]

    def handle_event(self, event):
        for graph in self.graphs:
            for event_node in graph.event_nodes:
                event_node.try_add_partial_result(event)

    def get_results(self) -> typing.List[typing.List]:
        return [graph.root_node.get_results() for graph in self.graphs]
