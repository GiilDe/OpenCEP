from .pattern_query_graph import *
import typing


class GraphInitializer:
    """
    abstract class initializing the evaluation graph
    """

    def get_graph(self, pattern_query: processing_utilities.CleanPatternQuery,
                  output_interface: processing_utilities.OutputInterface = processing_utilities.TrivialOutputInterface()) \
            -> PatternQueryGraph:
        pass


class TestingTree(GraphInitializer):
    """
    test class for the full_tree_test.py file
    """

    def get_graph(self, pattern_query: processing_utilities.CleanPatternQuery,
                  output_interface: processing_utilities.OutputInterface = processing_utilities.TrivialOutputInterface()) -> PatternQueryGraph:
        def conditions1(A, B):
            return 10 * A.volumes > B.volumes

        def conditions2(A, B):
            return A.volumes < B.volumes

        stocks = ['AAME', 'ZHNE', 'AAME', 'MCRS']
        stock_types_with_identifiers = \
            [processing_utilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stocks)]
        root_node = ConditionNode(processing_utilities.ListWrapper(), processing_utilities.Seq([-1, -3]),
                                  pattern_query.time_limit, identifier=-2,
                                  conditions=[processing_utilities.Condition(conditions1, [0, 3])])
        root_node.set_output_interface(output_interface)
        left_son = ConditionNode(processing_utilities.ListWrapper(), processing_utilities.And(),
                                 pattern_query.time_limit, identifier=-1,
                                 conditions=[processing_utilities.Condition(conditions2, [0, 1])])
        right_son = ConditionNode(processing_utilities.ListWrapper(), processing_utilities.And(),
                                  pattern_query.time_limit, identifier=-3,
                                  conditions=[])
        events = [EventNode(processing_utilities.ListWrapper(), pattern_query.time_limit, stock_and_id)
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
        graph = PatternQueryGraph(root_node, events)
        return graph


class LeftDeepTreeInitializer(GraphInitializer):
    """
    This class receives a PatternQuery and generates a trivial left deep tree representing the pattern query.
    This class cannot implement operator nesting.
    """

    def get_graph(self, pattern_query: processing_utilities.CleanPatternQuery,
                  output_interface: processing_utilities.OutputInterface = processing_utilities.TrivialOutputInterface()) \
            -> PatternQueryGraph:
        """
        assumes no operator nesting
        :param output_interface:
        :param pattern_query:
        :return: PatternQueryGraph.PatternQueryGraph that is a left deep tree representing the pattern query
        """

        def get_params_to_operator_construction():
            if operator_type == processing_utilities.Seq or operator_type == processing_utilities.And:
                return [initial_condition_node_identifier + 1, i]

        operator = pattern_query.event_pattern.operator
        operator_type = type(operator)
        event_dict = {event_and_identifier.identifier: event_and_identifier
                      for event_and_identifier in pattern_query.event_pattern.event_types_or_patterns}
        events = pattern_query.event_pattern.event_types_or_patterns if type(operator) != processing_utilities.Seq \
            else processing_utilities.Seq.get_sorted_by_identifier_order(event_dict, operator.identifiers_order)

        # our own condition node identifiers
        initial_condition_node_identifier = -1
        events_num = len(events)
        conditions = pattern_query.conditions
        inner_nodes = []
        old_parent = EventNode(processing_utilities.ListWrapper(), pattern_query.time_limit, events[0])
        leaves = [old_parent]
        seen_events = {events[0].identifier}

        # building the tree bottom-up
        if events_num > 1:
            for i in range(1, events_num):
                right_child = EventNode(processing_utilities.ListWrapper(), pattern_query.time_limit,
                                        events[i])
                identifier = events[i].identifier
                seen_events.add(identifier)
                leaves.append(right_child)
                new_parent = ConditionNode(processing_utilities.ListWrapper(),
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
        root_node.set_output_interface(output_interface)
        pattern_query_graph = PatternQueryGraph(root_node, leaves, inner_nodes, pattern_query.use_const_window)
        return pattern_query_graph


class NaiveMultipleTreesGraphBasedProcessing(processing_utilities.EvaluationModel):
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

    def set_pattern_queries(self, pattern_queries: typing.Iterable[processing_utilities.CleanPatternQuery],
                            output_interfaces: typing.List[processing_utilities.OutputInterface]):
        """
        initializes the graphs in our evaluation model according to the queries and corresponding output interfaces
        using the graph initializer class
        :param pattern_queries: the model's pattern queries
        :param output_interfaces: the corresponding output interfaces to the model's pattern queries
        """
        self.graphs = [self.graph_initializer.get_graph(pattern_query, output_interface) for
                       pattern_query, output_interface in zip(pattern_queries, output_interfaces)]

    def handle_event(self, event, event_counter):
        """
        passes the event to the model's trees in an attempt to try to add it where fit as a partial result
        :param event: the event to add
        :param event_counter: the corresponding event counter if the pattern uses fixed window instead of time limit
        """
        for graph in self.graphs:
            if graph.use_const_window:
                event.set_time_to_counter(event_counter)
            for event_node in graph.event_nodes:
                event_node.try_add_partial_result(event)

    def get_results(self) -> typing.List[typing.List]:
        return [graph.root_node.get_results() for graph in self.graphs]

    def clear(self):
        for g in self.graphs:
            g.clear()
