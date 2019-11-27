import Processor
import ProcessingUtilities
import GraphBasedProcessingUtilities

stock_types = ['AAME', 'AAON', 'MCRS', 'ZHNE']
a_type = lambda event: event.symbol == 'AAME'
b_type = lambda event: event.symbol == 'AAON'
c_type = lambda event: event.symbol == 'MCRS'
d_type = lambda event: event.symbol == 'ZHNE'


if __name__ == "__main__":
    # Naive seq test
    processor = Processor.Processor("sorted_NASDAQ_20080201_1.txt", Processor.metastock7_attributes,
                                    Processor.metastock7_time_index)
    event_types_conditions = [ProcessingUtilities.Condition(func, [i])
                              for i, func in enumerate([a_type, b_type, c_type, d_type])]
    pattern_query = ProcessingUtilities.PatternQuery(ProcessingUtilities.EventPattern(4, ProcessingUtilities.Seq()),
                                                     event_types_conditions, 35)
    left_deep_initializer = GraphBasedProcessingUtilities.LeftDeepTreeInitializer()
    graph_based_processor = GraphBasedProcessingUtilities.GraphBasedProcessing(left_deep_initializer)
    processor.query(pattern_query, graph_based_processor, "seq_test_results.txt")
    # Naive and test
    pattern_query = ProcessingUtilities.PatternQuery(ProcessingUtilities.EventPattern(4, ProcessingUtilities.And()),
                                                     event_types_conditions, 35)
    processor.query(pattern_query, graph_based_processor, "and_test_results.txt")
