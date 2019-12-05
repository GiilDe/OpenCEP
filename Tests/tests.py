import Processor
import ProcessingUtilities
from GraphBasedProcessing import GraphBasedProcessingUtilities
import data_formats

stock_types = ['AAME', 'AAON', 'MCRS', 'ZHNE']


if __name__ == "__main__":
    # Naive seq test
    processor = Processor.Processor("../sorted_NASDAQ_20080201_1.txt", data_formats.metastock7_attributes,
                                    data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    stock_types_with_identifiers = \
        [ProcessingUtilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stock_types)]
    # shuffle order for testing purposes
    temp = stock_types_with_identifiers[2]
    stock_types_with_identifiers[2] = stock_types_with_identifiers[0]
    stock_types_with_identifiers[0] = temp
    event_pattern = ProcessingUtilities.EventPattern(stock_types_with_identifiers, ProcessingUtilities.Seq(range(4)))
    pattern_query = ProcessingUtilities.CleanPatternQuery(event_pattern, [], 16)
    left_deep_initializer = GraphBasedProcessingUtilities.LeftDeepTreeInitializer()
    graph_based_processor = GraphBasedProcessingUtilities.GraphBasedProcessing(left_deep_initializer)
    processor.query(pattern_query, graph_based_processor, ProcessingUtilities.TrivialInputInterface(),
                    ProcessingUtilities.FileOutputInterface("seq_test_results.txt"))
    # Naive and test
    event_pattern = ProcessingUtilities.EventPattern(stock_types_with_identifiers, ProcessingUtilities.And())
    pattern_query = ProcessingUtilities.CleanPatternQuery(event_pattern, [], 16)
    processor.query(pattern_query, graph_based_processor, ProcessingUtilities.TrivialInputInterface(),
                    ProcessingUtilities.FileOutputInterface("and_test_results.txt"))
