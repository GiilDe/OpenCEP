import Processor
import ProcessingUtilities
from GraphBasedProcessing import GraphBasedProcessingUtilities
import data_formats

stock_types = ['AAME', 'AAON', 'MCRS', 'ZHNE']


if __name__ == "__main__":
    processor = Processor.Processor("../sorted_NASDAQ_20080201_1.txt", data_formats.metastock7_attributes,
                                    data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    stock_types_with_identifiers = \
        [ProcessingUtilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stock_types)]
    to_and_with_identifiers =\
        [ProcessingUtilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate( ,stock_types)]
    and_pattern = ProcessingUtilities.EventPattern(to_and, ProcessingUtilities.And())
    and_pattern_with_identifier = ProcessingUtilities.EventTypeOrPatternAndIdentifier(and_pattern, 2)
    event_pattern = ProcessingUtilities.EventPattern(stock_types_with_identifiers + and_pattern_with_identifier,
                                                     ProcessingUtilities.Seq(range(3)))
