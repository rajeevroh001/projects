# used framework: Apache Beam
import apache_beam as beam
import typing
import logging
import sys

logger = logging.getLogger('apache_beam_sol')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

# Schema for input dataset: dataset1.csv
Dataset1Schema = typing.NamedTuple(
    'Dataset1Schema',
    invoice_id=int,
    legal_entity=str,
    counter_party=str,
    rating=int,
    status=str,
    value=int,
    tier=int
)

beam.coders.registry.register_coder(Dataset1Schema, beam.coders.RowCoder)


class GroupByCols(beam.DoFn):
    """Groups elements by the legal_entity and counter_party fields."""

    def process(self, element, **kwargs):
        yield (('l_e', element.legal_entity), ('c_p', element.counter_party)), (
            element.status, element.rating, element.value)
        yield ('l_e', element.legal_entity), (element.status, element.rating, element.value)
        yield ('c_p', element.counter_party), (element.status, element.rating, element.value)
        yield ('tier', element.tier), (element.status, element.rating, element.value)


class MapToSchema(beam.DoFn):
    """Converts a row of data to a Dataset1Schema object."""

    def process(self, element, tier_dict, **kwargs):
        element = element.split(',')
        yield Dataset1Schema(
            invoice_id=int(element[0]),
            legal_entity=str(element[1]),
            counter_party=str(element[2]),
            rating=int(element[3]),
            status=str(element[4]),
            value=int(element[5]),
            tier=int(tier_dict[element[2]])
        )


class ConvertToResultRow(beam.DoFn):
    """Converts final row of data, where the value is a tuple of the status, rating, and value from the dataset1 and
    the tier from the dataset2."""

    def process(self, element):
        count = element[1][-1]
        if len(element[0][0]) == 2:
            yield ",".join([str(cell) for cell in [element[0][0][1], element[0][1][1], count, *element[1][:-1]]])
        else:
            yield ",".join(
                [str(cell) for cell in
                 [
                     element[0][1] if element[0][0] == 'l_e' else count,
                     element[0][1] if element[0][0] == 'c_p' else count,
                     element[0][1] if element[0][0] == 'tier' else count,
                     *element[1][:-1]]
                 ]
            )


class CalculateSumMax(beam.CombineFn):
    """Calculates the maximum rating and the total value for each (legal_entity, counter_party) pair."""

    def create_accumulator(self):
        return 0, 0, 0, 0

    def add_input(self, accumulator, element):
        max_rating, total_value_arap, total_value_accr, count = accumulator
        status, rating, value = element
        count += 1
        max_rating = max(max_rating, rating)
        if status == 'ARAP':
            total_value_arap += value
        elif status == 'ACCR':
            total_value_accr += value
        return max_rating, total_value_arap, total_value_accr, count

    def merge_accumulators(self, accumulators):
        max_rating, total_value_arap, total_value_accr, count = zip(*accumulators)
        return max(max_rating), sum(total_value_arap), sum(total_value_accr), sum(count)

    def extract_output(self, accumulator):
        return accumulator


def main():
    """Runs the Apache Beam pipeline."""

    logger.info("Process started using Apache-beam Framework")

    with beam.Pipeline() as p:
        dataset2 = (
                p | "Read dataset2" >> beam.io.ReadFromText('inputs/dataset2.csv', skip_header_lines=True,
                                                            delimiter="\r".encode())
                | "Filter empty rows dataset2" >> beam.Filter(lambda row: not row.isspace())
                | "Convert ds2 to json" >> beam.Map(lambda row: row.split(','))
        )

        dataset1 = (
                p | "Read dataset1" >> beam.io.ReadFromText('inputs/dataset1.csv', skip_header_lines=True,
                                                            delimiter="\r".encode())
                | "Filter empty rows dataset1" >> beam.Filter(lambda row: not row.isspace())
                | "Convert row to object" >> beam.ParDo(MapToSchema(), beam.pvalue.AsDict(dataset2)).with_output_types(
            Dataset1Schema)
        )
        logger.info("Datasets read successfully")
        (
                dataset1
                | 'Create group key tuple' >> beam.ParDo(GroupByCols())
                | 'Calculate max_rating and total_values' >> beam.CombinePerKey(CalculateSumMax())
                | 'Convert to csv rows' >> beam.ParDo(ConvertToResultRow())
                | 'ExportToCsv' >> beam.io.WriteToText(
                    "outputs/apache_beam_output",
                    ".csv",
                    shard_name_template='',
                    header="legal_entity,counter_party,tier,max_rating,total_value_ARAP, total_value_ACCR"
                )
        )

    logger.info("Process completed using Apache-beam Framework")


if __name__ == "__main__":
    main()
