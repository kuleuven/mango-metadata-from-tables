import io

from irods.meta import iRODSMeta
from pytest_cases import fixture, parametrize_with_cases
import pytest

import mango_metadata_from_tables.run as metadata_from_tabular
import mango_metadata_from_tables.preprocessing as preprocessing


@fixture
@parametrize_with_cases("input_file,config,expected_output", prefix="case_")
def avus(
    input_file: str, config: io.StringIO, expected_output: list[iRODSMeta]
) -> dict:
    """
    Based on a pair of path-to-tabular-file and a StringIO object
    representing the config YAML, generate a dictionary with
    absolute paths as keys and a list of AVUs as values.
    """
    process_file = preprocessing.apply_config(config)
    processed_config_data = process_file(input_file, session=None)
    sheets = processed_config_data["sheets"]
    multivalue_columns = processed_config_data["multivalue_columns"]
    multivalue_separator = processed_config_data["multivalue_separator"]
    schema_instructions = processed_config_data["schema_instructions"]

    results = {}
    for _, sheet in sheets.items():
        for data_object, md_dict in metadata_from_tabular.generate_rows(
            sheet, multivalue_columns, multivalue_separator
        ):
            results[data_object] = metadata_from_tabular.dict_to_avus(
                md_dict, **schema_instructions
            )
    return results, expected_output


def test_avus(avus):
    results, expected_output = avus
    for data_object, list_of_avus in results.items():
        # sort arrays to make sure the equivalence works
        list_of_avus.sort(key=lambda x: x.name)
        expected_output[data_object].sort(key=lambda x: x.name)
        assert list_of_avus == expected_output[data_object]


@parametrize_with_cases("input_file,config,err_type,err_msg", prefix="error")
def test_exceptions(input_file: str, config: io.StringIO, err_type, err_msg: str):
    process_file = preprocessing.apply_config(config)
    processed_config_data = process_file(input_file, session=None)
    sheets = processed_config_data["sheets"]
    with pytest.raises(err_type, match=err_msg):
        preprocessing.validate_schema_columns(
            sheets, processed_config_data["schema_instructions"].get("schema", None)
        )
