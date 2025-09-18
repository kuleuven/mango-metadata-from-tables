import io

from irods.meta import iRODSMeta
from pytest_cases import fixture, parametrize_with_cases

import metadata_from_tabular


@fixture
def basic_metadata():
    """Basic expected output, to be adapted based on configuration changes."""
    return {
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file1.txt": [
            iRODSMeta("size", "small"),
            iRODSMeta("color", "green"),
            iRODSMeta("shape", "star"),
        ],
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file2.txt": [
            iRODSMeta("size", "medium"),
            iRODSMeta("color", "red"),
            iRODSMeta("shape", "heart"),
        ],
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file3.txt": [
            iRODSMeta("size", "big"),
            iRODSMeta("color", "blue"),
            iRODSMeta("shape", "square"),
        ],
    }


def namespace_metadata(avu: iRODSMeta) -> iRODSMeta:
    """Turn an AVU into its schema counterpart ('test' schema)."""
    return iRODSMeta(f"mgs.test.{avu.name}", avu.value)


def namespace_all_metadata(avu_list: list[iRODSMeta]) -> list[iRODSMeta]:
    """Turn all the metadata in a list of AVUs into their schema counterparts."""
    return [namespace_metadata(avu) for avu in avu_list]


def namespace_partial_metadata(avu_list: list[iRODSMeta]) -> list[iRODSMeta]:
    """Only turn metadata into schema metadata if it fits the v2.0.0 schema."""

    def filter_metadata(avu):
        if avu.name == "color" or (
            avu.name == "size" and avu.value in ["small", "big"]
        ):
            return namespace_metadata(avu)
        return avu

    return [filter_metadata(avu) for avu in avu_list]


def multiple_sheets_metadata(metadata: dict) -> dict:
    """Add the AVU of the multiple sheets case."""
    md_copy = {
        dataobject: [avu for avu in avu_list]
        for dataobject, avu_list in metadata.items()
    }
    new_md = iRODSMeta("vibe", "like a forest on a sunny day")
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file3.txt"
    ].append(new_md)
    return md_copy


@fixture
@parametrize_with_cases("input_file,config")
def avus(input_file: str, config: io.StringIO) -> dict:
    """
    Based on a pair of path-to-tabular-file and a StringIO object
    representing the config YAML, generate a dictionary with
    absolute paths as keys and a list of AVUs as values.
    """
    process_file = metadata_from_tabular.apply_config(config)
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
    return results


def test_avus(avus, basic_metadata, current_cases):
    case_id, case_fun, config = current_cases["avus"]["config"]

    if case_id == "basic":
        expected_output = basic_metadata
    elif case_id == "blacklist":
        expected_output = {
            dataobject: [avu for avu in list_of_avus if avu.name != "color"]
            for dataobject, list_of_avus in basic_metadata.items()
        }
    elif case_id == "multiple_sheets":
        expected_output = multiple_sheets_metadata(basic_metadata)
    elif case_id == "schema_metadata":
        if config["path"] == "file_does_not_exist":
            # case: there is no valid schema
            expected_output = basic_metadata
        elif config["path"] == "testdata/test-1.0.0-published.json":
            # case: the valid schema matches all data
            expected_output = {
                dataobject: namespace_all_metadata(list_of_avus)
                for dataobject, list_of_avus in basic_metadata.items()
            }
        else:
            # case: partial-match schema
            expected_output = {
                dataobject: namespace_partial_metadata(list_of_avus)
                for dataobject, list_of_avus in basic_metadata.items()
            }
            if config["exclude_other_metadata"]:
                # case: non-schema metadata is excluded
                expected_output = {
                    dataobject: [
                        avu for avu in list_of_avus if avu.name.startswith("mgs")
                    ]
                    for dataobject, list_of_avus in expected_output.items()
                }
            elif config["ignore_invalid_schema_metadata"]:
                # case: only invalid schema metadata is excluded
                expected_output = {
                    dataobject: [avu for avu in list_of_avus if avu.name != "size"]
                    for dataobject, list_of_avus in expected_output.items()
                }

    for data_object, list_of_avus in avus.items():
        # sort arrays to make sure the equivalence works
        list_of_avus.sort(key=lambda x: x.name)
        expected_output[data_object].sort(key=lambda x: x.name)
        assert list_of_avus == expected_output[data_object]
