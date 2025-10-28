import io

from irods.meta import iRODSMeta
from pytest_cases import fixture, parametrize_with_cases
import pytest

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


@fixture
def pattern_path_metadata():
    main_coll = "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables"
    results = {
        "testdata/testdata_path_from_columns.csv": [
            {"path": "small_shapes/a_green_star.jpg", "shape": "star"},
            {"path": "big_shapes/a_red_heart.jpg", "shape": "heart"},
        ],
        "testdata/testdata_path_from_columns_with_filters.csv": [
            {
                "path": "small_shapes/star_06121992.jpg",
                "shape": "staR",
                "date": "06/12/1992",
            },
            {
                "path": "big_shapes/heart_05072010.jpg",
                "shape": "HEART",
                "date": "05/07/2010",
            },
        ],
    }
    avus_0 = [
        iRODSMeta("size", "small"),
        iRODSMeta("color", "green"),
    ]
    avus_1 = [
        iRODSMeta("size", "big"),
        iRODSMeta("color", "red"),
    ]

    def get_paths_and_avus(items):
        path_0 = items[0].pop("path")
        path_1 = items[1].pop("path")
        return {
            f"{main_coll}/{path_0}": avus_0
            + [iRODSMeta(k, v) for k, v in items[0].items()],
            f"{main_coll}/{path_1}": avus_1
            + [iRODSMeta(k, v) for k, v in items[1].items()],
        }

    return {k: get_paths_and_avus(v) for k, v in results.items()}


def get_schema_version(version: int) -> iRODSMeta:
    return iRODSMeta("mgs.test.__version__", f"{version}.0.0")


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


def multiple_values(metadata: dict) -> dict:
    """Add the AVUs of the multiple values case."""
    md_copy = {
        dataobject: [avu for avu in avu_list]
        for dataobject, avu_list in metadata.items()
    }
    jane_doe = iRODSMeta("author", "Jane Doe")
    john_doe = iRODSMeta("author", "John Doe")
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file1.txt"
    ] += [john_doe, jane_doe]
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file2.txt"
    ] += [john_doe]
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file3.txt"
    ] += [jane_doe]
    return md_copy


def multiple_values_multiple_sheets(metadata: dict) -> dict:
    """Add the avus for the multiple values multiple sheets case"""
    md_copy = multiple_values(metadata)

    # adding metadata for second sheet
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file4.txt"
    ] = [
        iRODSMeta("color", "purple"),
        iRODSMeta("size", "big"),
        iRODSMeta("shape", "irregular"),
        iRODSMeta("author", "Jane Doe"),
        iRODSMeta("author", "Fulano De Tal"),
        iRODSMeta(
            "description", "This is a text with a colon ; but it should not be split"
        ),
    ]
    return md_copy


@fixture
@parametrize_with_cases("input_file,config", prefix="case_")
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


def test_avus(avus, basic_metadata, pattern_path_metadata, current_cases):
    case_id, case_fun, config = current_cases["avus"]["config"]

    if case_id == "basic":
        expected_output = basic_metadata
    elif case_id in ["blacklist", "whitelist"]:
        """Both the blacklist and whitelist test exclude the column 'color,
        either by blacklisting it, or whitelisting all other columns"""
        expected_output = {
            dataobject: [avu for avu in list_of_avus if avu.name != "color"]
            for dataobject, list_of_avus in basic_metadata.items()
        }
    elif case_id == "multiple_sheets":
        expected_output = multiple_sheets_metadata(basic_metadata)
    elif case_id == "multiple_values":
        expected_output = multiple_values(basic_metadata)
    elif case_id == "multiple_values_multiple_sheets":
        expected_output = multiple_values_multiple_sheets(basic_metadata)
    elif case_id == "schema_metadata":
        if config["path"] == "file_does_not_exist":
            # case: there is no valid schema
            expected_output = basic_metadata
        elif config["path"] == "testdata/test-1.0.0-published.json":
            # case: the valid schema matches all data
            expected_output = {
                dataobject: namespace_all_metadata(list_of_avus)
                + [get_schema_version(1)]
                for dataobject, list_of_avus in basic_metadata.items()
            }
        else:
            # case: partial-match schema
            expected_output = {
                dataobject: namespace_partial_metadata(list_of_avus)
                + [get_schema_version(2)]
                for dataobject, list_of_avus in basic_metadata.items()
            }
            if config[metadata_from_tabular.EXCLUDE_INVALID_SCHEMA_MD]:
                # cases: invalid schema metadata is excluded
                expected_output = {
                    dataobject: [avu for avu in list_of_avus if avu.name != "size"]
                    for dataobject, list_of_avus in expected_output.items()
                }
            if config[metadata_from_tabular.EXCLUDE_NONSCHEMA_MD]:
                # cases: non-schema metadata is excluded
                expected_output = {
                    dataobject: [
                        avu
                        for avu in list_of_avus
                        if avu.name.startswith("mgs") or avu.name == "size"
                    ]
                    for dataobject, list_of_avus in expected_output.items()
                }
    elif case_id == "path_from_columns":
        expected_output = pattern_path_metadata[config["mapping"]["input_file"]]

    for data_object, list_of_avus in avus.items():
        # sort arrays to make sure the equivalence works
        list_of_avus.sort(key=lambda x: x.name)
        expected_output[data_object].sort(key=lambda x: x.name)
        assert list_of_avus == expected_output[data_object]


@parametrize_with_cases("input_file,config,err_type,err_msg", prefix="error")
def test_exceptions(input_file: str, config: io.StringIO, err_type, err_msg: str):
    process_file = metadata_from_tabular.apply_config(config)
    processed_config_data = process_file(input_file, session=None)
    sheets = processed_config_data["sheets"]
    with pytest.raises(err_type, match=err_msg):
        metadata_from_tabular.validate_schema_columns(
            sheets, processed_config_data["schema_instructions"].get("schema", None)
        )
