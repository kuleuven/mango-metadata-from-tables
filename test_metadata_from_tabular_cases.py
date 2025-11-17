import io
import yaml

from irods.meta import iRODSMeta
from pytest_cases import parametrize


basic_examples = [
    {"input_file": "testdata/testdata.csv", "config": {"separator": ";"}},
    {"input_file": "testdata/testdata.xlsx", "config": {"sheets": ["Tabelle1"]}},
    {
        "input_file": "testdata/testdata_relative_path.xlsx",
        "config": {
            "path_column": {
                "column_name": "file",
                "path_type": "relative",
                "workdir": "/icts/home/datateam_icts_icts_test",
            },
            "sheets": ["Tabelle1"],
        },
    },
]

# this could be expanded to test other filters eventually
path_from_columns_examples = [
    {
        "id": "basic",
        "input_file": "testdata/testdata_path_from_columns.csv",
        "pattern": "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/{{size}}_shapes/a_{{color}}_{{shape}}.jpg",
    },
    {
        "id": "date_filter",
        "input_file": "testdata/testdata_path_from_columns_with_filters.csv",
        "pattern": (
            "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/"
            "{{ size }}_shapes/{{ shape|lower }}_{{ date|date_format(input_format='%d/%m/%Y',output_format='%d%m%Y')}}.jpg"
        ),
    },
]


default_config = {
    "path_column": {"column_name": "dataobject", "path_type": "absolute"},
    "separator": ",",
    "sheets": ["single_sheet"],
}

basic_metadata = {
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

pattern_path_output = {
    "basic": {
        "small_shapes/a_green_star.jpg": [
            iRODSMeta("size", "small"),
            iRODSMeta("color", "green"),
            iRODSMeta("shape", "star"),
        ],
        "big_shapes/a_red_heart.jpg": [
            iRODSMeta("size", "big"),
            iRODSMeta("color", "red"),
            iRODSMeta("shape", "heart"),
        ],
    },
    "date_filter": {
        "small_shapes/star_06121992.jpg": [
            iRODSMeta("size", "small"),
            iRODSMeta("color", "green"),
            iRODSMeta("shape", "staR"),
            iRODSMeta("date", "06/12/1992"),
        ],
        "big_shapes/heart_05072010.jpg": [
            iRODSMeta("size", "big"),
            iRODSMeta("color", "red"),
            iRODSMeta("shape", "HEART"),
            iRODSMeta("date", "05/07/2010"),
        ],
    },
}


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


def config_dict_to_yaml(config_dict: dict) -> io.StringIO:
    config = {k: v for k, v in default_config.items()}  # copy default
    config.update(config_dict)  # update with specific config for this testcase
    return io.StringIO(yaml.dump(config))


@parametrize("mapping", basic_examples, idgen=lambda mapping: mapping["input_file"])
def case_basic(mapping):
    config_as_file = config_dict_to_yaml(mapping.get("config", {}))
    return mapping["input_file"], config_as_file, basic_metadata


def case_blacklist():
    config_as_file = config_dict_to_yaml({"separator": ";", "blacklist": ["color"]})
    expected_output = {
        dataobject: [avu for avu in list_of_avus if avu.name != "color"]
        for dataobject, list_of_avus in basic_metadata.items()
    }
    return "testdata/testdata.csv", config_as_file, expected_output


def case_whitelist():
    config_as_file = config_dict_to_yaml(
        {"separator": ";", "whitelist": ["shape", "size"]}
    )
    expected_output = {
        dataobject: [avu for avu in list_of_avus if avu.name != "color"]
        for dataobject, list_of_avus in basic_metadata.items()
    }
    return "testdata/testdata.csv", config_as_file, expected_output


def case_multiple_sheets():
    config_as_file = config_dict_to_yaml({"sheets": ["Tabelle1", "Sheet1"]})
    return (
        "testdata/testdata_multiple_sheets.xlsx",
        config_as_file,
        multiple_sheets_metadata(basic_metadata),
    )


def case_multiple_values():
    config_as_file = config_dict_to_yaml(
        {"multivalue_columns": ["author"], "multivalue_separator": ";"}
    )
    return (
        "testdata/testdata_multiple_values.csv",
        config_as_file,
        multiple_values(basic_metadata),
    )


def case_multiple_values_multiple_sheets():
    config_as_file = config_dict_to_yaml(
        {
            "multivalue_columns": ["author"],
            "multivalue_separator": ";",
            "sheets": ["Sheet1", "Sheet2"],
        }
    )
    return (
        "testdata/testdata_multiple_values_multiple_sheets.xlsx",
        config_as_file,
        multiple_values_multiple_sheets(basic_metadata),
    )


@parametrize("mapping", path_from_columns_examples, idgen=lambda mapping: mapping["id"])
def case_path_from_columns(mapping):
    config_as_file = config_dict_to_yaml(
        {
            "path_column": {
                "path_type": "pattern",
                "column_name": None,
                "pattern": mapping["pattern"],
            }
        }
    )
    main_coll = "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables"
    # apply collection name to data object paths
    expected_output = {
        f"{main_coll}/{fname}": avu_list
        for fname, avu_list in pattern_path_output[mapping["id"]].items()
    }
    return mapping["input_file"], config_as_file, expected_output


@parametrize(
    "path",
    [
        "testdata/test-1.0.0-published.json",
        "testdata/test-2.0.0-published.json",
        "file_does_not_exist",
    ],
)
@parametrize("exclude_non_schema_metadata", [True, False])
@parametrize("exclude_invalid_schema_metadata", [True, False])
def case_schema_metadata(
    path, exclude_non_schema_metadata, exclude_invalid_schema_metadata
):
    input_file = "testdata/testdata.csv"
    custom_config = {
        "separator": ";",
        "mango_schema": {
            "path": path,
            "exclude_non_schema_metadata": exclude_non_schema_metadata,
            "exclude_invalid_schema_metadata": exclude_invalid_schema_metadata,
        },
    }
    if path == "file_does_not_exist":
        # case: there is no valid schema
        expected_output = basic_metadata
    elif path == "testdata/test-1.0.0-published.json":
        # case: the valid schema matches all data
        expected_output = {
            dataobject: namespace_all_metadata(list_of_avus) + [get_schema_version(1)]
            for dataobject, list_of_avus in basic_metadata.items()
        }
    else:
        # case: partial-match schema
        expected_output = {
            dataobject: namespace_partial_metadata(list_of_avus)
            + [get_schema_version(2)]
            for dataobject, list_of_avus in basic_metadata.items()
        }
        if exclude_invalid_schema_metadata:
            # cases: invalid schema metadata is excluded
            expected_output = {
                dataobject: [avu for avu in list_of_avus if avu.name != "size"]
                for dataobject, list_of_avus in expected_output.items()
            }
        if exclude_non_schema_metadata:
            # cases: non-schema metadata is excluded
            expected_output = {
                dataobject: [
                    avu
                    for avu in list_of_avus
                    if avu.name.startswith("mgs") or avu.name == "size"
                ]
                for dataobject, list_of_avus in expected_output.items()
            }
    return input_file, config_dict_to_yaml(custom_config), expected_output


# @todo add tests for errors!
def error_schema_metadata():
    input_file = "testdata/testdata_missing_column.csv"
    custom_config = {
        "separator": ";",
        "mango_schema": {
            "path": "testdata/test-1.0.0-published.json",
            "exclude_non_schema_metadata": True,
            "exclude_invalid_schema_metadata": True,
        },
    }
    err_msg = "None of the sheets contain all the required fields of the schema."
    return input_file, config_dict_to_yaml(custom_config), KeyError, err_msg
