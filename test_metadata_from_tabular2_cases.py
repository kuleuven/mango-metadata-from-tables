import io

import yaml
from pytest_cases import parametrize


basic_examples = [
    {"input_file": "testdata/testdata.csv", "config": {"separator": ";"}},
    {"input_file": "testdata/testdata.xlsx", "config": {"sheets": ["Tabelle1"]}},
]

default_config = {
    "path_column": {"column_name": "dataobject", "path_type": "absolute"},
    "separator": ",",
    "sheets": ["single_sheet"],
}


def config_dict_to_yaml(config_dict: dict) -> io.StringIO:
    config = {k: v for k, v in default_config.items()}  # copy default
    config |= config_dict  # update with custom
    return io.StringIO(yaml.dump(config))


@parametrize("mapping", basic_examples)
def case_basic(mapping):
    config_as_file = config_dict_to_yaml(mapping.get("config", {}))
    return mapping["input_file"], config_as_file


def case_multiple_sheets():
    config_as_file = config_dict_to_yaml({"sheets": ["Tabelle1", "Sheet1"]})
    return "testdata/testdata_multiple_sheets.xlsx", config_as_file


@parametrize(
    "path",
    [
        "testdata/test-1.0.0-published.json",
        "testdata/test-2.0.0-published.json",
        "file_does_not_exist",
    ],
)
@parametrize("exclude_other_metadata", [True, False])
@parametrize("ignore_invalid_schema_metadata", [True, False])
def case_schema_metadata(path, exclude_other_metadata, ignore_invalid_schema_metadata):
    input_file = "testdata/testdata.csv"
    custom_config = {
        "separator": ";",
        "mango_schema": {
            "path": path,
            "exclude_other_metadata": exclude_other_metadata,
            "ignore_invalid_schema_metadata": ignore_invalid_schema_metadata,
        },
    }
    return input_file, config_dict_to_yaml(custom_config)
