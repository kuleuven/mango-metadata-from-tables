"""Tests for the module 'excels2metadata'"""

import json
import pytest
import yaml
import metadata_from_tabular

# define test-cases


testcases = [
    {
        # csv with one sheet containing absolute paths and ; separator
        "input_file": "testdata/testdata.csv",
        "configuration_file": "testdata/testdata.csv.yml",
        "intended_results_file": "testdata/testdata.csv.json",
    },
    {
        # excel with one sheet containing absolute paths
        "input_file": "testdata/testdata.xlsx",
        "configuration_file": "testdata/testdata.xlsx.yml",
        "intended_results_file": "testdata/testdata.xlsx.json",
    },
    {
        # excel with multiple sheets containing absolute paths
        "input_file": "testdata/testdata_multiple_sheets.xlsx",
        "configuration_file": "testdata/testdata_multiple_sheets.xlsx.yml",
        "intended_results_file": "testdata/testdata_multiple_sheets.xlsx.json",
    },
    {
        # excel with one sheet containing relative paths
        "input_file": "testdata/testdata_relative_path.xlsx",
        "configuration_file": "testdata/testdata_relative_path.xlsx.yml",
        "intended_results_file": "testdata/testdata_relative_path.xlsx.json",
    },
    {
        # testcase for blacklisting
        # excel with one sheet containing absolute paths;
        # the column 'shape' is blacklisted
        "input_file": "testdata/testdata.xlsx",
        "configuration_file": "testdata/testdata.xlsx_blacklisted.yml",
        "intended_results_file": "testdata/testdata.xlsx_blacklisted.json",
    },
    {
        # testcase for whitelisting
        # excel with one sheet containing absolute paths;
        # the columns 'color' and 'shape' are whitelisted
        "input_file": "testdata/testdata.xlsx",
        "configuration_file": "testdata/testdata.xlsx_whitelisted.yml",
        "intended_results_file": "testdata/testdata.xlsx_whitelisted.json",
    },
    {
        # testcase for columns with multiple values
        # csv containing absolute paths
        # the column 'author' contains multiple values in some rows
        "input_file": "testdata/testdata_multiple_values.csv",
        "configuration_file": "testdata/testdata_multiple_values.csv.yml",
        "intended_results_file": "testdata/testdata_multiple_values.csv.json",
    },
    {
        # testcase for columns with multiple values
        # xlsx containing two sheets with absolute paths
        # the column 'author' is present in both sheets and contains multiple values in some rows,
        # split with ;.
        # the column 'description' has ; in it, but should not be split
        "input_file": "testdata/testdata_multiple_values_multiple_sheets.xlsx",
        "configuration_file": "testdata/testdata_multiple_values_multiple_sheets.xlsx.yml",
        "intended_results_file": "testdata/testdata_multiple_values_multiple_sheets.xlsx.json",
    },
]


@pytest.mark.parametrize("testcase", testcases)
def test_parse_inputfile_with_single_sheet(testcase):
    """Tests whether a given input file configuration lead to the right paths and metadata"""

    results = {}
    input_file = testcase["input_file"]
    configuration_file = testcase["configuration_file"]
    intended_results_file = testcase["intended_results_file"]

    print(
        f"Testing flow for input file {input_file} with configuration {configuration_file}"
    )
    with open(configuration_file) as config:
        # needed to open configuration as file-like object
        process_file = metadata_from_tabular.apply_config(config)
        config.seek(0)
        yml = yaml.safe_load(config)
        multivalue_columns = yml.get("multivalue_columns") or []
        multivalue_separator = yml.get("multivalue_separator") or ""
    sheets = process_file(input_file, session=None)
    for sheetname, sheet in sheets.items():
        for data_object, md_list in metadata_from_tabular.generate_rows(
            sheet, multivalue_columns, multivalue_separator
        ):
            results[data_object] = md_list

    with open(intended_results_file, "r") as f:
        intended_results = json.load(f)
    assert intended_results == results
