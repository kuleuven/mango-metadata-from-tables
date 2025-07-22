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
        # testcase for building a path based on other columns
        # csv containing a number of columns to build the path from
        "input_file": "testdata/testdata_path_from_columns.csv",
        "configuration_file": "testdata/testdata_path_from_columns.csv.yml",
        "intended_results_file": "testdata/testdata_path_from_columns.csv.json",
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
    sheets = process_file(input_file, session=None)
    for sheetname, sheet in sheets.items():
        for data_object, md_dict in metadata_from_tabular.generate_rows(sheet):
            results[data_object] = md_dict

    with open(intended_results_file, "r") as f:
        intended_results = json.load(f)
    assert intended_results == results
