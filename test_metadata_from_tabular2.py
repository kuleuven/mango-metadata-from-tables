from pytest_cases import parametrize_with_cases, fixture
from irods.meta import iRODSMeta
import metadata_from_tabular


@fixture
def basic_metadata():
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
    return iRODSMeta(f"mgs.test.{avu.name}", avu.value)


def namespace_all_metadata(avu_list: list[iRODSMeta]) -> list[iRODSMeta]:
    return [namespace_metadata(avu) for avu in avu_list]


def namespace_partial_metadata(avu_list: list[iRODSMeta]) -> list[iRODSMeta]:
    def filter_metadata(avu):
        if avu.name == "color" or (
            avu.name == "size" and avu.value in ["small", "big"]
        ):
            return namespace_metadata(avu)
        return avu

    return [filter_metadata(avu) for avu in avu_list]


def multiple_sheets_metadata(metadata: dict) -> dict:
    md_copy = {k: [vv for vv in v] for k, v in metadata.items()}
    new_md = iRODSMeta("vibe", "like a forest on a sunny day")
    md_copy[
        "/icts/home/datateam_icts_icts_test/mango-metadata-from-tables/file3.txt"
    ].append(new_md)
    return md_copy


@fixture
@parametrize_with_cases("input_file,config")
def avus(input_file, config):
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
    avus_id, avus_fun, avus_params = current_cases["avus"]["config"]
    expected_output = None
    if avus_id == "basic":
        expected_output = basic_metadata
    elif avus_id == "multiple_sheets":
        expected_output = multiple_sheets_metadata(basic_metadata)
    elif avus_id == "schema_metadata":
        if avus_params["path"] == "file_does_not_exist":
            # case: there is no valid schema
            expected_output = basic_metadata
        elif avus_params["path"] == "testdata/test-1.0.0-published.json":
            # case: the valid schema matches all data
            expected_output = {
                k: namespace_all_metadata(v) for k, v in basic_metadata.items()
            }
        else:
            # case: partial-match schema
            expected_output = {
                k: namespace_partial_metadata(v) for k, v in basic_metadata.items()
            }
            if avus_params["exclude_other_metadata"]:
                # case: non-schema metadata is excluded
                expected_output = {
                    k: [vv for vv in v if vv.name.startswith("mgs")]
                    for k, v in expected_output.items()
                }
            elif avus_params["ignore_invalid_schema_metadata"]:
                # case: only invalid schema metadata is excluded
                expected_output = {
                    k: [vv for vv in v if vv.name != "size"]
                    for k, v in expected_output.items()
                }

    if expected_output is not None:
        for k, v in avus.items():
            # sort arrays to make sure the equivalence works
            v.sort(key=lambda x: x.name)
            expected_output[k].sort(key=lambda x: x.name)
            assert v == expected_output[k]
