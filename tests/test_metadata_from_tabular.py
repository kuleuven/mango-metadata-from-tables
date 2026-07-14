import io

from irods.meta import iRODSMeta
from pytest_cases import fixture, parametrize_with_cases
import pathlib
import pytest

import mango_metadata_from_tables.run as metadata_from_tabular
import mango_metadata_from_tables.preprocessing as preprocessing


@fixture
@parametrize_with_cases("input_file,config,expected_output", prefix="case_")
def avus(
    input_file: str, config: io.StringIO, expected_output: list[iRODSMeta]
) -> tuple:
    """
    Based on a pair of path-to-tabular-file and a StringIO object
    representing the config YAML, generate a dictionary with
    absolute paths as keys and a list of AVUs as values.
    """
    results = {
        result["dataobject"]: result["avus"]
        for result in metadata_from_tabular.apply_metadata_from_table(
            input_file, config, dry_run=True
        )
    }
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


@fixture
def irods_session():
    from irods.helpers import make_session

    session = make_session()
    assert session.zone == "icts"
    assert session.collections.exists(
        "/icts/home/datateam_icts_icts_quality/mango-metadata-from-tables"
    )
    yield session
    session.cleanup()


@fixture
@parametrize_with_cases("input_file,config,expected_output", prefix="case_")
def irods_objects(input_file, config, expected_output, irods_session):
    created_subcollections = []
    for path in expected_output.keys():
        parent = str(pathlib.Path(path).parent)
        if not irods_session.collections.exists(parent):
            irods_session.collections.create(parent, recurse=True)
        irods_session.data_objects.create(path)
    yield input_file, config, expected_output
    for path in expected_output.keys():
        irods_session.data_objects.unlink(path)  # review
        for subcollection in created_subcollections:
            irods_session.collections.remove(subcollection)


def test_irods(irods_objects, irods_session, subtests):
    input_path, config, expected_output = irods_objects
    results = metadata_from_tabular.apply_metadata_from_table(
        input_path, config, session=irods_session
    )
    for result in results:
        dataobject = result["dataobject"]
        assert dataobject in expected_output
        avus = irods_session.data_objects.get(dataobject).metadata.items()
        for avu in expected_output[dataobject]:
            with subtests.test(avu=avu):
                assert avu in avus
