import io

from irods.meta import iRODSMeta
from pytest_cases import fixture, parametrize_with_cases
import pathlib
import pytest

from mango_metadata_from_tables import ItemType
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
    processed_config_data = preprocessing.process_tabular_file(
        input_file, config, session=None
    )
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
def irods_objects(input_file, config, expected_output, irods_session, current_cases):
    is_object = "collection" not in current_cases["irods_objects"]["config"].id
    created_subcollections = []
    for path in expected_output.keys():
        if is_object:
            parent = str(pathlib.Path(path).parent)
            if not irods_session.collections.exists(parent):
                irods_session.collections.create(parent, recurse=True)
            irods_session.data_objects.create(path)
        else:
            irods_session.collections.create(path, recurse=True)
    yield input_file, config, expected_output, is_object
    for path in expected_output.keys():
        if is_object:
            irods_session.data_objects.unlink(path, force=True)  # review
            for subcollection in created_subcollections:
                irods_session.collections.remove(subcollection, force=True)
        else:
            irods_session.collections.remove(path, force=True)


def test_irods(irods_objects, irods_session, subtests):
    input_path, config, expected_output, is_object = irods_objects
    results = metadata_from_tabular.apply_metadata_from_table(
        input_path, config, session=irods_session
    )
    manager = irods_session.data_objects if is_object else irods_session.collections
    for result in results:
        dataobject = result["dataobject"]
        assert dataobject in expected_output
        avus = manager.get(dataobject).metadata.items()
        for avu in expected_output[dataobject]:
            with subtests.test(avu=avu):
                assert avu in avus
