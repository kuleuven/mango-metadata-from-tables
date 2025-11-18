import pandas as pd
from irods.session import iRODSSession
from mango_mdschema import Schema
from irods.meta import iRODSMeta, AVUOperation
from collections.abc import Generator
from . import console, DATAOBJECT


def unlist_value(value: list, field) -> str | int:
    """Unlist values for non repeatable fields"""
    if len(value) == 1 and not field.repeatable:
        return value[0]


def dict_to_avus(
    row: dict,
    schema: Schema = None,
    exclude_non_schema_metadata: bool = True,
    exclude_invalid_schema_metadata: bool = False,
) -> list[iRODSMeta]:
    """Convert a dictionary of metadata name-value pairs into a list of iRODSMeta"""
    if schema is not None:
        dict_to_validate = {
            k: unlist_value(v, schema.fields[k])
            for k, v in row.items()
            if k in schema.fields
        }
        valid_schema_metadata = schema.validate(
            dict_to_validate
        )  # dict with metadata that passed the schema
        for k, v in valid_schema_metadata.items():
            if v is None:
                line1 = f"Found a value '{dict_to_validate[k]}' of column '{k}' that does not match the schema."
                line2 = (
                    "It will be excluded."
                    if exclude_invalid_schema_metadata
                    else "It will be added as non-schema metadata."
                )
                console.print(f"{line1} {line2}")
        schema_avus = schema.to_avus(valid_schema_metadata)
        if len(schema_avus) > 0:
            schema_avus += [
                iRODSMeta(f"{schema.prefix}.{schema.name}.__version__", schema.version)
            ]

        # create empty dict if other metadata is ignored; otherwise dict of metadata that did not pass
        def is_invalid_schema_metadata(k):
            return k in dict_to_validate and valid_schema_metadata.get(k, None) is None

        def is_nonschema_metadata(k):
            return k not in schema.fields

        invalid_schema_metadata = (
            {}
            if exclude_invalid_schema_metadata
            else {k: v for k, v in row.items() if is_invalid_schema_metadata(k)}
        )
        nonschema_metadata = (
            {}
            if exclude_non_schema_metadata
            else {k: v for k, v in row.items() if is_nonschema_metadata(k)}
        )

        other_metadata = {**nonschema_metadata, **invalid_schema_metadata}
    else:  # if there is no schema
        schema_avus = []  # no schema metadata
        other_metadata = row  # all metadata

    non_schema_avus = [
        iRODSMeta(str(key), str(value_item))
        for key, value in other_metadata.items()  # empty if all metadata is from schema or the other metadata is ignored
        for value_item in value
        if not pd.isna(value_item)
    ]
    return schema_avus + non_schema_avus


def generate_rows(
    dataframe: pd.DataFrame, multivalue_columns: list, multivalue_separator: str
) -> Generator[tuple]:
    """Yield a tuple of filename and metadata-dictionary from a dataframe"""
    for _, row in dataframe.iterrows():
        md_dict = {}
        for k, v in row.items():
            if k != DATAOBJECT:
                if k in multivalue_columns and isinstance(v, str):
                    md_dict[k] = [
                        val.strip()
                        for val in v.split(multivalue_separator)
                        if val.strip()
                    ]
                else:
                    md_dict[k] = [v]
        yield (row[DATAOBJECT], md_dict)


def apply_metadata_to_data_object(
    path: str, avu_dict: dict, schema_instructions: dict, session: iRODSSession
):
    """Add metadata from a dictionary to a given data object"""
    try:
        obj = session.data_objects.get(path)
        avus = dict_to_avus(avu_dict, **schema_instructions)
        obj.metadata.apply_atomic_operations(
            *[AVUOperation(operation="add", avu=item) for item in avus]
        )
        return len(avus)
    except Exception as e:
        print(e)
        return 0
