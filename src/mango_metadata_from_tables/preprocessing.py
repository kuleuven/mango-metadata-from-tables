import os
import pandas as pd
import click
import jinja2
import datetime
from pathlib import Path
from irods.session import iRODSSession
from irods.column import Criterion
from irods.models import Collection, DataObject
from . import DATAOBJECT
import yaml
from mango_mdschema import Schema
from .read_table import parse_tabular_file
from . import DATAOBJECT, EXCLUDE_NONSCHEMA_MD, EXCLUDE_INVALID_SCHEMA_MD, console


def search_objects_with_identifier(session, workingdirectory, identifier, exact_match):
    """Searches a given project for objects starting with a certain identifier


    Arguments
    ---------
    session: obj
        An iRODSSession object

    workingdirectory: str
        Path to the collection in iRODS

    identifier: str
        The identifier you want to search for

    Returns
    -------
    paths: str
        A list of data object paths matching the identifier
    """

    operator = "=" if exact_match else "like"
    query = (
        session.query(DataObject.name, Collection.name)
        .filter(Criterion("like", Collection.name, workingdirectory + "%"))
        .filter(Criterion(operator, DataObject.name, identifier + "%"))
    )
    paths = [f"{result[Collection.name]}/{result[DataObject.name]}" for result in query]
    return paths


def query_dataobjects_with_filename(
    session, df, filename_column, workingdirectory, exact_match=True
):
    """
    Queries data objects in iRODS based on identifiers in the dataframe,
    and creates a row for each result with the accompanying metadata.
    """

    new_rows = []
    for index, identifier in enumerate(df[filename_column]):
        paths = search_objects_with_identifier(
            session, workingdirectory, identifier, exact_match
        )
        for path in paths:
            new_row = df.iloc[index].drop(filename_column)
            new_row[DATAOBJECT] = path
            # create a 1 row dataframe, which needs to be transposed (hence the T)
            new_rows.append(new_row.to_frame().T)
    if len(new_rows) > 0:
        new_df = pd.concat(new_rows, ignore_index=True)
    else:
        columns = [column for column in df.columns if column != filename_column]
        columns.append(DATAOBJECT)
        new_df = pd.DataFrame(columns=columns)
    return new_df


def render_single_path_from_pattern(
    row: pd.Series, pattern: str, env: jinja2.Environment
) -> str:
    """Render a path from a pattern using a single DataFrame row."""

    path_template = env.from_string(pattern)
    return path_template.render(row.to_dict())


def create_path_based_on_pattern(
    df: pd.DataFrame, pattern: str, env: jinja2.Environment
):
    """Create a column for data object paths based on info from other columns"""

    path_template = env.from_string(pattern)
    constructed_paths = [
        path_template.render(row.to_dict()) for _, row in df.iterrows()
    ]
    df[DATAOBJECT] = constructed_paths
    return df


def chain_collection_and_filename(
    df: pd.DataFrame, filename_column: str, workingdirectory: str
):
    """Renames the column with the relative data object path and completes it with the collection path"""
    df = df.rename(columns={filename_column: DATAOBJECT})
    df[DATAOBJECT] = [str(Path(workingdirectory) / Path(x)) for x in df[DATAOBJECT]]
    return df


# filters for creating patterns


def date_format(value, input_format="%d-%m-%Y", output_format="%Y-%m-%d"):
    """
    Return a date in the specified output format

    If the value is a datetime object, it will be converted immediately.
    If the value is a string, it will be converted to a datetime object
    according to the input format, and then converted to the output format.
    """

    if isinstance(value, str):
        value = datetime.datetime.strptime(value, input_format)
    # If already a datetime object, skip conversion
    return value.strftime(output_format)


def create_jinja_environment_with_filters():
    jinja_environment = jinja2.Environment()
    jinja_environment.filters["date_format"] = date_format
    return jinja_environment


def validate_schema_columns(sheets: dict[pd.DataFrame], schema: Schema) -> list[str]:
    if schema is None:
        return list(sheets.keys())

    required_fields = [
        field_name
        for field_name, field in schema.fields.items()
        if field.required and not field.default
    ]
    sheets_for_schema = [
        sheetname
        for sheetname, sheet in sheets.items()
        if all(field_name in sheet.columns for field_name in required_fields)
    ]
    if len(sheets_for_schema) == 0:
        raise KeyError(
            "None of the sheets contain all the required fields of the schema."
        )
    return sheets_for_schema


def apply_config(config: click.File) -> callable:
    """Parse the configuration file and apply the preprocessing"""

    yml = yaml.safe_load(config)

    def process_tabular_file(filename: str, session: iRODSSession):
        """Apply the preprocessing to a file -this function is returned by apply_config()"""
        sheets = parse_tabular_file(filename, session, yml.get("separator", None))
        sheets_to_return = {}
        for sheetname, sheet in sheets.items():
            if sheetname not in yml["sheets"]:
                continue
            path_column_name = yml["path_column"]["column_name"]
            if yml["path_column"]["path_type"] == "part":
                sheet = query_dataobjects_with_filename(
                    session,
                    sheet,
                    path_column_name,
                    yml["path_column"]["workdir"],
                    exact_match=False,
                )
                if sheet.empty:
                    continue
            elif yml["path_column"]["path_type"] == "relative":
                sheet = chain_collection_and_filename(
                    sheet, path_column_name, yml["path_column"]["workdir"]
                )
            elif yml["path_column"]["path_type"] == "pattern":
                env = create_jinja_environment_with_filters()
                sheet = create_path_based_on_pattern(
                    sheet, yml["path_column"]["pattern"], env
                )
            else:
                sheet = sheet.rename(columns={path_column_name: DATAOBJECT})

            if "whitelist" in yml:
                sheet = sheet[
                    [c for c in sheet.columns if c in [DATAOBJECT] + yml["whitelist"]]
                ]
            elif "blacklist" in yml:
                sheet = sheet[[c for c in sheet.columns if c not in yml["blacklist"]]]
            sheets_to_return[sheetname] = sheet

        multivalue_columns = yml.get("multivalue_columns", [])
        multivalue_separator = yml.get("multivalue_separator", "")
        schema_info = yml.get("mango_schema", {})
        if os.path.exists(schema_info.get("path", "")):
            schema_instructions = {
                "schema": Schema(schema_info["path"]),
                EXCLUDE_NONSCHEMA_MD: schema_info.get(EXCLUDE_NONSCHEMA_MD, True),
                EXCLUDE_INVALID_SCHEMA_MD: schema_info.get(
                    EXCLUDE_INVALID_SCHEMA_MD, False
                ),
            }

        else:
            console.print("No schema found, metadata will be added as is.")
            schema_instructions = {}

        processed_config_data = {
            "sheets": sheets_to_return,
            "multivalue_columns": multivalue_columns,
            "multivalue_separator": multivalue_separator,
            "schema_instructions": schema_instructions,
        }
        return processed_config_data

    return process_tabular_file


# only connect to irods if requested
def get_sheets(example: str, sep=",", irods=False):
    """Parse a tabular file in iRODS or locally, with the right separator"""
    if irods:
        try:
            env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
        except KeyError:
            env_file = os.path.expanduser("~/.irods/irods_environment.json")

        ssl_settings = {}
        with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
            sheets = parse_tabular_file(example, session, sep)
    else:
        sheets = parse_tabular_file(example, separator=sep)
    return sheets
