import pandas as pd
import jinja2
import datetime
from pathlib import Path
from irods.column import Criterion
from irods.models import Collection, DataObject
from . import DATAOBJECT


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
