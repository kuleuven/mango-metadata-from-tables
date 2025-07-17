import pandas as pd
import os.path
from pathlib import Path
from irods.session import iRODSSession
from irods.meta import iRODSMeta, AVUOperation
from irods.exception import DataObjectDoesNotExist, CollectionDoesNotExist
from irods.data_object import iRODSDataObject
from irods.column import Criterion
from irods.models import Collection, DataObject
from collections.abc import Generator
from typing import Set
import click
from rich.prompt import Prompt, Confirm
from rich.console import Console, Group
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.progress import track
import yaml

DATAOBJECT = "dataobject"


# region OpenExcel
def create_file_object(path: str, session=None):
    """Turn path to file into a file-like object.

    Args:
        session: (iRODSSession or None): session to connect to iRODS.
            Session can be set to None for testing non-irods functionalities.
        path (str): Path to tabular file

    Raises:
        FileNotFoundError: If the file cannot be found locally or in ManGO.

    Returns:
        pathlib.Path or irods.iRODSDataObject: File-like object to read metadata from.
    """
    ppath = Path(path)
    if ppath.suffix not in [".xlsx", ".csv", ".tsv"]:
        raise IOError("Filetype not accepted")
    if ppath.exists():
        return ppath
    if session:
        try:
            return session.data_objects.get(path)
        except DataObjectDoesNotExist or CollectionDoesNotExist as e:
            raise e
    raise FileNotFoundError


def parse_tabular_file(path: str, session=None, separator: str = ","):
    """Parse tabular file.

    Args:
        path (str): Path to the tabular file.
        session (iRODSSession or None): session to connect to iRODS.
            If it is none, it is assumed that we are testing
        separator (str, optional): Separator for plain text files.. Defaults to ",".

    Raises:
        IOError: If the file cannot be parsed (it is not .xlsx, .csv or .tsv) it won't be read.

    Returns:
        dict: Dictionary of pandas.DataFrames with sheet names as keys.
    """

    file = create_file_object(path, session)
    if path.endswith("xlsx"):
        # Local excel files are binary and should be opened with 'rb'.
        # However, iRODS implemented their 'open' method differently,
        # and there you should use just 'r' instead
        reading_mode = "r" if type(file) == iRODSDataObject else "rb"
        with file.open(reading_mode) as f:
            sheets = pd.read_excel(f, sheet_name=None)
        if any(x.strip() != x for x in sheets.keys()):
            sheets = {k.strip(): v for k, v in sheets.items()}
    else:
        # these types are not binary and should be opened with 'r'
        with file.open("r") as f:
            sheets = {"single_sheet": pd.read_csv(f, sep=separator)}
    for sheet in sheets.values():
        sheet.columns = sheet.columns.str.strip()
    return sheets


# endregion
# region Preprocessing


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


def chain_collection_and_filename(
    df: pd.DataFrame, filename_column: str, workingdirectory: str
):
    """Renames the column with the relative data object path and completes it with the collection path"""
    df = df.rename(columns={filename_column: DATAOBJECT})
    df[DATAOBJECT] = [str(Path(workingdirectory) / Path(x)) for x in df[DATAOBJECT]]
    return df


# endregion
# region Core


def dict_to_avus(row: dict) -> Generator[iRODSMeta]:
    """Convert a dictionary of metadata name-value pairs into a generator of iRODSMeta"""
    avus = (
        iRODSMeta(str(key), str(value_item))
        for key, value in row.items()
        for value_item in value
        if not pd.isna(value_item)
    )
    return avus


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


def apply_metadata_to_data_object(path: str, avu_dict: dict, session: iRODSSession):
    """Add metadata from a dictionary to a given data object"""
    try:
        obj = session.data_objects.get(path)
        obj.metadata.apply_atomic_operations(
            *[
                AVUOperation(operation="add", avu=item)
                for item in dict_to_avus(avu_dict)
            ]
        )
        return True
    except Exception as e:
        print(e)
        return False


# endregion

# region prompts

console = Console()


def explain_multiple_choice():
    console.print(
        "Type one answer at a time, pressing Enter afterwards. Press Enter twice when you are done.",
        style="italic magenta",
    )


def select_sheets(sheet_collection: dict) -> list:
    """Ask user to choose which sheets to use from an Excel"""
    selection_of_sheets = list(sheet_collection.keys())
    if len(sheet_collection) == 1:
        if selection_of_sheets[0] == "single_sheet":
            console.print(
                "You have provided a plain text file, no multiple sheets, great work!"
            )
        else:
            console.print(
                Markdown(
                    f"The file you provided has only one sheet: `{selection_of_sheets[0]}`."
                )
            )
        return selection_of_sheets[0]
    all_sheets = Confirm.ask("Would you like to use all of the available sheets?")
    if all_sheets:
        return selection_of_sheets
    explain_multiple_choice()
    selected_sheets = []
    while True:
        selected_sheet = Prompt.ask(
            "Which of the available sheets would you like to select?",
            choices=selection_of_sheets + [""],
        )
        if selected_sheet:
            selected_sheets.append(selected_sheet)
        else:
            break
    return selected_sheets


def identify_dataobject_column(sheet_collection: dict) -> str:
    """Ask user which column contains the unique data object information"""
    columns = set([col for sheet in sheet_collection.values() for col in sheet.columns])
    dfs = "dataframe has" if len(sheet_collection) == 1 else "dataframes have"
    cols = "1 column" if len(columns) == 1 else f"{len(columns)} columns"
    column_intro = f"Your {dfs} {cols}:\n\n"
    column_list = "\n\n".join(f"- {col}" for col in columns)
    console.print(Markdown(column_intro + column_list))
    return Prompt.ask(
        "Which column contains an unique identifier for the target data object?",
        choices=columns,
    )


def classify_dataobject_column(dataobject_column: str) -> dict:
    """Ask user whether the unique identifier is a relative path or part of a filename"""
    import re

    path_type = Prompt.ask(
        f"Is the path coded in `{dataobject_column}` a relative path or part of a filename?",
        choices=["relative", "part"],
    )
    workdir = ""
    while not re.match("/[a-z_]+/home/[^/]+/?", workdir):
        workdir = Prompt.ask(
            "What is the absolute path of the collection where we can find these data objects? (It should start with `/{zone}/home/{project}/...`)"
        )
    if path_type == "relative":
        console.print(
            Markdown(
                f"Great! The relative paths in `{dataobject_column}` will be chained to `{workdir}`!"
            )
        )
    else:
        console.print(
            Markdown(
                f"Great! Data objects will be found by querying the contents of `{dataobject_column}` within `{workdir}`!"
            )
        )
    return {"path_type": path_type, "workdir": workdir}


def filter_columns(columns: list) -> dict:
    """Ask user to blacklist or whitelist columns"""
    filter_how = Prompt.ask(
        "Would you like to whitelist or blacklist some columns?",
        choices=["whitelist", "blacklist", "neither"],
        default="neither",
    )
    if filter_how == "neither":
        return {}
    explain_multiple_choice()
    # using a set to get a list without duplicates
    filter_what = set()
    while len(columns) > 0:
        ans = Prompt.ask(
            f"Which column(s) would you like to {filter_how}?", choices=columns + [""]
        )
        if ans:
            filter_what.add(ans)
            columns.remove(ans)
        else:
            break
    # convert set back to list because a set cannot
    # be added to a yml
    filter_what = list(filter_what)
    if len(filter_what) == 0:
        return {}
    return {filter_how: filter_what}


def ask_multivalue_columns(columns: list) -> list:
    """Ask user whether the sheets contain any colums with multiple values"""

    explain_multiple_choice()
    # using a set to get a list without duplicates
    multivalue_columns = set()

    while len(columns) > 0:
        ans = Prompt.ask(
            f"Which column(s) can contain multiple values?",
            choices=columns + [""],
        )
        if ans:
            multivalue_columns.add(ans)
            columns.remove(ans)
        else:
            break

    # convert set back to list because a set cannot
    # be added to a yml
    multivalue_columns = list(multivalue_columns)
    return multivalue_columns


def list_columns_with_character(dfs: list[pd.DataFrame], character: str) -> Set[str]:
    """
    Given a list of pandas DataFrames, return a set of column names
    where at least one value contains the specified character.
    """
    columns = set()
    for df in dfs:
        for col in df.columns:
            # Only check string columns to avoid errors
            if df[col].dtype == object:
                if (
                    df[col]
                    .astype(str)
                    .str.contains(character, na=False, regex=False)
                    .any()
                ):
                    columns.add(col)
    return columns


# endregion

# region Chains


# These are the functions that would be called from the command line :)
# (and use click)
@click.group()
def mdtab():
    """Process tabular files to add iRODS metadata to data objects."""
    pass


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


@mdtab.command()
@click.option("--sep", default=",", help="Separator for plain text files.")
@click.option(
    "--irods/--no-irods", default=False, help="Whether an iRODS session is needed."
)
@click.argument("example")
@click.argument("output", type=click.File("w"))
def setup(example, output, sep=",", irods=False):
    """
    Generate configuration file.

    EXAMPLE is the path to the tabular file to parse.

    OUTPUT is the path where the configuration file will be saved.
    """
    import re
    import yaml

    while True:
        sheets = get_sheets(example, sep, irods)
        if len(sheets) > 1:
            break
        column_names = list(sheets.values())[0].columns
        if len(column_names) > 1:
            break
        update_separator = Confirm.ask(
            f"Your sheet has only one column: `{column_names[0]}`, would you like to provide another separator?"
        )
        if update_separator:
            sep = Prompt.ask("Which separator would you like to try now?") or " "
        else:
            break

    # select which sheets to use, if there are more than one
    selection_of_sheets = select_sheets(sheets)

    sheets = {k: v for k, v in sheets.items() if k in selection_of_sheets}

    # identify the column with data objects identifiers
    dataobject_column = identify_dataobject_column(sheets)
    # only keep sheets that contain that column
    sheets = {k: v for k, v in sheets.items() if dataobject_column in v.columns}

    # start config yaml with the info we have
    for_yaml = {
        "sheets": list(sheets.keys()),
        "separator": sep,
        "path_column": {
            "column_name": dataobject_column,
        },
    }

    # check the first data object name to see if it is absolute
    first_dataobject = list(sheets.values())[0][dataobject_column][0]
    if re.match("/[a-z_]+/home/[^/]+/", first_dataobject):
        dataobject_column_type = {"path_type": "absolute"}
    else:
        # if the path is not absolute, ask:
        # - whether it is relative or part of a filename
        # - in which working directory (at least project level) it should be searched
        dataobject_column_type = classify_dataobject_column(dataobject_column)
    # add path and working directory info to the yaml
    for_yaml["path_column"].update(dataobject_column_type)

    # ask if any columns need to be blacklisted OR whitelisted
    column_filter = filter_columns(
        list(
            set(
                [
                    col
                    for sheet in sheets.values()
                    for col in sheet.columns
                    if col != dataobject_column
                ]
            )
        )
    )
    # update yaml with column information
    for_yaml.update(column_filter)

    if Confirm.ask(
        "Do your sheet(s) have columns which may contain multiple values per row?"
    ):

        valid_separator_chosen = False
        while not valid_separator_chosen:
            multivalue_separator = Prompt.ask(
                "What is the separator of your columns with multiple values?"
            )
            columns_with_separator = list_columns_with_character(
                sheets.values(), multivalue_separator
            )
            if multivalue_separator == sep:
                print(
                    "This separator cannot be used, because it is used to separate your columns."
                )

            elif len(columns_with_separator) == 0:
                print(
                    "This separator cannot be used, because it does not appear in your file."
                )
            else:
                valid_separator_chosen = True

        # ask for multivalue columns
        multivalue_columns = ask_multivalue_columns(
            list(col for col in columns_with_separator if col != dataobject_column)
        )
        # update yaml with multivalue columns information
        for_yaml["multivalue_separator"] = multivalue_separator
        for_yaml["multivalue_columns"] = multivalue_columns

    # create yaml from the dictionary
    yml = yaml.dump(for_yaml, default_flow_style=False, indent=2)
    # Make a group and indicate where it is saved
    panel_group = Group(
        Markdown("# This is your config yaml"),
        Syntax(yml, "yaml"),
        Markdown(f"_It will be saved in `{output.name}`._"),
    )
    console.print(panel_group)
    click.echo(yml, file=output)


@mdtab.command()
@click.option(
    "--config",
    type=click.File("r"),
    required=True,
    help="Configuration file created by `setup`.",
)
@click.option("--dry-run", is_flag=True, help="Simulate applying the metadata.")
@click.argument("filename")
def run(filename, config, dry_run=False):
    """Apply metadata from a tabular file to data objects.

    FILENAME is the path to the tabular file containing the metadata.
    It should have some column with a unique identifier for the data objects,
    and columns for other metadata fields.
    """

    process_file = apply_config(config)  # parse the configuration file
    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment.json")

    ssl_settings = {}
    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        sheets = process_file(filename, session)  # preprocess the tabular file
        for sheetname, sheet in sheets.items():
            progress_message = f"Adding metadata from {sheetname + ' in ' if len(sheets) > 1 else ''}`{filename}`..."
            n = 0
            errors = 0

            # need to put the pointer back to the start of the yml file
            # in order to read configuration
            config.seek(0)
            yml = yaml.safe_load(config)
            multivalue_columns = yml.get("multivalue_columns") or []
            multivalue_separator = yml.get("multivalue_separator") or ""

            # loop over each row printing a progress bar
            for dataobject, md_dict in track(
                generate_rows(sheet, multivalue_columns, multivalue_separator),
                description=progress_message,
            ):
                res = True
                if not dry_run:
                    res = apply_metadata_to_data_object(dataobject, md_dict, session)
                if res:
                    n += 1
                else:
                    errors += 1

            console.print(
                Markdown(
                    # This calculation may not be correct anymore in case of multiple values,
                    # since the md_dict of each object can now have a different length
                    f"{'Created' if dry_run else 'Applied'} {len(md_dict)} AVUs for each of {n} data objects, with the following keys:\n\n"
                    + "\n\n".join(f"- **{k}**" for k in md_dict.keys())
                )
            )
            if errors > 0:
                console.print(
                    f"{errors} data objects were skipped because the paths were not valid!",
                    style="red bold",
                )


def apply_config(config: click.File) -> callable:
    """Parse the configuration file and apply the preprocessing"""

    yml = yaml.safe_load(config)

    def process_tabular_file(filename: str, session: iRODSSession):
        """Apply the preprocessing to a file -this function is returned by apply_config()"""
        sheets = parse_tabular_file(filename, session, yml.get("separator", None))
        sheets_to_return = {}
        for sheetname, sheet in sheets.items():
            if not sheetname in yml["sheets"]:
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
            else:
                sheet = sheet.rename(columns={path_column_name: DATAOBJECT})

            if "whitelist" in yml:
                sheet = sheet[
                    [c for c in sheet.columns if c in [DATAOBJECT] + yml["whitelist"]]
                ]
            elif "blacklist" in yml:
                sheet = sheet[[c for c in sheet.columns if c not in yml["blacklist"]]]
            sheets_to_return[sheetname] = sheet

        return sheets_to_return

    return process_tabular_file


# endregion

if __name__ == "__main__":
    mdtab()
