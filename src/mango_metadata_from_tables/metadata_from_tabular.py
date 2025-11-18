import pandas as pd
import os.path
from irods.session import iRODSSession
from irods.meta import iRODSMeta, AVUOperation
from collections.abc import Generator
from typing import Set
import click
from rich.prompt import Prompt, Confirm
from rich.console import Console, Group
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.progress import track
import yaml
from mango_mdschema import Schema
from .read_table import parse_tabular_file
from .preprocessing import (
    query_dataobjects_with_filename,
    render_single_path_from_pattern,
    create_path_based_on_pattern,
    chain_collection_and_filename,
    create_jinja_environment_with_filters,
)
from . import DATAOBJECT, EXCLUDE_INVALID_SCHEMA_MD, EXCLUDE_NONSCHEMA_MD

# region Core


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


# endregion

# region prompts

console = Console()


def explain_multiple_choice():
    console.print(
        "Type one answer at a time, pressing Enter afterwards. \
            Press Enter twice when you are done.",
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


def test_pattern_on_first_column(sheet_collection: dict[pd.DataFrame], pattern: str):
    """
    Apply a pattern on the first row of the first dataframe of a dictionary of dataframes.
    """

    df = list(sheet_collection.values())[0]
    row = df.iloc[0]
    env = create_jinja_environment_with_filters()
    try:
        result = render_single_path_from_pattern(row, pattern, env)
    except Exception as e:
        message = f"The following error occured while trying to apply the pattern to a row:\n {type(e).__name__}: {repr(e.args)}"
        print(message)
        result = None
    return result


def classify_object_column(sheet_collection: dict) -> dict:

    import re

    message = """
    In order to add metadata to your data objects, each row needs
    to have a reference to your data object.

    For your table, how can we find the data object in each row?
    

    1) A column contains the absolute path to the data object
    2) A column contains the relative path to the data object
    3) A column contains (part of the) data object name
    4) The absolute path of the data object can be reconstructed by combining 
       info of multiple columns and strings.
    """

    answer = Prompt.ask(message, choices=["1", "2", "3", "4"])
    choice_mapping = {"1": "absolute", "2": "relative", "3": "part", "4": "pattern"}
    path_type = choice_mapping[answer]
    workdir = ""
    pattern = ""
    if path_type == "pattern":
        dataobject_column = ""
        pattern_question = """
    Provide a path pattern using double curly braces ({{ }}) to reference column names.
    Example: '/zone/home/project/{{ lab }}_{{ experiment }}.txt' will use values from the 'lab' and 'experiment' columns in each row.
    You can also use filters to modify the values of the columns before using them in the path.
    For more information, see the documentation in docs/construct_path_from_columns.md.\n"""
        pattern_okay = False
        while not pattern_okay:
            pattern = Prompt.ask(pattern_question)
            preview = test_pattern_on_first_column(sheet_collection, pattern)
            if preview is None:
                print("The pattern you provided is not valid.")
            else:
                pattern_ok_message = (
                    f"Based on the pattern you provided, the first row in your file contains the following path: {preview}."
                    "\n Does this look okay?"
                )
                pattern_okay = Confirm.ask(pattern_ok_message)

        print(
            f"Great! Data objects will be found by combining columns and strings in the following pattern: {pattern}"
        )

    else:
        dataobject_column = identify_dataobject_column(sheet_collection)
        if path_type in ["relative", "part"]:
            while not re.match("/[a-z_]+/home/[^/]+/?", workdir):
                workdir = Prompt.ask(
                    "What is the absolute path of the collection where we can find these data objects? \
                    (It should start with `/{zone}/home/{project}/...`)"
                )
        if path_type == "relative":
            console.print(
                Markdown(
                    f"Great! The relative paths in `{dataobject_column}` will be chained to `{workdir}`!"
                )
            )
        elif path_type == "part":
            console.print(
                Markdown(
                    f"Great! Data objects will be found by querying the contents of `{dataobject_column}` within `{workdir}`!"
                )
            )
    return {
        "dataobject_column": dataobject_column,
        "path_type": path_type,
        "pattern": pattern,
        "workdir": workdir,
    }


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
            "What is the absolute path of the collection where we can find these data objects? \
                (It should start with `/{zone}/home/{project}/...`)"
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
                f"Great! Data objects will be found by querying the contents \
                    of `{dataobject_column}` within `{workdir}`!"
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
    # creating a 'choices'-list so we don't modify the original columns list
    choices = columns.copy() + [""]
    while any(c for c in choices):
        ans = Prompt.ask(
            f"Which column(s) would you like to {filter_how}?", choices=choices
        )
        if ans:
            filter_what.add(ans)
            choices.remove(ans)
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
    choices = columns.copy() + [""]
    # creating a 'choices'-list so we don't modify the original columns list
    while any(c for c in choices):
        ans = Prompt.ask(
            "Which column(s) can contain multiple values?",
            choices=choices,
        )
        if ans:
            multivalue_columns.add(ans)
            choices.remove(ans)
        else:
            break

    # convert set back to list because a set cannot
    # be added to a yml
    multivalue_columns = list(multivalue_columns)
    return multivalue_columns


def list_columns_with_character(
    dfs: list[pd.DataFrame], eligible_columns: list, character: str
) -> Set[str]:
    """
    Given a list of pandas DataFrames, return a set of column names
    where at least one value contains the specified character.
    """

    return set(
        col
        for df in dfs
        for col in df.columns
        if col in eligible_columns
        and df[col].dtype == object
        and df[col].astype(str).str.contains(character, na=False, regex=False).any()
    )  # not sure about how this gets split in rows


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
    import yaml

    while True:
        sheets = get_sheets(example, sep, irods)
        if len(sheets) > 1:
            break
        column_names = list(sheets.values())[0].columns
        if len(column_names) > 1:
            break
        update_separator = Confirm.ask(
            f"Your sheet has only one column: `{column_names[0]}`, \
                would you like to provide another separator?"
        )
        if update_separator:
            sep = Prompt.ask("Which separator would you like to try now?") or " "
        else:
            break

    # select which sheets to use, if there are more than one
    selection_of_sheets = select_sheets(sheets)
    sheets = {k: v for k, v in sheets.items() if k in selection_of_sheets}

    # get info on the dataobject column, if there is any
    path_info = classify_object_column(sheets)
    dataobject_column = path_info["dataobject_column"]

    # if there is a dedicated dataobject column,
    # only keep the sheets that contain that column
    if dataobject_column:
        sheets = {k: v for k, v in sheets.items() if dataobject_column in v.columns}

    # start config yaml with the info we have
    for_yaml = {
        "sheets": list(sheets.keys()),
        "separator": sep,
        "path_column": {
            "column_name": dataobject_column,
            "path_type": path_info["path_type"],
            "pattern": path_info["pattern"],
            "workdir": path_info["workdir"],
        },
    }

    # ask if any columns need to be blacklisted OR whitelisted
    all_column_names = list(
        set(
            col
            for sheet in sheets.values()
            for col in sheet.columns
            if col != dataobject_column
        )
    )
    column_filter = filter_columns(all_column_names)

    # calculating columns based on filter
    if column_filter.get("whitelist", False):
        filtered_columns = [
            col for col in all_column_names if col in column_filter["whitelist"]
        ]
    elif column_filter.get("blacklist", False):
        filtered_columns = [
            col for col in all_column_names if col not in column_filter["blacklist"]
        ]
    else:
        filtered_columns = all_column_names

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
            if multivalue_separator == sep:
                print(
                    "This separator cannot be used, because it is used to separate your columns."
                )
                continue
            columns_with_separator = list_columns_with_character(
                sheets.values(), filtered_columns, multivalue_separator
            )
            if len(columns_with_separator) == 0:
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

    # ask about schema metadata
    if Confirm.ask("Do you have a ManGO metadata schema to validate your metadata?"):
        # for now, only support local schemas, we are not checking in with iRODS (yet)
        schema_file = ""
        while not os.path.exists(schema_file):
            # TODO add mango-mdschema validation OF the schema file
            schema_file = Prompt.ask("Please provide a valid path for your schema: ")
            if not schema_file:
                print("Changed your mind? We won't use a schema then!")
                break
        if schema_file:
            invalid_schema_metadata_question = (
                "Should we discard invalid schema values? "
                "(Otherwise, they will be added as non-schema metadata, "
                "e.g. 'size=medium' instead of 'mgs.schema.size=medium')"
            )
            exclude_invalid_schema_metadata = Confirm.ask(
                invalid_schema_metadata_question, default=False
            )
            nonschema_metadata_question = (
                "Should we discard the columns not covered by schema? "
                "(If you say no, they will be added as non-schema metadata):"
            )

            exclude_nonschema_metadata = Confirm.ask(
                nonschema_metadata_question, default=True
            )
            for_yaml["mango_schema"] = {
                "path": schema_file,
                EXCLUDE_NONSCHEMA_MD: exclude_nonschema_metadata,
                EXCLUDE_INVALID_SCHEMA_MD: exclude_invalid_schema_metadata,
            }

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
        processed_config_data = process_file(
            filename, session
        )  # preprocess the tabular file

        sheets = processed_config_data["sheets"]
        multivalue_columns = processed_config_data["multivalue_columns"]
        multivalue_separator = processed_config_data["multivalue_separator"]
        schema_instructions = processed_config_data["schema_instructions"]
        sheets_for_schemas = validate_schema_columns(
            sheets, schema_instructions.get("schema", None)
        )
        for sheetname, sheet in sheets.items():
            sheet_schema_instructions = (
                schema_instructions if sheetname in sheets_for_schemas else {}
            )
            progress_message = f"Adding metadata from {sheetname + ' in ' if len(sheets) > 1 else ''}`{filename}`..."
            n = 0
            errors = 0
            min_avus = None
            max_avus = None

            # loop over each row printing a progress bar
            for dataobject, md_dict in track(
                generate_rows(sheet, multivalue_columns, multivalue_separator),
                description=progress_message,
            ):
                if not dry_run:
                    simulated_avus = apply_metadata_to_data_object(
                        dataobject, md_dict, sheet_schema_instructions, session
                    )
                else:
                    console.print(
                        f"Creating the following AVUs for dataobject {dataobject}:"
                    )
                    avus = dict_to_avus(md_dict, **sheet_schema_instructions)
                    simulated_avus = len(avus)
                    print(avus)
                    console.print("\n")
                if simulated_avus:
                    n += 1
                    if max_avus is None or simulated_avus > max_avus:
                        max_avus = simulated_avus
                    if min_avus is None or simulated_avus < min_avus:
                        min_avus = simulated_avus
                else:
                    errors += 1

            avu_length_range = (
                max_avus if min_avus == max_avus else f"{min_avus} to {max_avus}"
            )
            console.print(
                Markdown(
                    # This calculation may not be correct anymore in case of multiple values,
                    # since the md_dict of each object can now have a different length
                    f"{'Simulated' if dry_run else 'Applied'} {avu_length_range} AVUs for each of {n} data objects"
                )
            )
            if errors > 0:
                console.print(
                    f"{errors} data objects were skipped because the paths were not valid!",
                    style="red bold",
                )


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


# endregion

if __name__ == "__main__":
    mdtab()
