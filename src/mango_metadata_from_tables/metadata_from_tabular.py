import pandas as pd
import os.path
from irods.session import iRODSSession

import click
from rich.prompt import Prompt, Confirm
from rich.console import Group
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.progress import track
import yaml
from mango_mdschema import Schema
from .read_table import parse_tabular_file
from .preprocessing import (
    query_dataobjects_with_filename,
    create_path_based_on_pattern,
    chain_collection_and_filename,
    create_jinja_environment_with_filters,
)
from .dataframe2avus import dict_to_avus, generate_rows, apply_metadata_to_data_object
from .prompts import (
    select_sheets,
    classify_dataobject_column,
    filter_columns,
    ask_multivalue_columns,
    list_columns_with_character,
)
from . import DATAOBJECT, EXCLUDE_INVALID_SCHEMA_MD, EXCLUDE_NONSCHEMA_MD, console


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
    path_info = classify_dataobject_column(sheets)
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
