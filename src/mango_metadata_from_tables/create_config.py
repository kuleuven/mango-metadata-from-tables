import click
from rich.prompt import Prompt, Confirm
from rich.console import Group
from rich.syntax import Syntax
from .prompts import (
    select_sheets,
    classify_dataobject_column,
    filter_columns,
    ask_multivalue_columns,
    list_columns_with_character,
)
import os.path
from rich.markdown import Markdown
from .preprocessing import get_sheets
from . import EXCLUDE_INVALID_SCHEMA_MD, EXCLUDE_NONSCHEMA_MD, console


@click.command()
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
