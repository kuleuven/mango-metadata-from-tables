import pandas as pd
from . import console
from rich.markdown import Markdown
from rich.prompt import Prompt, Confirm
from .preprocessing import (
    create_jinja_environment_with_filters,
    render_single_path_from_pattern,
)
from typing import Set


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


def classify_dataobject_column(sheet_collection: dict) -> dict:

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
