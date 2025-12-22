# Python module to extract metadata from tables

Use this module to process tabular files in which each row represents an iRODS data object
and each column contains either an identifier or metadata to add to this data object.
It supports plain text files and Excel files, which could be stored locally or in iRODS itself.

To get started, create a virtual environment with pip and install the dependencies described in the [requirements file](./requirements.txt):

```sh
python -m venv venv
source venv/bin/activate
pip install -e .
```

Now, you can run the script with the command `mango-metadata-from-tables`.  


## Usage

This module can run on the command line with two commands: `setup` and `run`.

The `setup` command takes as arguments the path to a tabular file (local or in iRODS) and
the desired path for the output YAML, asks the user questions about how to
parse the tabular file, and outputs a configuration file.

This configuration file can then be provided as the `--config` option to the `run`
command in order to standardize tabular files and properly obtain paths to data objects
and attach metadata to them based on the columns of these files.

**Note:** Empty values in the table are ignored, and will not be added as metadata.  
In some cases, users may find it meaningful to include the absence of a value as contextual information.  
In those cases, we advice to use a value like "Unknown", "Not applicable" or "NA" in your table instead.   

### Examples

### A small csv file

The following file simulates having [a small semicolon-separated file](./testdata/testdata.csv)
with absolute paths in a "dataobject" column and a few columns with metadata.

First, with the `setup` command, we answer a few questions on how to parse the tabular file
and create a "test-config.yaml" configuration file that keeps track of the answers.

Then, with the `run` command, we use the information on the configuration YAML file to parse
the tabular file and, because it's just a "dry run", we simulate adding the metadata to each
data object. Note that this `run` command could then also be used on other tabular files
with the same properties as the original one.

```sh
mango-metadata-from-tables setup testdata/testdata.csv test-config.yaml --sep ";"
mango-metadata-from-tables run testdata/testdata.csv --config test-config.yaml --dry-run
```

### A larger Excel file with multiple sheets

In this second example the file is an [Excel file with multiple sheets](./testdata/bigger-testdata.xlsx),
including one that has no relevant metadata. Again, with the `setup` command we indicate
how the Excel should be parsed and record the answers in a YAML configuration file.
Then, with the `run` command we parse the Excel and simulate adding the metadata.

```sh
mango-metadata-from-tables setup testdata/bigger-testdata.xlsx bigger-test-config.yaml
mango-metadata-from-tables run testdata/bigger-testdata.xlsx --config bigger-test-config.yaml --dry-run
```

## `setup`

The configuration file can be created as follows:

```sh
mango-metadata-from-tables setup filename output_path
```

In this case `filename` is the path to a tabular file (csv, tsv, Excel...),
stored either locally or in iRODS. If it lives in iRODS, the `--irods` flag should be used,
so that an iRODS session is started:

```sh
mango-metadata-from-tables setup /zone/home/project/path/to/tabular output_path --irods
```

If the tabular file is a plain text file, it is possible to specify a column separator
with the `--sep` option, which has "," as a default. If a wrong separator is provided and
the parser finds a single column, it will warn you and give you the possibility to correct it.

```sh
mango-metadata-from-tables setup testdata/testdata.csv test-output.yml --sep ";"
```

If the file can be found and opened as a dataframe, the user will be prompted with questions
that will later guide preprocessing of equivalent tabular files:

- If there are multiple sheets in an Excel file, which one(s) should be used?
- Which of the columns contains a unique identifier of the data objects that metadata has to be attached to?
- If the unique identifier is not an absolute path, is it a relative path or part of filename?
And if so, within which collection should the data objects be found?
- Should any columns be whitelisted or blacklisted?

The final YAML will be printed on the console and saved as a file locally.

## `run`

Given a path to a tabular file with metadata and a YAML with the settings to preprocess it,
metadata can be added with the `run` command:

```sh
mango-metadata-from-tables  run path_to_tabular --config path/to/config.yml
```

For testing purposes, it is possible to use
the `--dry-run` flag, which simulates the preprocessing and identification of metadata and
prints a small report at the end.
An iRODS session will be initiated always, so **make sure you have a valid active iRODS Session**.


```sh
mango-metadata-from-tables  run path_to_tabular --config path/to/config.yml --dry-run
```

It is not necessary to rerun both `setup` and `run` for each tabular file:
if you have several tabular files with the same properties, and that thus can be described by the same
YAML configuration file, you just need to run `setup` with one of them, and then
`run` with each of the tabular files and the same configuration file.
