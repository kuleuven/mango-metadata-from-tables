# Constructing paths from other columns

This application allows users to construct the paths of data objects by combining information from one or more columns of the table.  
This can be configured during setup.  
This documentation will guide you how to create your own pattern to construct paths, both by taking info from other columns, and by modyfing these values with filters.  

## 1. Creating a pattern

A normal data object path will look roughly like follows:  

`'/zone/home/project/collection/subcollection/filename.extension'`  

Ideally, your table contains a column with this literal information, or with a relative path to be combined with a working directory.  
Alternatively, by putting a column name between [two curly braces](https://jinja.palletsprojects.com/en/stable/templates/#variables) (`{{ }}`), you can inject a value from another column into the path.  
As example, let's imagine an Excel file with the following contents:  

| sample | lab           | experiment  |
|--------|---------------|-------------|
| 01     | chemistry_lab | experimentA |
| 02     | physics_lab   | experimentB |

Imagine the data object paths are as follows:  
 
- /zone/home/sciences/chemistry_lab/output/experimentA.txt  
- /zone/home/sciences/physics_lab/output/experimentB.txt  

In that cases, the pattern to construct these paths is as follows:

`'/zone/home/sciences/{{ lab }}/output/{{ experiment }}.txt'`


## 2. Modifying information with filters

Sometimes, the values in a column do not have the same format as they should have in the data object paths.  
In that case, you can use [filters](https://jinja.palletsprojects.com/en/stable/templates/#filters) to change the formatting of the values.

The syntax is as follows:

`'{{ column_name | filter_name }}'`

If the filter needs one or more arguments:

`'{{ column_name | filter_name(argument1=value, argument2=value, ...) }}'`

For example, imagine the following excel, and corresponding data object paths: 

| sample | lab           | experiment  |
|--------|---------------|-------------|
| 01     | CHEMISTRY_LAB | experimentA |
| 02     | PHYSICS_LAB   | experimentB |

- /zone/home/sciences/chemistry_lab/output/experimentA.txt  
- /zone/home/sciences/physics_lab/output/experimentB.txt  

In this case, we need to convert the value of the column 'lab' to lowercase.
This can be done as follows:

`'/zone/home/sciences/{{ lab | lower }}/output/{{ experiment }}.txt'`

## 3. List of available filters

In order to modify values, you can use a list of **built-in filters** created by Jinja, on which this paths construction system is based.  
You can find a list of built-in filters and their usage [here](https://jinja.palletsprojects.com/en/stable/templates/#builtin-filters).  

Apart from that, this application also contains its own filters, of which you can find a list below:  

### `date_format`

The `date_format` filter converts a [datetime object](https://docs.python.org/3/library/datetime.html) or a valid string representing a datetime into a string in a [specific format](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).

Argument | Type | Description | Default
--- | --- | --- | ---
`input_format` | string | The format of the source string | YYYMMDD
`output_format` | string | The format of the target string | YYYYMMDD

#### Examples

Value of `my_date` | Pattern | Result
---- | --- | ---
"20000201" | `{{ my_date \| date_format(output_format="%d/%m/%Y") }}` | "01/02/2000"
"2000-02-01" | `{{ my_date \| date_format(input_format="%Y-%m-%d") }}` | "20000201"
