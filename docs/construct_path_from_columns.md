# Constructing paths from other columns

This module allows users to construct the paths of data objects by combining information from one or more columns of the table.  
This can be configured during setup.  
This documentation will guide you how to create your own pattern to construct paths, both by taking info from other columns, and by modyfing these values with filters.  

## 1. Creating a pattern

A normal data object path will look roughly like follows:  

`'/zone/home/project/collection/subcollection/filename.extension'`  

Ideally, your table contains a column with this literal information.  
Alternatively, by putting a column name between two curly braces (`{{ }}`), you can inject a value from another column into the path.  
Mind that there should be spaces between the curly braces and the column name.  
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
In that case, you can use filters to change the formatting of the values.

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


### date_format

Description:  
    The date_format filter converts a date from one format (e.g. DD/MM/YYYY) to another (e.g. YYMMDD).  
    If the value is a datetime object, it will be converted immediately.  
    If the value is a string, it will be converted to a datetime object
    according to the input format, and then converted to the output format.  
Arguments:   
 - input_format: str  
      The format of the date you are converting.  
      Default is YYYYMMDD.  
 - output_format: str  
      The format of the date you are converting.  
      Default is YYYYMMDD.  
Example:  
   ```
   '20000201' | date_format(input_format="%Y%m%d", output_format="%d/%m/%Y")
   ``` 
   turns `20000201` into `01/02/2000`.


### lower

Description:    
    The lower filter converts a text string into lowercase.    
Arguments:  
    None  
Example:  
    ```
    'Hello World' | lower
    ```
    turns 'Hello World' into 'hello world'

