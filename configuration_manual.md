# "PORTADA DATA LAYER" CONFIGURATION AND USER MANUAL

_"Portada Data Layer"_ is a Spark-based library designed to facilitate ETL (Extract, Transform, Load) processing for project data, provide common Spark configuration, and manage the Delta Lake storage layer. Furthermore, _Portada Data Layer_ transparently utilizes specialized dataframes that automate metadata creation, making it easier to track the transformations applied to the original data throughout the ETL process. Users simply need to rely on the library for the creation, retrieval, and storage of these dataframes. Once created, all standard Spark functionalities for data transformation and querying can be applied to these dataframes without any limitations.

## Initial Configuration

The library supports various methods for initial configuration. Classes for managing and storing dataframes are instantiated using "builders," which can be configured either through specific methods or by providing the path to a JSON configuration file.

### Data Lake Parameters

The configuration allows for the specification of various Data Lake parameters:

**Data Lake Storage Protocol.** This defines the protocol used for storing data within the Data Lake (e.g., "file://", "hdfs://", or any other valid storage protocol). The protocol specified here must be compatible with the one used by the actual Data Lake. The _Portada_ project uses Hadoop (_hdfs://_) as its protocol.

In the JSON configuration file, the `protocol` attribute is used; alternatively, a method of the same name can be called to override the configuration when necessary.

**Data Lake Base Path.** This indicates the base path where the Data Lake is located within the underlying storage system (e.g., Hadoop, a local file system, S3, Azure, etc.). This parameter establishes the root directory from which all Data Lake data will be stored.

In the JSON configuration file, the `base_path` attribute is used; alternatively, a method of the same name can be called to override the configuration when necessary. **Data lake project name**. This specifies the project name, which—if provided—is used as part of the root path for the Delta Lake. In conjunction with the previous parameter, it allows a single storage system to be shared across multiple data lakes, each configured with a distinct base path and/or project name.

Within the JSON file, the `project_data_name` attribute or the `project_name` method can be used to override the configuration whenever necessary.

Let's look at an example:

`Example of a JSON file for configuring the storage protocol:`
```
{
    "protocol": "hdfs://",
    "base_path": "/app/delta_test/docker",
    "project_data_name": "portada_project",
    ...
}

```
`If the file is stored at config/data_layer_config.json, the builder configuration call would be:`

``` python
# Builder configuration using an initial JSON configuration file
builder = PortadaBuilder("config/data_layer_config.json")
# It can also be overwritten using the appropriate method
builder = builder.protocol("file://").base_path("/delta_lake").project_name("portada")
```

### Parameters for _Spark_

The configuration also allows for the definition of _Spark_ parameters. All parameters compatible with Spark are supported, both within the JSON file and via builder method calls. In the JSON file, any attribute beginning with the prefix _"spark."_ is treated as part of the Spark session configuration. While most Spark session parameters use the _"spark."_ prefix, there are exceptions—as well as certain versions—that do not. These should be configured using the _"configs"_ attribute in the JSON, structured as an array of dictionaries containing attributes and their corresponding values. The _key/value_ pairs within _"configs"_ can include attributes both with and without the _"spark."_ prefix.

Alternatively, you can configure these parameters by calling the _config_ method on the builder. There are, however, two specific methods with their own dedicated names: _master_ and _app_name_. For all other parameters, the _config_ method should be used.

You can review the available Spark configuration properties here: [https://spark.apache.org/docs/latest/configuration.html#spark-properties](https://spark.apache.org/docs/latest/configuration.html#spark-properties).

We see an example of a JSON file:

```
{ 
    "protocol": "file://", 
    "base_path": "/app/delta_test/docker", 
    "app_name": "DataLakePortadaCuration_01", 
    "project_data_name": "project_cover", 
    "master": "local[*]", 
    "spark.sql.shuffle.partitions": "4", 
    "spark.databricks.delta.schema.autoMerge.enabled": "true", 
    "spark.sql.caseSensitive": "false", 
    "spark.sql.storeAssignmentPolicy": "LEGACY", 
    "spark.sql.parquet.mergeSchema": "true", 
    "spark.sql.legacy.allowUntypedScalaUDF": "true", 
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1", 
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", 
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", 
    "configs":[ 
        {"spark.sql.parquet.fs.optimized.committer.optimization-enabled":"false"}, 
        {"spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"}, 
        {"mapreduce.fileoutputcommitter.algorithm.version":"2"} 
    ]
}
```

Which will allow us to configure a _"cover data layer" _builder_ from the builder. As in the previous cases, it is also possible to add or overwrite specific values ​​by calling methods on the builder:

``` python
# Builder configuration using an initial JSON configuration file
builder = PortadaBuilder("config/data_layer_config.json")
# Values ​​can be overwritten using the appropriate method
builder.app_name("Portada_APP")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.driver.maxResultSize", "7g")
    .config("spark.driver.memory", "7g")
```

### Other parameters

In a production environment, the _Portada data layer_ classes use Redis to track the state of certain processes. During development and testing, this can be disabled using the _use_redis_metadata_ parameter. This parameter can only be configured by calling the method of the same name and passing a boolean value to indicate whether or not Redis should be used for state tracking. Note that the default value for this parameter is _True_.

``` python
builder = builder.app_name("Portada_APP")
```

## Specific configuration for data cleaning

Two configuration files are required for the data cleaning process: one containing the schema and the operations to be applied to each field, and a second containing a map of character substitutions and modifications based on data type. Both files are loaded into the instance of the class responsible for cleaning (e.g., BoatFactCleaning) as a JSON dictionary before the cleaning processes begin. 

```python
...
with open(schema_path_file, "r") as f: 
    schema_cfg = json.load(f)
with open(regex_mapping_path_file, "r") as f: 
    regex_mapping = json.load(f)

layer = PortadaBuilder(init_config_path_file).build(BoatFactCleaning.__name__)
layer.use_schema(schema_cfg).use_mapping_to_clean_chars(regex_mapping)
...
```

The substitution map file allows for the generic cleaning of fields based on their type and functionality. For date and numeric types, the file enables the replacement of written-out numbers (one through nine) with their corresponding digits. It also accounts for errors introduced by OCR, converting specific characters into digits (e.g., 'O' to '0', 'Z' to '2', '&' to '6'), and transforms mixed textual values ​​into purely numeric ones (e.g., "2 and a half" to "2.5", "3 and 1/4" to "3.75"). Regarding character strings, the system distinguishes between single-word fields, text-only fields (containing no digits), multi-word fields that are not paragraphs, and text that does not permit parentheses, among others. Furthermore, the file supports specific substitutions tailored to individual fields identified by name.

These substitutions are carried out using two mechanisms: a list of characters mapped to replacement values, and regular expressions mapped to replacement values. The latter mechanism allows for the use of groups to extract portions of the original text. The map is structured as a key-value pair system, where the keys represent the characters or regular expressions to be detected, and the values ​​represent the replacement text. Single-character keys result in the replacement of the target character with the character specified as the value. Keys longer than one character are treated as regular expressions, and their corresponding values ​​replace the content of any fields that match the expression's pattern. Examples:

```
{
    "numeric_map":{
        "^\\s*un.?\\s*$": "1",
        "^\\s*dos\\s*$": "2",    
        "^\\s*tres\\s*$": "3",
        ...
        "A": "4",
        "S": "5",
        ...
    }
    ...
    "only_text_map":{
        "0": "O",
        "6": "G",
        ...
    }
    "one_word_map":{
        "[": "",
        "{": "",
        ...
    }
    "master_role": {
        "epn": "cpn",
        ...
    }
    ...
}
```

The schema file must follow JSON schema syntax, with some variations that will be discussed later. The schema must represent the desired final structure—not the original structure obtained from the extraction, but the one intended after the cleaning process has been applied. The original data resulting from the ingestion phase will contain all extracted fields as strings, regardless of the actual data type they represent.

The accepted variations make it possible to identify and define which cleaning operations should be applied to each field. The "*type*" attribute identifies the required type for the field after the cleaning process. The "*for_cleaning*" attribute specifies which substitution maps should be used when cleaning a field—for instance, to handle cases such as removing digits, parentheses, or restricting the content to a single word. The "_calculable_if_null_" attribute allows for a specific, limited set of operations—such as calculating a prior date based on a time period, or determining the duration between two dates—to be applied to fields marked with this attribute. Finally, the "*fill_with*" attribute enables the configuration of custom cleaning routines to modify fields that meet certain criteria, using expressions, values ​​from other columns, literal values, and so on. While the functionality of "*fill_with*" could potentially replace "_calculable_if_null_," the former is more complex and requires a deeper level of expertise to implement; therefore, "_calculable_if_null_" is retained, with the possibility of adding further operations in the future.
At the root level of the schema, it is also possible to define the _delete_if_ attribute—which specifies the conditions for removing records—as well as the _order_and_partitions_ attribute, which 
defines the ordering and partitions used to resolve values ​​marked as "idem." Examples:

```
{
    ...
    "publication_edition": {
        "type": "string",
        "description": "The edition in which the entry's news item was published.",
        "minLength": 1,
        "maxLength": 1,
        "nullable": false,
        "for_cleaning":["not_cleanable"]
    },
    ...
    "travel_departure_date": {
        "type": "string",
        "description": "Date of departure from the port, in YYYY-MM-DD format.",
        "format": "date",
        "nullable": true,
        "fill_with": {
            "type": "replace",
            "from": "LIT('')",
            "condition": "publication_name = 'DB'"
        },
        "calculable_if_null": {"operation": "decrement_data", "parameters":{"origin_date": "travel_arrival_date", "decrement": "travel_duration_value", "decrement_unit": "travel_duration_unit"}}
    },
    ...
    "ship_type": {
        "type": "string",
        "description": "Type of vessel.",
        "nullable": true,
        "for_cleaning":["accepted_abbreviations", "not_digits", "not_parenthesis"],
        ...
    }
    ...
    "order_and_partitions": {
        "order": ["publication_name", "publication_date", "publication_edition", "entry_id"],
        "partitions": ["publication_name", "publication_date", "publication_edition"]
    },
    "delete_if": "parsed_text rlike '(?i).*(?:(?:acorazado)|(?:torpedero)).*'"
}
```

Each of the cleaning processes will extract the relevant information from the configuration and act accordingly. All cleaning processes generate metadata containing historical values ​​to support the traceability of changes made during this phase.

### Metadata for cleaning processes and traceability.
The `data_layer` library in the `portada` project strikes a balance, ensuring that the stored meta-information provides sufficient insight into the traceability of changes made during the cleaning phase, without significantly compromising the temporal efficiency of the processes. To this end, three levels of metadata are distinguished: a log of generic operations on the dataframe; a history of values ​​and cleaning operations to support traceability; and a history of values ​​without support for traceability.

The first level is a detailed log of all operations performed on the dataframes during the cleaning process. It serves as a historical record of changes made at the dataframe level (e.g., copying columns, using Spark functions to modify column values, dropping columns, etc.). While useful for tracking specific operations and the columns involved, it is impractical for maintaining traceability during extensive cleaning processes that involve a large number of changes. It is disabled by default. 

```
Registration example
--------------------
log_id:3471fe4a-24b2-1114-ac41-128a5dfa159
timestamp:2026-11-03T23:20:13.212459+00:00
storage id: 34b0f70a-61e1-4013-a4e7-12cf38afc007
transformer_name: save_original_values_of_entries
dataframe_name: DF(ingest/ship_entries/*/*/*/*/*/*.json).toDF(["'ai_observations'", "'ai_observations_detailed_value'", "'ai_observations_extraction_source_method'", "'cargo_list'", "'cargo_list_detailed_value'", ..."]).withColumn(["'obs'", 'obs_BAD_CHAR']).drop(["'obs_BAD_CHAR'"])...)
action: withColumn("'publication_date'", "Column<'CASE WHEN (publication_date IS NULL) THEN publication_date WHEN (publication_date = null) THEN publication_date ELSE translate(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(publication_date, ^\\s*un.?\\s*$, 1, 1), ^\\s*two\\s*$, 2, 1), ^\\s*three\\s*$, 3, 1), ^\\s*four\\s*$, 4, 1), ^\\s*five\\s*$, 5, 1), ^\\s*six\\s*$, 6, 1), ^\\s*seven\\s*$, 7, 1), ^\\s*ocho\\s*$, 8, 1), ^\\s*nueve\\s*$, 9, 1), ^\\s*(\\d+) *y? medi[oa], $1.5, 1), ^\\s*(\\d+) *y? cuarto, $1.25, 1), ^\\s*(\\d+) *(?:y )?1 ?/ ?4, $1.25, 1), ^\\s*(\\d+) *(?:y )?1 ?/ ?2, $1.5, 1), ^\\s*(\\d+) *(?:y )?3 ?/ ?4, $1.75, 1), ^\\s*(\\d+) *(?:y )?1 ?/ ?3, $1.34, 1), ^nu[l1][l1]$, , 1), OoQCIil|!tZzASóG&Bög, 00001111112245666889) END'>")
involved_columns: publication_date
```

The second level provides insight into the reason for the changes—what could be termed a "semantic level." It requires human intervention during configuration, but this approach ensures that all the information necessary to uncover the specific operations applied, the rationale behind them, and the order in which they were executed is stored, thereby maintaining the traceability of the changes. The operations performed are categorized into processes, which are further divided into stages, as shown in the table below:

<table>
<tr>
<th>Stages</th>
<th>Processes</th>
<th>Operations</th>
<th>Description</th>
</tr>
<tr>
<td rowspan='6'>fixed_extraction</td>
<td rowspan='3'>extraction</td>
<td>default_value</td>
<td>Obtained as a default value during the extraction process. Therefore, it is possible that the</td>
</tr> value does not appear in the body of the news item.</td>
</tr>
<tr>
<td>original_value</td>
<td>The value was obtained directly from the extraction.</td>
</tr>
<tr>
<td>calculated_value</td>
<td>The value was obtained through a calculation method; therefore, it may not appear in the body of the news item.</td>
</tr>
<tr>
<td rowspan='2'>boat_fact_model</td>
<td>create unique ID</td>
<td>The value was generated during the ingestion data reading process to ensure that every record has an unambiguous, unique identifier.</td>
</tr>
<tr>
<td>datetime to string date</td>
<td>When the publication date value from the ingestion is in a numeric format—such as a datetime or timestamp—it is converted into a date in string format.</td>
</tr>
<tr>
<td>fix_extraction_inconsistencies</td>
<td>Schema specification ID or '0000', '0001', ...</td>
<td>These are various operations designed to correct extracted data that does not properly fit its assigned category. These operations are defined by the user in the _schema.json_ configuration file under the _fix_extraction_inconsistencies_ attributes.</td>
</tr>
<tr>
<td rowspan='11'>fixed</td>
<td>fill_gaps</td>
<td>Schema specification ID or '0000', '0001', ...</td>
<td>These are various operations designed to correct data—addressing omissions or minor errors. These operations are defined by the user in the _schema.json_ configuration file under the _fill_with_ attributes.</td>
</tr>
<tr>
<td rowspan='8'>fixing</th>
<td>resolve_idems</th>
<td>This operation allows for the resolution of values ​​containing... referenced by the pronouns *idem*, by consulting the values ​​from previous records.</td>
</tr>
<tr>
<td>trim_punctuation</th>
<td>This operation removes extra spaces from the ends.</td>
</tr>
<tr>
<td>numeric_map</th>
<td>This operation converts non-numeric values ​​into their most likely numeric equivalents.</td>
</tr>
<tr>
<td>not_paragraph_map</th>
<td>This operation removes or converts characters that are not supported in fields other than paragraph fields—for example, exclamation marks, question marks, etc.</td>
</tr>
<tr>
<td>one_word_map</th>
<td>This operation removes or converts characters that are not permitted in single-word fields, such as spaces or other separators.</td>
</tr>
<tr>
<td>only_text_map</th>
<td>This operation removes or converts characters that represent numeric digits.</td>
</tr>
<tr>
<td>not_parenthesis_map</th>
<td>This operation removes or converts characters that are parentheses.</td>
</tr>
<tr>
<td>[FIELD_NAME]_map</th>
<td>Operations for cleaning and replacing characters, as defined in the file _config/mapping_to_clean_chars.json_ for the specific field being referenced.</td>
</tr>
<tr>
<td rowspan='2'>calculation</th>
<td>decrement_data</th>
<td>An operation that decrements the date in a specified field by an amount of time—defined as a parameter—which is derived from another field or an expression within the record.</td>
</tr>
<tr>
<td>data_difference</th>
<td>An operation that calculates the time span between two dates contained in two fields of a record, as specified by parameters.</td>
</tr>
<tr>
<td rowspan='4'>cleaned</th>
<td rowspan='4'>cleaned</th>
<td>cast_to_integer</th>
</tr>
<tr>
<td>cast_to_number</th>
<td>An operation that converts the data in a field into a numeric type.</td>
</tr> If it cannot be converted, its value is nullified.</td>
</tr>
<tr>
<td>cast_to_boolean</th>
<td>Operation to convert the field data to a boolean type. If it cannot be converted, its value is nullified.</td>
</tr>
<tr>
<td>...</th>
<td>Operation to perform other type conversions.</td>
</tr>
<tr>
<td>normalized</th>
<td>normalized</th>
<td>normalize_string</th>
<td>Operation to normalize text data by converting it to lowercase and removing special characters.</td>
</tr>
</table>

During the cleaning process, information regarding any changes made is added to each field and record. Specifically, two additional fields are included, with the suffixes '*_history*' and '*_history_sequence*'. The first field stores the final value obtained during the process, along with a list of the operations performed. This data allows for the retrieval of descriptions and details concerning the operations, which are stored in the metadata section. The fields with the '*_history_sequence*' suffix provide information on the execution sequence of processes at each stage, making it easy to recover details about the most recent operation performed at any given stage.

Consider the example of record 'DB_0000233980': during extraction, the port name was captured with extraneous text appended to the end. The cleaning process includes a specific operation—defined in the configuration as "_delete_last_bad_part_" and categorized under the _fill_gaps_ stage—which successfully yields the cleaned port name. Finally, during the normalization process, the port name is converted to lowercase.
```
entry_id: DB_0000233980
...
travel_departure_port_history: {'extraction': Row(obtained_value='Id en d.', operations=['default_value', 'original_value']), 'fill_gaps': Row(obtained_value='Id', operations=['delete_last_bad_part']), 'fixing': Row(obtained_value='Alicante', operations=['resolve_idem'])
'normalized': Row(obtained_value='alicante', operations=['normalize_string'])}
travel_departure_port_history_sequence: Row(fixed_extraction=['extraction'], fixed=['fill_gaps', 'fixing'], cleaned=['fixing'], normalized=['fixing', 'normalized'])
...
```
This record contains only the operations that resulted in changes, making it easy to identify exactly what was modified and why those changes were made. The field with the suffix `_history_sequence` allows us to determine that the extracted value contained in the news item is 'Id' by querying `reg.travel_departure_port_history[reg.travel_departure_port_history_sequence['extraction'][-1]]['obtained_value']`.

Furthermore, as previously mentioned, the details of all operations performed during the cleaning process are also stored in the metadata directory, allowing for the retrieval of their textual descriptions and the specifics of what was executed. If we examine the `_modify_` type operation identified as `_delete_last_bad_part_` for the `_travel_departure_port_` field during the `_cleaning_` stage, we obtain the following information:

```
description: Removes the erroneous text at the end.
detailed_operation: { 
    "type": "replace", 
    "from": { 
        "type": "expr", 
        "value": "regexp_extract(travel_departure_port, '(?i)^(?!en)\\\\s*(\\\\w{2}.*?)(?:\\\\s+en\\\\s+.+\\\\s+[dhm]|:?\\\\s+\\\\w+\\\\s+[dhm]|:?\\\\s+[dhm])?\\\\W*$', 1)" 
    }, 
    "condition": { 
        "and": [ 
            "publication_name = 'DB'", 
            "travel_departure_port rlike '(?i)^(?!en)\\\\s*(\\\\w{2}.*?)(?:\\\\s+en\\\\s+.+\\\\s+[dhm]|:?\\\\s+\\\\w+\\\\s+[dhm]|:?\\\\s+[dhm])?\\\\W*$'"
        ]
    },
    "description": "Deletes the erroneous final text.",
    "id": "delete_last_bad_part",
}

```

The third level is actually a simplification of the second, since instead of storing results with the sequence of operations that obtain them, only the results obtained for each of the 4 stages are stored. This level is useful for speeding up storage in applications where it is important to know the sequence of data in broad strokes (stages), ignoring the reason and the operations that produce them.

### Description of the cleaning processes

There are 9 cleaning processes that should preferably be executed in the order indicated.

1) **Complete the agreed-upon structure**. Calling the `complete_accepted_structure` method and passing it the dataframe to be cleaned ensures that the required fields exist and that the structure of the dataframe matches the schema exactly—ignoring data types and focusing only on field names and nested structures. If a structure different from the expected one is anticipated, the `normalize` attribute can be used to resolve this and adapt the extracted structure to the desired one. This method does not remove fields that are not present in the schema, nor does it modify their content. This allows for the option of saving a copy of the original data using the `save_original_values_of_ship_entries` method, which helps avoid inconsistent reading issues when the extraction process has resulted in fields with varying schemas.
For example, we know that `travel_port_of_call_list` has been extracted using different formats: either as a simple list of ports (an array of strings) or as a list containing port names along with arrival and departure dates for each port (an array of objects with two or three fields). To handle these variations, the `normalize` parameter allows for the specification of multiple alternatives, ensuring the extraction results are adapted to the required structure. The `normalize` configuration supports the following:
```
"normalize": {
    "type": "foreach_item",   // or "single" (default). Use "foreach_item" if the field is an array of structures; use "single" if it is a single structured value. 
    "options": [ ... ],       // A list of potential adaptation strategies. The first successful adaptation is applied. 
    "otherwise": { ... }      // adaptation if all previous attempts fail
}
```

For each option, you can define a _try_cast_to_ using one of the expected structures, along with an _equivalence_ that specifies the mapping between structures (adapting the old structure to the new one). Let’s look at an example:

>As previously mentioned, `travel_port_of_call_list` may arrive in various structures. We will define the adaptation for each of these; if all attempts fail, we will specify how to populate the default structure.

```
    ...
    "travel_port_of_call_list": {
        "type": "array",
        "description": "Lista de puertos donde el barco hizo escala.",
        "items": {
            "type": "object",
            "properties": {
                "port_of_call_place": {
                    "type": "string",
                    "description": "Puerto donde hizo la escala.",
                    "nullable": false,
                    "for_cleaning": ["accepted_idem", "not_digits"]
                },
                "port_of_call_arrival_date": {
                    "type": "string",
                    "format": "date",
                    "description": "Fecha de llegada al puerto de escala.",
                    "nullable": true
                },
                "port_of_call_departure_date": {
                    "type": "string",
                    "format": "date",
                    "description": "Fecha de llegada al puerto de escala.",
                    "nullable": true
                }
            },
            "required": ["port_of_call_place"],
            "additionalProperties": false
        },
        "normalize": {
            "type": "foreach_item",
            "options": [
                {
                    "try_cast_to": {
                        "type": "object",
                        "properties":{
                            "port_of_call_place": {
                                "type": "string"
                            },
                            "port_of_call_arrival_date": {
                                "type": "string"
                            },
                            "port_of_call_departure_date": {
                                "type": "string"
                            }
                        }
                    },
                    "equivalence": {
                        "type": "struct",
                        "new_value": {
                            "port_of_call_place": {
                                "type": "column",
                                "value": "old_value.port_of_call_place"
                             },
                             "port_of_call_arrival_date": {
                                 "type": "column",
                                 "value": "old_value.port_of_call_arrival_date"
                             },
                             "port_of_call_departure_date": {
                                 "type": "column",
                                 "value": "old_value.port_of_call_departure_date"
                             }
                        }
                    }
                },{
                    "try_cast_to": {
                        "type": "string"
                     },
                     "equivalence": {
                         "type": "struct",
                         "new_value": {
                             "port_of_call_place": {
                                  "type": "column",
                                  "value": "old_value"
                             },
                             "port_of_call_arrival_date": {
                                  "type": "literal",
                                  "value": null
                              },
                              "port_of_call_departure_date": {
                                  "type": "literal",
                                  "value": null
                              }
                         }
                     }
                }
           ],
           "otherwise":{
               "equivalence": {
                   "type": "struct",
                   "new_value": {
                       "port_of_call_place": {
                            "type": "literal",
                            "value": null
                        },
                        "port_of_call_arrival_date": {
                            "type": "literal",
                            "value": null
                        },
                        "port_of_call_departure_date": {
                             "type": "literal",
                             "value": null
                        }
                  }
             }
         }
     },
     ...
```

>It should be noted that if the expected structure matches the final structure, it must also be configured as an adaptation option to account for cases where the format is unknown, thereby preventing any failures. This configuration ensures the following:
>If the structures match, each field is copied to its corresponding counterpart. If the input is an array of strings, the resulting value is copied to `port_of_call_place`, while the remaining fields are set to `null`. If neither of these conditions is met, all fields in the structure are set to `null` to avoid a fatal error.

2) **Correcting inconsistent extractions**. The schema file must adhere to JSON schema specifications, subject to certain limitations and supplemented with additional information. Flexible structures—such as those using "allOf," "anyOf," or "if-then" specifications—are not permitted; the final structure must have a single, fixed form. However, the fields within this structure may be enriched with extra information useful for various data-cleaning tasks. The `fix_extraction_inconsistencies` attribute is used to rectify extracted data that contains inconsistent or erroneous values. This attribute employs the same syntax as the `fill_with` attribute; the distinction between them is purely conceptual, and it is up to the user to decide which attribute to use.

3) **Populating field content according to the schema**. The schema file must adhere to JSON schema specifications, subject to certain limitations and supplemented with additional information. Flexible structures—such as those using "allOf," "anyOf," or "if-then" specifications—are not permitted; the final structure must have a single, fixed form. However, the fields within this structure may be enriched with extra information useful for various data-cleaning tasks. The `fill_with_schema` method utilizes the `fill_with` attribute to populate fields that are either empty or contain identified, incorrect data. Any field within the structure that serves as a root field in the schema can be configured with formal specifications that allow _Spark_ processes to populate or correct its content—whether by using a literal value, another column, or a calculation. The _fill_with_ parameter can be an object or an array of objects; using an array allows for the definition of multiple population or correction rules for a single field. Each object must include a _type_ attribute, which can be set to one of the following values: "_replace_", "_replace_if_not_exist_", "_add_array_item_", "_add_array_from_array_", or "_add_array_from_list_". Additionally, a _from_ attribute is required to specify the data source, while an optional _to_ attribute can be used to target a specific part of a structured field when the correction applies only to a portion of that structure. An optional _condition_ attribute may also be used to define the criteria under which the replacement or correction should take place. Let’s look at some examples.
_**Using _replace_** or **_replace_if_not_exist_**_. Both can be used to replace the content of a field with a literal value, the value of another column, or a calculation or expression supported by _Spark_. As the name suggests, _replace_if_not_exist_ only applies the change if the current value of the field (to which the _fill_with_ attribute is attached) is _null_. In contrast, _replace_ will update the value in all cases—unless a condition is specified, in which case it will only apply when that condition is met. Example:

> _If a field name is accidentally changed during the extraction process, this utility can be used to fix it. In fact, some of the extractions specified the unit for the ship's capacity using the field name "ship_tons_units," while in others, it was labeled with the correct name, "ship_tons_unit." The correct rows will contain a value in that field, whereas the incorrect ones—thanks to the aforementioned utility (`complete_accepted_structure`)—will contain a null value. We will use this property to trigger a replacement with the corresponding value from the "ship_tons_units" column._

```
{  
  "$id": "https://portadaproject.eu/ship_entry.schema.json",  
  "$schema": "https://json-schema.org/draft/2020-12/schema",  
  "title": "ship_entry",  
  "type": "object",  
  "properties": {
      ...
     "ship_tons_unit": {  
        "type": "string",  
        "nullable": true,  
        "fill_with": {
            "type": "replace_if_not_exist", 
            "from": "ship_tons_units"
         }  
     },
    ...
  }
}
```

> _The "from" attribute can take other forms to include a literal value or a complex expression. For example, to prevent the trip duration units from being left empty, you can enforce a default value of 'd.':_

```
{  
  "$id": "https://portadaproject.eu/ship_entry.schema.json",  
  "$schema": "https://json-schema.org/draft/2020-12/schema",  
  "title": "ship_entry",  
  "type": "object",  
  "properties": {
      ...
      "travel_duration_unit": {  
          "type": "string",  
          "nullable": true,  
          "fill_with": {
              "type": "replace_if_not_exist", 
              "from": "LIT('d.')"
          }  
      },
      ...
  },
  ...
}
```

> _We can use a regular expression when we want to extract a portion of a value. This is particularly useful because the `__parsed_text__` field contains a large amount of information, which can help us verify whether the extracted data is correct—or allow us to extract the information if it wasn't captured correctly. In some cases, the extraction process has mistakenly identified the departure port name as the vessel type. To remove this erroneous content, we can define a condition using Spark's `rlike` function:_

```
{  
  "$id": "https://portadaproject.eu/ship_entry.schema.json",  
  "$schema": "https://json-schema.org/draft/2020-12/schema",  
  "title": "ship_entry",  
  "type": "object",  
  "properties": {
      ...
      "travel_departure_port": {  
          "type": "string",
          ...
          "fill_with": [
              ...
              {
                  "type": "replace",
                  "from": "LIT('')",
                  "condition":"travel_departure_port rlike '(?i)^\\\\W*(?:(?:bote)|(?:bergant[ií]n)|(?:buque)|(?:balandra)|(?:corbeta)|(?:crucero)|(?:fragata)|(?:goleta)|(?:lugre)|(?:guairo)|(?:m[íi]stico)|(?:jabeque)|(?:pailebot)|(?:la[úu]d)|(?:paquete)|(?:polacra)|(?:queche)|(?:tartana)|(?:transporte)|(?:vapor))(?:(?:\\\\W+)|$).*$'"  
              },
              ...
          ],
          ...
      },
      ...
  }
```
> _The **regexp_extract** function allows us to extract a portion of the content from a column, which can then be used as a correction value. Let’s look at an example. In some cases, the extraction process failed to identify the trip duration, even though it was present in the original text. In the *Diario de Barcelona* entries, the duration is the only temporal reference provided. Therefore, when the text contains a phrase such as "... [numerical value] months (or days, or hours) ..." but the "travel_duration_value" field is empty, we can correct it by extracting that value using a regular expression that captures the duration within a group._

```
{  
  "$id": "https://portadaproject.eu/ship_entry.schema.json",  
  "$schema": "https://json-schema.org/draft/2020-12/schema",  
  "title": "ship_entry",  
  "type": "object",  
  "properties": {
      ...
      "travel_duration_value": {  
          "type": "number",
          ...
          "fill_with": [
              ...
              {  
                  "type": "replace",  
                  "from": {
                      "type": "expr", 
                      "value": "regexp_extract(parsed_text, '(?i)^.{2,78}\\\\s+([\\\\dóoöi|!l&]+)\\\\s*(?:(?:horas?)|(?:d[íi]as?)|(?:meses)|(?:mois)|(?:months?))\\\\W.*', 1)"},  
                      "condition": {  
	                        "and": [  
	                            "(travel_duration_value is null or cast(travel_duration_value as string) rlike '(?i)^(?:(?:\\\\?+)|(?:[\\\\dóoöi|!l&]+ ?[dhm].{0,4})|(?:\\\\d*[óoöi|!l&VftiICálÍüKsSí.ì''dO% ]+))$')",
	                            "parsed_text rlike '(?i)^.{2,78}\\\\s+([\\\\dóoöi|!l&]+)\\\\s*(?:(?:horas?)|(?:d[íi]as?)|(?:meses)|(?:mois)|(?:months?))\\\\W.*'"  
                          ]  
                      }
                  },
              ...
          ],
          ...
      },
      ...
  }
```

> _regexp_extract(parsed_text, '(?i)^.{2,78}\\\\s+([\\\\dóoöi|!l&]+)\\\\s*(?:(?:horas?)|(?:d[íi]as?)|(?:meses)|(?:mois)|(?:months?))\\\\W.*', 1)` allows for the extraction of the content in group 1 for texts that match the specified pattern. This condition will help us perform the change only on records that contain errors and have compatible information.

The *from* attribute supports two formats: text or object. When provided as text, it must specify the name of a field (from which the content is to be copied) or the expression `LIT('[literal value]')` to indicate a literal value. When provided as an object, it accepts a `type` attribute—to specify the nature of the content to be copied—and a `value` attribute—to specify the source from which that content should be extracted. The `type` attribute can be set to one of the following options: "literal" (or the alias "l"), "field" (or the alias "f" or "column"), and "expr". If "literal" is used, the `value` must be a string interpreted as a literal value to be used for substitution. If "field" is used, the text in `value` corresponds to the name of a field (or a subfield, if defined using the `.` separator). Using the "expr" option indicates that the text in the `value` attribute should be interpreted as a valid Spark expression. Consider the following equivalent `_fill_with_` examples:

```
...
    "fill_with": {
        "type": "replace_if_not_exist", 
        "from": "ai_observations"
    }
...
    "fill_with":{
        "type": "replace_if_not_exist", 
        "from": {
            "type":"field",
            "value": "ai_observations"
        }
```

The _condition_ attribute refers to the conditional expression used to select the records to which the substitution should be applied; it can be provided in either text or object format:
1. Text: the condition must be compatible with a Spark SQL expression. For example: `"travel_departure_port = 'Barcelona'"` or `"travel_departure_port rlike '^B.*$'"` or `"travel_duration_value >= 100"`, or even `"(travel_port_of_call_list is null or size(travel_port_of_call_list) = 0)"`.
2. Object: These represent the logical operators of a complex expression formed by a set of conditions combined using _or_ or _and_. Each logical operator will contain an array of the various conditions upon which it operates. These conditions may be expressed as a string or as another logical operator object, allowing them to be combined.

Use of _**add_array_from_array**_: This type of replacement requires that the source data—obtained via the _from_ attribute—already be an array of the same (or a similar) type as the destination. A special attribute called _mode_ can be defined to specify whether the new content should replace the existing content (`mode = replace` or `mode = replace_null`) or whether the new values ​​should be added while preserving the existing ones (`mode = append`). If the _to_ attribute is not specified, the array obtained from _from_ must have the same structure as, or be compatible (castable) with, the structure of the destination array. If _to_ is specified, it allows for the explicit mapping of all or some subfields from the source to the destination. For instance, one could populate a subset of subfields while leaving the others empty, or alter the destination of the subfields (for example, by mapping `cargo.cargo_commodity` to `cargo.cargo_unit` and `cargo.cargo_unit` to `cargo.cargo_commodity`, etc.). Furthermore, a condition can be specified to control which records should be affected by the change. Examples:

> _Assuming that the extraction process added the ports of call for a voyage into a column with an incorrect name, we can perform the change directly (first case). If the format does not exactly match the destination, we use the _to_ attribute (second case)..._ case)._

```
...
    "fill_with": {
        "type": "add_array_from_array", 
        "mode":"replace",
        "from": "travel_port_of_calls",
        "condition": "(travel_port_of_call_list is null or size(travel_port_of_call_list) = 0)" 
    }
...
    "fill_with":{
        "type": "add_array_from_array", 
        "mode": "replace",
        "from": {
            "type":"field",
            "value": "travel_port_of_calls"
        },
        "to":{"port_of_call_place":"port_of_call"}
        "condition": "(travel_port_of_call_list is null or size(travel_port_of_call_list) = 0)" 
    }
```
Using _**add_array_from_list**_. In cases where you only have a list of scalar values ​​(such as a list of strings, numbers, or booleans) to populate the items, you can use this type to extract that list and add the resulting items to the destination.
> _Assuming that a list of scalar values ​​can be extracted from the __parsed_text__, you could obtain the desired list by using Spark’s `split` function. The _to_ attribute specifies the target subfield for the value._

```
...
    "fill_with": [  
        {  
            "type": "add_array_from_list", 
            "mode":"replace", 
            "from": {  
               "type": "expr",  
               "value": "filter(slice(transform(split(regexp_replace(travel_departure_port, '(?i)\\\\s+[yýỳveéè;]\\\\s+', ','), ','), x -> trim(x)), 2, 50), x -> x is not null and length(x) > 0)"  
            },  
            "to": ["port_of_call_place"],  
            "condition": {  
                "and": [  
                    "travel_departure_port rlike '(?iuU)^(?:[\\\\w¡!(). ]+, ?)*[\\\\w¡!(). ]+?(?:(?:(?:(?: +[yýỳv ])|,) +)|(?:eéè i))[\\\\w¡!(). ]+$'",  
                    "(travel_port_of_call_list is null or size(travel_port_of_call_list) = 0)"  
                ]  
            }
        },
        ...
    ]
...
```

Use of _**add_array_item**_. This allows for the generation of a single item from a source. In this case, the source does not provide a collection, but rather a single item to be added to the destination. Example:

> In the "Argentine" node, simple fields were added that needed to be integrated into the structure of the vessel's cargo. For instance, it was agreed that passengers traveling on a ship should be treated as cargo, as they are described as such in the source text (e.g., "with 50 quintals of wheat and 30 passengers"). It was also agreed that ballast should be classified as cargo, given that, in some cases, the ballast was not discarded but rather had an owner and had to be returned as a commodity (e.g., "in ballast for Juan García de Villavicencio"). During the data extraction from Argentine newspapers, a specific field—with a boolean value (true/false)—was used to indicate whether a ship was carrying ballast. Another specific field was used to record the number of passengers on board. To integrate this information into the cargo list, relevant data was added to the "cargo_list" field; thus, if a field named "passengers" or "in_ballast" is detected, the corresponding information is appended as if it were an additional item of cargo._

```
...
    "fill_with": [  
        {  
            "type": "add_array_item",  
            "from": [  
                null,  
                {  
                   "type": "add_array_item",  
                   "to": ["cargo_quantity", "cargo_unit", "cargo_commodity"],  
                   "from": ["passengers", null, "LIT('pasajeros')"]  
                }  
            ],  
            "to": ["cargo_merchant_name", "cargo"]  
        },  
       {  
            "type": "add_array_item",  
            "from": [  
                null,  
               {  
                   "type": "add_array_item",  
                   "to": ["cargo_quantity", "cargo_unit", "cargo_commodity"],  
                   "from": [null, null, "LIT('en lastre')"]  
               }  
            ],  
            "to": ["cargo_merchant_name", "cargo"],  
            "condition": { "column": "in_ballast", "op": "==", "value": { "type": "l", "value": true } }  
       }  
    ]
...
```

It is important to note that, to accommodate nested arrays, the substitution structure can be defined recursively. For example, in the first case mentioned above, the initial `from` takes the value of the fields specified in `to` (`cargo_merchant_name` and `cargo`). In this scenario, the first field becomes `null`, while for the second, a new item is added with values ​​of `null`, `null`, and `"en lastre"` (in ballast) for the subfields of the `cargo_list` subfield—as indicated in the second `to` clause (`cargo_quantity`, `cargo_unit`, `cargo_commodity`).

3) **Deleting records specified in the schema**. Within the schema, an additional attribute—`_delete_if_`—can be defined at the top level to specify the conditions under which records should be deleted. This attribute accepts a string containing a single condition, an object of logical operators (`and`, `or`), or an array of various conditions. Each item in the array is evaluated independently, and the result is equivalent to using a single condition combined with an 'or' operator. Example:

```
{  
  "$id": "https://portadaproject.eu/ship_entry.schema.json",  
  "$schema": "https://json-schema.org/draft/2020-12/schema",  
  "title": "ship_entry",  
  "type": "object",  
  "properties": {
      ...
   },
   ...
  "delete_if": [  
    "parsed_text rlike '(?i)^(?:(?:(?:\\\\W|[^d])[^ed][^el]?)|(?:di[ap]:? )|(?:id.?))?.{0,2}para.*$'",  
    "parsed_text rlike '(?i).*(?:(?:acorazado)|(?:torpedero)).*'"  
  ]  
}
```

The instructions specified in the `delete_if` attribute are executed via the `delete_if_from_schema` method, which takes the dataframe to be cleaned as a parameter and utilizes the schema assigned to the instance.

4) **Removing fields not accepted in the schema**. The `prune_unaccepted_fields` method is used to remove fields that were generated during extraction but are not accepted according to the schema. However, this utility preserves fields containing valuable information regarding the extraction process and the original value. For each field in the schema, the ingestion process adds two additional fields with the suffixes "extraction_source_method" and "detailed_value," respectively. The first indicates the generation method used to obtain the final extracted value, while the second contains a record of the values ​​and methods employed to generate that extracted value. These specific fields are not removed during this process; only fields whose names do not match those defined in the schema (excluding those with the aforementioned suffixes) are removed.

5) **Resolving "idem" values**. The `resolve_idems` method is used to detect values ​​in fields marked with the "accepted_idem" tag—within the schema attribute known as `for_cleaning`—that consist of "idem" or its variations (e.g., "id", "id.", etc.). For each such record and field identified, the system attempts to resolve the value as follows:
1. If the field type is not an array, the system searches for a valid value in the preceding record or earlier records. To locate the records corresponding to previous entries (those that appeared in the newspaper), the dataframe must be sorted by the source publication's name, date, and edition—along with their respective identifiers (`entry_id`)—and partitioned by the same criteria (name, date, and edition). This ensures that "idem" (ditto) values ​​are consistently resolved to the original newspaper and date, maintaining the correct sequence.
2. If a field type is an array, the system first attempts to resolve the valid value by checking preceding items within that array. Only if the value remains unresolved upon reaching the first item does the process proceed as described in step 1. This methodology accounts for nested arrays; if the field is part of an inner (nested) array, the system attempts to resolve the value using the corresponding item from the parent (main) array before falling back to the process described in step 1.

6) **Field Cleaning**. The `_clean_field_values_from_schema_` method is used to clean fields based on their name, data type, and the tags specified in the `for_cleaning` attribute for each field. This utility utilizes a map of characters to be cleaned, which is provided to the `PortadaCleaning` instance beforehand. The field name allows for the definition of specific substitutions for each individual field. The data type determines the specific processing applied—whether numeric, date, or text. For numeric and date fields, the system attempts to convert any non-digit characters into a numeric value, using the `numeric_map` specifications defined in the cleaning map. Text fields are, by default, assumed to contain multiple words but should not include question marks, exclamation points, or other punctuation typical of full paragraphs. However, text fields can undergo additional processing via `for_cleaning` tags, which dictate the specific cleaning operations to be applied. The objective is to standardize text fields based on shared cleaning requirements. Currently, the following tags have been defined:
- The **non_digits** tag allows cleaning of digit characters. It is assumed that in fields where digits are not expected, such as proper nouns, port city names, or ship types, numeric characters are likely an OCR error and are converted to text based on physical similarity.

- The **one_word** tag is used for fields that should contain a single word, such as units of measurement, the role of the person in charge, etc. The process involves removing or converting character types that do not belong to a word, such as spaces, punctuation marks, etc.

- The **paragraph** tag prevents default cleaning. In this case, only leading and trailing spaces are cleaned.

- The **not_parenthesis** tag forces the cleaning of parentheses found within the text.

- The **accepted_abbreviations** tag allows the period at the end of short words (fewer than 5 letters).

- The **avoid_changes_for_keys** tag. It allows you to reserve the content of fields with the values ​​specified in the tag and expressed as parameters using the following syntax: `{"algorithm:avoid_changes_for_keys", "params":["<VALUE1>", "<VALUE2>", ...]}`. For example, if a field were tagged as: `"for_cleaning":["one_word", "not_digits", {"algorithm:avoid_changes_for_keys", "params":["NOT FOUND", "ZERO CONTENT"]}]`, the processes associated with `_one_word` and `_not_digits` would not be applied to fields containing any of the values ​​specified in the tag's parameters.

- The `not_cleanable` tag prevents any changes to fields tagged with this tag.

- The `not_cleanable_if_value_is` tag accepts parameters to specify the value or values ​​that should be preserved from cleaning. Its syntax would be `{"algorithm:not_cleanable_if_value_is", "params":["<VALUE1>", "<VALUE2>", ...]}`.

6) **Calculations**. The `calculate_from_schema_` method applies the calculations specified in the `calculable_if_null_` attribute to nonexistent values. Currently, only two types of calculations are supported: `decrement_data` and `data_difference`. The former calculates the previous date based on a period and a starting date. Its syntax is: `{"operation": "decrement_data", "parameters":{"origin_date": "<COLUMN CONTAINING THE START DATE>", "decrement": "<COLUMN CONTAINING THE DURATION OF THE PERIOD TO BE SUBTRACTED>", "decrement_unit": "<COLUMN WITH THE TIME UNIT APPLIED TO THE PERIOD>"}}`.

The second calculates the time in days elapsed between two dates. Its syntax is: `{"operation": "data_difference", "parameters":{"origin_date": "<COLUMN CONTAINING THE START DATE>", "final_date": "<COLUMN CONTAINING THE FINAL DATE>"}}`.

6) **Convert the values ​​to the final type**. The `_string_to_type_from_schema_` method converts the data in the fields, which were previously in text format, to the data type specified in the schema.

6) **Normalize the text fields**. The `_normalize_strings_for_ship_entries_` method normalizes the text fields by removing special characters and converting all text to lowercase.

## Delta Lake
The data is stored in a Delta Lake organized into the following directories:

- [DELTA LAKE ROOT]/[PROJECT NAME]/ingest/. Data is stored here during the ingestion phase.

- [DELTA LAKE ROOT]/[PROJECT NAME]/bronze/. Data is stored here during the cleaning and normalization phase.

- [DELTA LAKE ROOT]/[PROJECT NAME]/silver/. This is where the data is stored during the disambiguation and curation phase.

[ROOT OF DELTA LAKE]/[PROJECT NAME]/gold/. Here, once the data has been cured and disambiguated, it is processed and dumped into the database along with the verification/revocation rules (manual or automatic locks/links to associate/block citations to the entities that emerged during the disambiguation process) that are decided upon.
