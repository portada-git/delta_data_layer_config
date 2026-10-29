# MANUAL DE CONFIGURACIÓN Y USO DE "PORTADA DATA LAYER"

_"Portada data layer"_ es una biblioteca basada en _Spark_ para facilitar el tratamiento ETL de los datos del proyecto, la configuración común de _Spark_ y la configuración y gestión del _delta lake_ de almacenamiento. Además, _portada data layer_ usa de forma totalmente transparente dataframes específicos que automatizan la creación de metadatos para facilitar el seguimiento de las transformaciones ejercidas sobre los datos originales durante los procesos ETL. Solo es necesario delegar en la biblioteca la creación, recuperación y el almacenaje de los dataframes. Sobre estos dataframes se podrán aplicar todas las funcionalidades de _Spark_ para la transformación y consulta de los datos, sin limitación alguna.

## Configuración inicial 

La biblioteca permite diferentes sistemas de configuración inicial. Las clases para la gestión y almacenamiento de los dataframes se instancian mediante builders que admiten ser configurados mediante métodos específicos o bien mediante un fichero JSON del que recibe la ruta donde se encuentra almacenado.

### Parámetros para el _data lake_

La configuración permite especificar parámetros para el _data lake_:

**El protocolo de almacenamiento para el _data lake_**. Admite "file://",  "hdfs://" u otro protocolo de almacenamiento válido usado para almacenar los datos en el _data lake_. El protocolo definido aquí debe ser compatible con el usado por el data_lake. En el proyecto portada se usa _hadoop_ (_hdfs://_) como protocolo.    

Desde el fichero JSON se usará el atributo _protocol_ o bien el método del mismo nombre para sobrescribir la configuración cuando sea necesario.

**Ruta base del _data lake_**. Indica la ruta base donde se encontrará ubicado nuestro _data lake_ en el sistema de almacenamiento usado (Hadoop, sistema, S3, Azure, ...). Este parámetro establece la ruta raíz a partir de la que se almacenarán los datos del _data lake_.

Desde el fichero JSON se usará el atributo _base_path_ o bien el método del mismo nombre para sobrescribir la configuración cuando sea necesario.

**Nombre del proyecto de los datos del _data lake_**. Indica el nombre del proyecto y se usará, si se ha especificado como parte de la ruta raíz del _delta lake_. En combinación con el parámetro anterior, nos permite compartir un mismo sistema de almacenaje con diferentes _data_lakes_, cada uno configurado con una ruta base y/o nombre de proyecto distintos.

Desde el fichero JSON se usará el atributo _project_data_name_ o bien el método _project_name_ para sobrescribir la configuración cuando sea necesario.

Veamos un ejemplo:

`Ejemplo de fichero  JSON para configurar el protocolo de almacenamiento:`
```
{  
  "protocol": "hdfs://",  
  "base_path": "/app/delta_test/docker",   
  "project_data_name": "portada_project",
  ...
}
```
``Si el fichero se encuentra almacenado en config/data_layer_config.json la llamada de configuración del builder será: `` 
``` python
# configuración del builder mediante un JSON de configuración inicial
builder = PortadaBuilder("config/data_layer_config.json")
# Y podrá sobrescribirse usando el método pertinente
builder = builder.protocol("file://").base_path("/delta_lake").project_name("portada")
```
### Parámetros para _Spark_

La configuración también permite definir parámetros de _Spark_. Todos los parámetros compatibles con Spark se admiten tanto en el JSON como vía llamada a un método del builder. En el fichero JSON, cualquier atributo que comience con el prefijo _"spark."_, se considerará propio de la configuración de una sesión de Spark. La mayoría de parámetros de una sesión de Spark tienen el prefijo "spark.", sin embargo, existen excepciones y versiones que no usan el prefijo. Estas deberán configurarse usando el atributo _"configs"_ del JSON con un formato array de diccionarios con atributos y valores.  Los elementos _key/value_ de _configs_ admiten tanto atributos con prefijo _"spark."_ así como sin prefijo.

Se puede usar la versión de configuración de llamada a un método del builder usando el método llamado _config_. Existen, sin embargo, dos métodos específicos con nomenclatura propia: _master_ y _app_name_. Para el resto de parámetros usaremos siempre el método _config_. 

Se pueden consultar las propiedades de configuración de Spark en la página: [https://spark.apache.org/docs/latest/configuration.html#spark-properties](https://spark.apache.org/docs/latest/configuration.html#spark-properties).

Vemos un ejemplo de fichero JSON:

```
{  
  "protocol": "file://",  
  "base_path": "/app/delta_test/docker",  
  "app_name": "DataLakePortadaCuration_01",  
  "project_data_name": "portada_project",  
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
El cual nos permitirá configurar un _builder_ de _"portada data layer"_ desde el constructor. Como en los casos anteriores, también será posible añadir o sobrescribir aquellos valores que se desee usando llamadas a métodos del builder:
 
``` python
# configuración del builder mediante un JSON de configuración inicial
builder = PortadaBuilder("config/data_layer_config.json")
# Y podrá sobrescribirse usando el mètodo pertinente
builder.app_name("Portada_APP")
       .config("spark.sql.shuffle.partitions", "50")
       .config("spark.driver.maxResultSize", "7g")
       .config("spark.driver.memory", "7g")
```

### Otros parámetros

Las clases de _portada data layer_ en explotación usarán Redis como registro de estados de algunos procesos. Durante el desarrollo y los tests, podrá evitarse su uso mediante el parámetro _use_redis_metadata_. Este parámetro solo puede configurarse mediante llamada al método del mismo nombre para asignar un valor booleano indicando si se desea o no el uso de Redis como registro de estados. Debemos tener en cuenta que, por defecto, el valor de este parámetro es _True_. 

``` python
 builder = builder.app_name("Portada_APP")
```

## Configuración específica para la limpieza de los datos

Dos son los ficheros de configuración necesarios para procesar la limpieza de los datos, uno conteniendo el schema y los procesos que cada campo requerirá y un segundo con un mapa de sustituciones y cambios en los caracteres según tipología. Ambos se cargan directamente en la instancia de la clase que procesará la limpieza (por ejemplo, BoatFactCleaning) como un diccionario JSON antes de iniciar los procesos de limpieza.

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
El fichero con el mapa de sustituciones permite definir la limpieza de los campos de forma genérica en función de su tipo y funcionalidad. Para las fechas y tipos numéricos, el fichero permite la sustitución de valores (del 1 al 9) escritos en letra, por sus correspondientes dígitos numéricos. También asume cambios provocados por errores de OCR, convirtiendo ciertos caracteres en dígitos (O por 0, Z por 2, & por 6, ...), así como valores textuales mixtos en valores íntegramente numéricos (2 y medio por 2.5, 3 y 1/4 por 3.75, ...).  Para cadenas de caracteres, se distinguen entre, campos de una sola palabra, campos de solo texto (sin dígitos),  campos de varias palabras que no son párrafos.  textos que no aceptan paréntesis, etc. Además el fichero admite también sustituciones especificas para un campo concreto identificado por su nombre. 

Dicha sustituciones se realizan mediante dos sistemas: lista de caracteres asociados a los valores de sustitución y expresiones regulares asociadas a sus valores de sustitución. El segundo sistema permite usar grupos para extraer partes del texto original. El mapa se define mediante un mapa  clave-valor donde las claves son los caracteres o expresiones regulares a detectar y los valores el texto por el que sustituir. Las claves  de un sólo carácter se resolverán mediante sustitución del carácter de búsqueda, por el carácter indicado como valor. Las claves de longitud mayor serán tomadas como expresiones regulares y su valor sustituirá el contenido de los campos que coincidan con el patrón de la expresión.

Ejemplos:
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

El fichero con el schema debe seguir la sintaxis de un schema JSON con algunas variaciones que se comentarán más adelante. El schema debe contener la estructura final deseada, no la original obtenida de la extracción, sino la que deseamos tener una vez aplicada la limpieza. Los datos originales procedentes de la ingestión contendrán todos los campos extraídos en un tipo _string_, con independencia del tipo representado. 

Las variaciones aceptadas permitirán identificar y definir qué operaciones de limpieza deberán aplicarse en cada campo. El atributo "*type*" permite identificar el tipo requerido al final de la limpieza. El atributo "*for_cleaning*" indicará qué mapas de sustitución deberán usarse cuando para la limpieza del campo cuando este sea de tipo string (_not_digits_,  _not_parenthesis_, _one_word_, ...). El atributo "_calculable_if_null_" permite establecer un conjunto reducido de operaciones (calculo de una fecha anterior usando un periodo de tiempo y cálculo del periodo existente entre dos fechas) a realizar sobre los campos así etiquetados. Finalmente, el atributo "*fill_with*" permite configurar una limpieza específica para modificar los campos que cumplan ciertas condiciones con expresiones, valores de otras columnas, valores literales, etc. El resultado de este último atributo, podría sustituir al atributo  "_calculable_if_null_" pero el primero requiere mayor conocimiento y se define con mucha más complejidad, por ello se mantiene, e incluso se puede valorar añadir más operaciones.  
A nivel raíz del schema se permite definir también el atributo _delete_if_ en el qual se pueden especificar las condiciones de eliminación de aquellos registros que se deseen borrar y el atributo _order_and_partitions_ para especificar el orden y particiones usadas perra poder resolver los valores con "idem".   Ejemplos:
```
{
    ...
        "publication_edition": {  
           "type": "string",  
      	   "description": "Edición en la que se publicó la noticia de la entrada.",  
          "minLength": 1,  
          "maxLength": 1,  
          "nullable": false,  
          "for_cleaning":["not_cleanable"]
        },
        ...
        "travel_departure_date": {  
           "type": "string",  
           "description": "Fecha de salida del puerto en formato YYYY-MM-DD.",  
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
            "description": "Tipo de embarcación.",  
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
Cada uno de los procesos de limpieza extraerá la información pertinente de la configuración y actuará en consecuencia. Existen 9 procesos de limpieza que deben ejecutarse preferiblemente en el orden indicado. 

1) **Completar la estructura acordada**. Llamando al método _complete_accepted_structure_ y pasándole el dataframe a limpiar, forzará la existencia de los campos y forzará que la estructura del dataframe pasado por parametro, coincida exactamente con el schema sin tener en cuenta los tipos de datos, solo se fuerzan los nombres y las estructuras compuestas. En caso de prever una structura diferente a la esperada, se usará el atributo "_normalize_" para resolver el problema y adaptar la estructura de la extracción a la deseada.  Este método no elimina los campos que no figuren en la estructura del schema, ni modifica el contenido de los mismos. Esto permite, si se desea, guardar una copia de los datos originales usando el método _save_original_values_of_ship_entries_ evitando problemas de lectura incoherente cuando la extracción haya producido campos con distintos schemas.  
Por ejemplo sabemos que _travel_port_of_call_list_, se ha extraído con diferentes formatos: solo lista de puertos (array de textos) o lista con nombres de puerto y fechas de llegada y salida de cada puerto (array de objetos con dos o tres campos). Para resolver las diferentes alternativas, _normalize_ permite indicar diversas alternativas para conseguir adaptar los resultados de la extracción a la estructura deseada. la configuración de _normalize_ admite:
```
"normalize": {
  "type": "foreach_item",   // o "single" (defecto). "foreach_item" se usa si el campo és un array de estructuras. Se uarà "single" si se trata de un valor estructurado.
  "options": [ ... ],       // lista de intentos de adaptaciones. Se queda con la primera adaptación que funcione.
  "otherwise": { ... }      // adaptación si todos los intentos anteriores fallan
}
``` 
En cada una de las opciones se permite definir un _try_cast_to_ con una de las estructuras esperadas y un _equivalence_ que indicará la correspondencia entre estructuras (adaptación de la antigua a la nueva). Veamos un ejemplo:

>Como ya hemos indicado, travel_port_of_call_list, puede llegar con multiples estructuras. Para cada una de ellas indicaremos su adaptación y en caso de fallar todas, definiremos como rellenar la estructura por defecto. 

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

>Cabe observar que si la estructura esperada coincide con la estructura final, debe configurarse también como opción de adaptación para poder definir una alternativa de formato desconocido y evitar cualquier fallo. Con esa configuració lo que se pretende es:
>Si la estuctura coincide, cada campo se copia al correspondiente. Si lo que llega és un array de strings, el valor obtenido se copia a _port_of_call_place_ i el resto se deja a null. En caso de no coincidir ninguna de las anteriores, para evitar un error fatal se define que deberan llenarse todos los datos de la estructura a null.

 2) **Completar el contenido de los campos de acuerdo a lo indicado en el schema**. El fichero con el schema debe cumplir las especificaciones de un schema JSON con ciertas limitaciones y con información ampliada. No se admiten estructuras flexibles con especificaciones tipo "allOf", "anyOf" ni "if-then". La estructura final solo debe tener una única forma. Sin embargo, los campos de la estructura podrán ser enriquecidos con información extra que será útil para las distintas utilidades de la limpieza. En el método _fill_with_schema_ se usará el atributo _fill_with_ para llenar los campos sin contenido o con contenido incorrecto reconocido. Cada campo de la estructura que sea un campo raíz del schema admite la información formal que permite ejecutar processos de _Spark_ para completar o corregir el contenido de los campos donde esté definido, usando un valor literal, una columna o un cálculo.  EL contenido de _fill_with_ puede ser un objeto o un array de objetos. La forma de array permite definir para un mismo campo múltiples sentencias de relleno/corrección. Los objetos deben disponer de un atributo _type_ el cual puede contener uno de estos valores:   "_replace_", "_replace_if_not_exist_", "_add_array_item_",  "_add_array_from_array_" o "_add_array_from_list_" . También debe tener el atributo _from_ para indicar el origen de los datos a rellenar y opcionalmente el atributo _to_ para indicar donde aplicar el relleno/corrección de los datos cuando se trate de un campo estructurado y la corrección ataña solamente a una parte de la estructura. Opcionalmente se usará el atributo _condition_ para establecer  la condición bajo la cual se realizará el remplazo o la corrección. Veamos algunos ejemplos.
_**Uso de _replace_** o **_replace_if_not_exist_**_.  Pueden usarse indistintamente para reemplazar el contenido de un campo por un valor literal, por el valor de otra columna o por un cálculo o expresión soportada por _Spark_. Como su nombre indica, _replace_if_not_exist_ realizará el cambio solamente en aquellas fila cuyo valor del campo al que pertenece el atributo _fill_with_ sea _null_, mientras que _replace_ cambiará el valor en todos los casos si carece de condición o en los casos que cumplan la condición si la hubiese . Ejemplo: 

 > _Si durante el proceso de extracción se cambia el nombre de una campo por error, puede arreglarse mediante esta utilidad.  De echo, algunas de las extracciones indicaron la unidad de la capacidad el barco con el nombre del campo "ship_tons_units", pero en el resto fue etiquetado con el nombre correcto de   "ship_tons_unit". Las filas correctas contendrán el valor en dicho campo, mientras que las incorrectas, gracias a la utilidad anterior (complete_accepted_structure), contendrán un valor nulo. Usaremos esta propiedad para indicar que se reemplace por el correspondiente valor da la columna "ship_tons_units"._
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
 > _El atributo "from" podría tomar otras formas a fin de añadir un literal o una expresión compleja. Por ejemplo, para evitar que las unidades de duración del viaje queden vacías, Se puede forzar que por defecto tomen el valor 'd.'  :_
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

> _Podemos usar una expresión regular cuando deseemos extraer una parte de un valor.  Esto se hace especialmente útil debido a que el campo __parsed_text__  contiene gran parte de la información y nos puede ayudar a verificar si la información extraída es correcta o a extraer la información si esta no lo fuera. En algunos casos la extracción ha confundido el nombre del puerto de salida con el tipo de barco. Para eliminar el contenido erróneo podemos definir una condición usando la función **rlike** de Spark:_
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
> _La función **regexp_extract** nos permite extraer el contenido parcial de una columna permitiendo usarlo como valor de corrección.  Veamos un ejemplo. En ciertas ocasiones, la extracción no fue capaz de discernir la duración del viaje a pesar de estar presente en el texto original. En las entradas del Diario de Barcelona no existe otra referencia temporal que la duración. Por tanto, cuando en el texto se indica "... [valor numérico] meses (o días u horas) ... " pero en el campo "travel_duration_value", no incluye valor, podremos corregirlo extrayendo su valor mediante una expresión regular que encierre la duración en un grupo._ 
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
>_regexp_extract(parsed_text, '(?i)^.{2,78}\\\\s+([\\\\dóoöi|!l&]+)\\\\s*(?:(?:horas?)|(?:d[íi]as?)|(?:meses)|(?:mois)|(?:months?))\\\\W.*', 1) permite extraer el contenido del grupo 1 para aquellos textos que cumplan el patrón indicado. La condición nos ayudará a realizar el cambio, solamente en los registros con faltas y con información compatible._

El atributo *from* admite dos formatos, texto u objeto. Como texto debe indicar el nombre de un campo (del que copiar su contenido) o la expresión *LIT('[valor literal]')* para indicar un literal. Como objeto admite un atributo _type_ para indicar la tipología del contenido a copiar y el atributo "value" para indicar la fuente de la que extraer dicho contenido. El valor de "type" se puede escoger entre las opciones "literal" (o el alias "l"), "field" (o el alias "f" o "column") y "expr". Usando "literal", el valor deberá ser una cadena que se entenderá como un valor literal a sustituir. El uso de "field" indicará que el texto de  "value" corresponderá al nombre de un campo (o subcampo si se define usando el separador [.]). Usar la opción "expr" indica que el texto del atributo "value", deberá interpretarse como una expresión valida usando Spark.     Sirva de ejemplo los siguientes _fill_with_ equivalentes:
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
El atributo _condition_ hará referencia a la expresión condicional que permita seleccionar los registros sobre los que deba aplicarse la sustitución y puede aceptar los formatos de texto u objeto:
 1. Texto: la condición debe ser compatible con una expresión sql de spark. Por ejemplo: `"travel_departure_port = 'Barcelona' "` o `"travel_departure_port rlike "'^B.*$'"` o `"travel_duration_value >=100"`o también   `"(travel_port_of_call_list is null or size(travel_port_of_call_list) = 0)".`
 2. Objeto: Representan los operadores lógicos de una expresión compleja formada por un conjunto de condiciones operadas mediante _or_ o _and_. Cada operador lógico contendrá un array con las diferentes condiciones sobre las que operará. Dichas condiciones se podrán expresar como una cadena o como otro objeto de operadores lógicos a fin de combinarlas.

Uso de _**add_array_from_array**_.  Este tipo de reemplazo  requiere que los datos fuente, obtenidos des de el atributo _from_, sean ya un array del mismo tipo que el destino o parecido.  Permite definir una atributo especial llamado _mode_ para indicar si el contenido nuevo eliminará el antiguo (mode = replace o mode = replace_null) o añadirá los nuevos valores manteniendo los antiguos (mode = append). Si no se especifica el atributo _to_ el array obtenido con _from_ debe tener la misma estructura o bien ser compatible (casteable) con la estructura del array destino. Si se especifica _to_ permite asignar explícitamente todos o algunos subcampos de la fuente al destino. Por ejemplo, se podría llenar un subconjunto de subcampos dejando el resto vacíos o bien alterar el destino de los subcampos (haciendo quizás que cargo.cargo_commodity sea igual a cargo.cargo_unit y cargo.cargo_unit sea igual a cargo.cargo_commodity, etc. Además se puede especificar la condición para controlar en qué registros deberá hacerse el cambio. Ejemplos:

> _Suponiendo que la extracción haya añadido las escalas de un viaje en una columna con nombre equivocado, podemos realizar el cambio directamente (primer caso). Si el formato no coincide exactamente con el destino usaremos el atributo _to_ (segundo caso)._
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
Uso de  _**add_array_from_list**_. En el caso que, para llenar los items dispongamos solamente de una lista de valores escalares (lista de strings, de números o booleanos), usaremos este tipo para extraer la lista y añadir los items resultantes en el destino. 
> _Suponiendo que del __parsed_text__ se pueda extraer la lista de escalas, conseguiríamos la lista deseada usando la función split de Spark. El atributo _to_ indica el subcampo destino del valor ._
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
Uso de _ **add_array_item**_. Permite generar un solo item a partir de la fuente. En este caso la fuente no presenta una colección sino un solo item que se añadirá al destino. Ejemplo:

> _En el nodo "argentino", Se añadieron campos simples que debían estar integrados en la estructura de la carga el buque. Por ejemplo, se consensuó que los pasajeros que viajaban en un buque se contemplarían como carga ya que aparecen como tal en la descripción de la misma ("con 50 quintales de trigo y 30 pasajeros"). También se  consensuó que el lastre debía indicarse como mercancía ya que a veces se usaba lastre que no se desechaba sino que tenia propietario y debía devolverse como una mercancía ("en lastre para Juan Gracía De Villavicencio"). En la extracción de los periodicos argentinos se indicó en una campo específico con un valor de (true/false) si el barco entraba en lastre. También se indicó en un campo específico el número de pasajeros que viajaban en la embarcación. Para conseguir integrar dicha información en un item de la lista de la carga, se añadió información pertinente en el campo "cargo_list" de manera que si encuentra un campo llamado "passangers" o "in_ballast", añadan la información relativa como si fuera una carga más._
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
 Es importante destacar que para adaptarse a los arrays anidados, es posible definir recursivamente la estructura de sustitución. Por ejemplo, en el primer caso anterior, el primer from toma el valor de los campos indicados en to ( cargo_merchant_name y cargo) de forma que el primero valga null pero en el segundo se añada un nuevo item con valores null, null y "en lastre" para los subcampos del subcampo cargo_list indicador en el segundo to (cargo_quantity, cargo_unit, cargo_commodity).   

3) **Eliminar los registros indicados en el schema**. En el schema se pude definir una atributo extra, en el primer nivel, llamado _delete_if_ donde se pueden indicar las condiciones bajo las que se eliminaran los registros. Este atributo admite un string con una única condición, un objeto de operadores lógicos (and, or) o un array de distintas condiciones. Cada ítem del array se toma de forma independiente y el resultado seria equivalente a usar una única condición operada con 'or'. Ejemplo:

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
Las indicaciones del atributo _delete_if_ se ejecutaran con el método _delete_if_from_schema_ al cual se le pasará por parámetro el dataframe a limpiar y usará el schema asignado a la instancia. 

4) **Eliminar los campos no aceptados en el schema**. El método _prune_unaccepted _fields_ se usa para eliminar los campos generados en la extracción pero no aceptados en el schema. Con todo, esta utilidad mantiene los campos de información valiosa referida a la generación de la extracción y su valor original. Para cada campo del schema la ingestión añadió dos campos extras con los sufijos "extraction_source_method" y  "detailed_value" respectivamente. En el primero se indica el método de generación con la que se obtuvo el valor final extraído. El segundo incluye una relación de valores y métodos usados para generar el valor extraído.  Dichos campos no se eliminaran en este proceso. Aquí solamente se eliminan los campos cuyo nombre no coincida con los definidos en el schema (sin tener el cuenta los de los sufijos). 

6) **Resolución de valores "idem"**. El método _resolve_idems_ se usa para detectar los valores de los campos marcados con la etiqueta "accepted_idem" en el atributo del schema llamado _for_cleaning_, cuyo contenido sea idem o sus variaciones (id, id., etc.). Para cada uno de los registros y campos encontrados se intentan resolver de la siguiente manera:
    1. Si el tipo del campo no es un array, se busca en el registro anterior o predecesores, un valor válido. Para encontrar los registros anteriores correspondientes a las entradas previas reales (las que figuraban en el periódico, el dataframe debe estar ordenado por nombre, fecha y edición de la publicación fuente además de sus identificadores (entry_id) y particionado por nombre, fecha y edición de la publicación fuente de manera que los valores ídem se resuelvan siempre en el periodico y fecha original y en el orden preciso.
    2. Si el tipo de campo es un array, antes de proceder como el proceso anterior, el valor válido se intenta descubrir entre los items anteriores del array. Solamente si al llegar al primer item, el valor se encuentra todavía sin resolver se procede como en el paso 1. Esta metodología tiene en cuenta el la anidación de arrays, por lo que si se trata de un array interno, antes de pasar al proceso 1. se intenta resolver con el ítem superior del array principal.  

6) **Limpieza de campos**. El método _clean_field_values_from_schema_ se usa para limpiar los campos de acuerdo con el nombre del campo, el tipo de dato y  las etiquetas indicadas en el atributo _for_cleaning_ de cada campo. Esta utilidad usa el mapa de caracteres a limpiar pasado a la instancia PortadaCleaning con anterioridad. El nombre del campo permite definir sustituciones específicas para cada uno de ellos. El tipo responde a tratamientos específicos según su tipo, numérico, fecha o texto. Con los numéricos y fechas se intenta convertir cualquier cosa que no sea dígito en un valor numérico usando las especificaciones _numeric_map_ del mapa de limpieza. Los de tipo texto, por defecto, se suponen que admiten varias palabras pero no interrogantes, ni admiración u otros caracteres de texto propios de párrafos. Sin embargo, el tipo texto admite procesos extras usando las etiquetas _for_cleaning_, las cuales indican los diferentes tratamientos que deberán aplicarse. La idea es unificar los campos de tipo texto según las necesidades de limpieza que tengan en común. Actualmente se han especificado las siguientes etiquetas:
   - Etiqueta **non_digits**. Permite limpiar caracteres de tipo dígitos. Se supone que en campos donde no se esperan dígitos, como nombres propios,  los nombres de ciudades portuarias, o la tipología de barcos, los caracteres numéricos deben ser un error de OCR y se intentan convertir a texto a partir del parecido físico.  
   - Etiqueta **one_word**. Se usa para campos que deban contener una única palabra, como las unidades de medida, el role de la persona al mando, etc.  El tratamiento consiste en eliminar o convertir tipos de caracteres no pertenecientes a una palabra como espacios, signos de puntuación, etc.
   -  Etiqueta **paragraph**. Impide la limpieza por defecto. En este caso solamente se limpian los espacios iniciales y finales.
   - Etiqueta **not_parenthesis**. Fuerza la limpieza de paréntesis encontrados entre el texto.
   -   Etiqueta **accepted_abbreviations**. Admite el punto final en palabras cortas (de menos  de 5 letras). 
   - Etiqueta **avoid_changes_for_keys**. Permite reservar el contenido de loa campos con aquellos valores indicados en la etiqueta y expresados como de parámetros usando la sintaxis siguiente: `{"algorithm":"avoid_changes_for_keys", "params":["<VALOR1>", "<VALOR2>", ...]}`. Si por ejemplo un campo estuviera etiquetado como: `"for_cleaning":["one_word", "not_digits", {"algorithm":"avoid_changes_for_keys", "params":["NOT FOUND", "ZERO CONTENT"]}]`, no se aplicarían los procesos asociados a _one_word_ ni a _not_digits_ en los campos que contengan alguno de los valores indicados en los parámetros de la etiqueta.
   - Etiqueta **not_cleanable**. Evita cualquier cambio en los campos así etiquetados.
   - Etiqueta **not_cleanable_if_value_is**. Esta etiqueta admite parámetros para indicar el valor o valores que deberán preservarse de la limpieza. Su sintaxis sería `{"algorithm":"not_cleanable_if_value_is", "params":["<VALOR1>", "<VALOR2>", ...]}`.

  6) **Cálculos**. El método _calculate_from_schema_ aplica los calculos especificados en el atributo _calculable_if_null_ para aquellos valores inexistentes. Actualmente solo se admiten dos tipos de cálculos: "decrement_data" y "data_difference". El primero calcula la fecha previa a partir de un periodo y una fecha inicial. Su sintaxis es: `{"operation": "decrement_data", "parameters":{"origin_date": "<COLUMNA DONDE SE ENCUENTRA LA FECHA INICIAL>", "decrement": "<COLUMNA DONDE SE ENCUENTRA LA DURACIÓN DEL PERIODO a RESTAR>", "decrement_unit": "<COLUMNA CON LA UNIDAD DE TIEMPO APLICADO AL PERIODO>"}}`. 
  El segundo calcula el tiempo en días, transcurrido entre dos fechas. Su sintaxis es: `{"operation":  "data_difference", "parameters":{"origin_date": "<COLUMNA CON LA FECHA INICIAL>", "final_date":  "<COLUMNA CON LA FECHA FINAL>"}}`.

 6) **Convertir los valores al tipo final**. El método _string_to_type_from_schema_ convierte los datos de los campos, que hasta ahora estaban en formato texto al tipo de dato indicado en el schema.

 6) **Normalizar los campo de texto**. El método _normalize_strings_for_ship_entries_ aplica una normalización en los campos de formato texto eliminando caracteres especiales y pasando toto el texto a minúsculas.

## Delta Lake
Los datos se guardan en un Delta Lake organizado en los siguientes directorios:
 - [RAÍZ DEL DELTA LAKE]/[NOMBRE DEL PROYECTO]/ingest/. Aquí se guardan los datos en la fase de ingestión
 - [RAÍZ DEL DELTA LAKE]/[NOMBRE DEL PROYECTO]/bronze/. Aquí se guardan los datos durante la fase de limpieza y noirmalización
 - [RAÍZ DEL DELTA LAKE]/[NOMBRE DEL PROYECTO]/silver/. Aquí se guardan los datos durante la fase de desambiguación y curación
 - [RAÍZ DEL DELTA LAKE]/[NOMBRE DEL PROYECTO]/gold/. Aquí, una vez curados y desambiguados los datos datos se procesan  para volcar-los en la base de datos junto con las reglas de verificación/revocación (bloqueos/links manuales o automáticos para asociar/bloquear las citas a las entidades emergidas en el proceso de desambiguación) que se decidan. 

## Principales clases y principales métodos para la curación de los datos

_Portada data layer_ dispone de las siguientes clases principales para procesar la curación de los datos del proyecto:
1) Fase de Ingestión:
    1) _BoatFactIngestion_ para la ingesta de las entradas de barcos.
    Métodos:
          1) _**copy_ingested_raw_data**( local_path, user)_ copiará el fichero íntegro (el JSON subido al servidor) en el delta lake, en el directorio de la fase de ingestión (*ingest*) y en la subruta *ingested_files/ship_entries/[USER]/[FILE]*. El método devuelve los datos almacenados en forma de diccionario JSON y la ruta en la que se han almacenado.
          2) _**save_raw_data**( diccionario_JSON, user )_ almacenará los datos copiados en el proceso anterior clasificados por periódico, año, mes, día, edición en un formato JSON de Spark (cada registro ocupa una línea).  El directorio será también el de la fase de ingestión pero con la subruta *sip_entries/[INICIALES PERIÓDICO (BP/DB/DM/EN/GM/LP/SM)]/[AÑO]/[MES]/[DÍA]/EDICIÓN]/[FILE]*. Los datos del JSON original, además de los datos extraídos, pueden mantener meta-información sobro la forma como se ha obtenido el dato de extraído, indicando por ejemplo que corresponde a un valor por defecto, a una extracción directa o a una extracción con un cálculo posterior. A este nivel, los campos con meta-información existente, dispondrán de 2 campos extras:
             - [NOMBRE_ORIGINAL_DEL_CAMPO]_original_data_method. Indica el nombre del método u operación con el que se ha obtenido/calculado el valor del campo considerado el valor original de la fuente. 
             - [NOMBRE_ORIGINAL_DEL_CAMPO]_detailed_value. Es una lista de claves y valores en los que las claves son el nombre del método y los valores los resultados del campo obtenidos mediante la operación referida por la clave (valor por defecto, extracción directa o cálculo en la extracción).         
          3) _**fast_save_raw_data**( diccionario_JSON, user )_. Se trata de una funcionalidad idéntica a la anterior  pero mucho más eficiente. Se encuentra en periodo de pruebas.
          4) _**read_raw_data**( publication_name, year, month, day, edition, user )_. Este método lee en los ficheros almacenados con _save_raw_data_. Todos los parámetros son opcionales.

    2) _KnowEntitiesIngestion_ para la ingesta de las listas de entidades conocidas.
    Métodos:
        1) _**copy_ingested_entities**(entity_type, local_path)_ copiará el fichero íntegro (el YAML subido al servidor) en el delta lake, en el directorio de la fase de ingestión (*ingest*) y en la subruta *ingested_files/known_entities/[TIPO DE ENTIDAD CONOCIDA (commodity, flag, master_role, ...)]/[FILE]*. El método devuelve los datos almacenados en forma de diccionario y la ruta en la que se han almacenado.
        2) _**save_raw_entities**(entity_type, diccionario)_ Guarda en formato parquet el diccionario de entidades conocidas. Cada registro tiene 2 campos: name (que es clave primaria) y voices (un lista con cada una de las voces correspondientes al nombre.   El directorio será también el de la fase de ingestión pero con la subruta *known_entities/[TIPO DE ENTIDAD CONOCIDA (commodity, flag, master_role, ...)]*. 
        3) _**read_raw_entities**( entity_type)_. Este método lee los datos del tipo de entidad conocido indicado, que se guardaron con _save_raw_entities_. 
    3) _ReviewedEntiresIngestion_ para la ingesta de la muestra de datos desambiguados manualmente.
    Metodos:
        1) _**copy_ingested_reviewed_entries**( local_path. is_cargo )_. Copia el fichero en formato csv con la revisión manual de la muestra desambiguada manualmente en el directorio  de la fase de ingestión (*ingest*) y en la subruta *ingested_files/reviewed_entries/ship_entries/[FILE]* si se trata de entradas o en *ingested_files/reviewed_entries/cargo_ship_entries/[FILE]* si se trata de la carga. El parámetro is_cargo permite distinguir entre los datos desambiguados de las entradas y las del cargo_list. El método devuelve los datos almacenados en forma de diccionario y la ruta en la que se han almacenado.
        2) _**save_raw_reviewed_entries**( diccionario, is_cargo)_ Almacena en formato parquet una copia de las revisiones realizadas manualmente indexadas por la clave _temp_key_ con campos: "temp_key", "rev_travel_departure_date",  "rev_travel_duration_value",  "rev_travel_duration_unit",  "rev_travel_departure_port",  "rev_travel_arrival_date",  "rev_ship_type",  "rev_ship_name",  "rev_ship_tons_capacity",  "rev_ship_tons_unit",  "rev_ship_flag",  "rev_master_role", "rev_master_name" si se trata de entradas y con campos: "temp_key",  "cargo_merchant_id",  "cargo_commodity_id",  "rev_cargo_merchant_name",  "rev_cargo_commodity",  "rev_cargo_unit" si se trata de la carga. El directorio será también el de la fase de ingestión pero con la subruta *reviewed_entries/ship_entries*, si se trata de entradas y *reviewed_entries/cargo_ship_entries* si son carga.
        3) _**read_raw_reviewed_entries**(is_cargo)_ Leerá los datos almacenados por _save_raw_reviewed_entries_ distinguiendo entradas de la carga mediante el parámetro _is_cargo_.  
        
2) Fase de limpieza;
    1) _BoatFactCleaning_ Para la limpieza tanto de las entradas de buques como de las entidades conocidas.
    Métodos:
        1) _**use_schema**( diccionario_JSON_con_schema ). Asigna el diccionario con el schema a la instancia que procesará la limpieza.
        1) _**use_mapping_to_clean_chars**( diccionario_JSON_con_mapa_caracteres ). Asigna el diccionario con los mapas de caracteres para realizar la limpieza.
        1) _**read_raw_entries**( force_all )_. Lee los datos almacenados en la fase de ingestión. Les registros ya procesados una vez finalizado todo el proceso de limpieza, se marcaran mediante una marca que indicará que estan límipios de manera que el proceso afecte solamente a los registros no procesados todavía. Sin embargo, usando el parámetro   _forca_all_ se podrá indicar que se desea volver a procesar todos los registros incluidos los que ya se hayan limpiado.
        1) _**complete_accepted_structure**( raw_dataframe)_. Asegura que la estructura mínima del dataframe coincide con la descrita en el schema. Devuelve el dataframe corregido. Este proceso solo añade los campos y estructuras definidos en el schema, que no existan. No elimina los campos extras que no aparecen en el. Además, como no todos los ficheros con los datos extraídos contienen meta-información relativa al proceso de extracción, en este proceso se añade y normaliza de manera que todos los campos del schema dispongan de ella.
        2) _**fix_field_consistency_errors**( dataframe )_. En diseño. Necesario si se quiere mantener como metadato el valor de extracción original coherente en lugar del erróneo. Este método contendría la parte de fill_with correspondiente a procesos de corrección de la coherencia de los campos.
       2) _**save_original_values_of_ship_entries**( dataframe )_. Guarda los datos originales de las entradas con las correcciones realizada hasta el momento en la fase de limpieza ("bronze") en la subruta "original_data/ship_entries" y organizados por periódicos.
       2) _**read_original_values_of_ship_entries**( force_all )_ Lee los registros aun no limpios del dataframe guardado mediante el método anterior. Puede forzarse la lectura de todos los registros mediante el parámetro _force_all_.
        3) _**fill_with_schema**( dataframe )_. Completa o corrige los campos del dataframe usando la configuración del atributo _fill_with_ de cada campo del schema. Devuelve el dataframe corregido.
        1) _**delete_if_from_schema**( dataframe )_. Atendiendo al atributo _delete_if_ del schema, eliminarà los registros que cumplan las condiciones indicadas.
        1) _**prune_unaccepted_fields**( dataframe, simplify_meta_fields)_ Se desprende de los campos no aceptados en el schama a execpción de los metadatos incluidos en el proceso de ingestión para controlar la trazabilidad. El parametro _simplify_metafields_ permite indicar si se desea simplificar las estructuras de los campos con meta-información (sufijos "detailed_value" y "origianl_data_method") reduciendo la información a un solo campo con sufijo  "original_value" el cual contendrá solo el valor y se perderá la información del método usado para conseguir dicho valor.
         1) _**resolve_idems**( dataframe )_. Procesa los campos indicados medianet la etiqueta _accepted_idem_ en el atributo _for_cleaning_ que contengan ídem o sinónimos recuperando los valores de los registros antecesores.
         1) _**clean_field_values_from_schema**( dataframe )_. Realiza una limpieza de los campos segun su nombre,  tipo y etiquetado del atributo _for_cleaning_ en concordancia del los mapas de limpieza de caracteres.
         1) _**calculate_from_schema**( dataframe ). Realiza los cálculos predefinidos (_data_difference_ o _decrement_data_) indicados en los campos con atributo _calculable_if_null_ para los registros sin valor.
         1) _**string_to_type_from_schema**( dataframe )_. Convierte los campos al tipo indicado en el esquema. Hasta aquí todos los datos simples se han conservado con formato de texto con independencia del tipo final. Además añade el valor del campo a uno extra llamado [NOMBRE_ORIGINAL_DEL_CAMPO]_cleaned. 
         1) _**normalize_strings_for_ship_entries**( dataframe )_ . Normaliza los datos eliminado o convirtiendo caracteres superiores al código 128 en otros compatibles, convierte todas la mayúsculas a minúsculas.  Además añade el valor del campo a uno extra llamado [NOMBRE_ORIGINAL_DEL_CAMPO]_normalized. 
         1) _**save_ship_entries**( dataframe, is_cleaned )_. Guarda el dataframe en la fase de limpieza ("bronze) en la subruta "ship_entries". Los datos se organizan por periódicos para facilitar el tratamiento. Si el parámetro is_cleaned es true, al guardar los registros se marcan como limpios. 
         1) _**read_ship_entries**()_. Lee los registros limpios almacenados con el método anterior.
         1) _**delete_ship_entries_by_ids**( ids )_. Elimina los registros contenidos en la lista de identificadores pasados por parámetro.
         1) _**normalize_strings_for_entities**( dataframe )_. Normaliza los datos de las entidades conocidas, eliminado o convirtiendo caracteres superiores al código 128 en otros compatibles, convierte todas la mayúsculas a minúsculas.
         1) _**normalize_strings_for_reviewed_ship_entries**( dataframe )_.  Normaliza los datos de las revisiones de la muestra desambiguadas manualmente, eliminado o convirtiendo caracteres superiores al código 128 en otros compatibles, convierte todas la mayúsculas a minúsculas.
         1) _**normalize_strings_for_reviewed_cargo_ship_entries**( dataframe )_. Normaliza los datos de las revisiones de la carga de la muestra desambiguadas manualmente, eliminado o convirtiendo caracteres superiores al código 128 en otros compatibles, convierte todas la mayúsculas a minúsculas.
         1) _**read_raw_entities**( entity_type, force_all )_. Lee las entidades conocidas guardadas durante la fase de ingestión (ingest) que sean del tipo pasado por parámetro. Además, _force_all_ permite seleccionar si se requieren todas la entidades o solo la que no se han limpiado todavía.
         1) _**save_entities** (entity_type, dataframe, is_cleaned)_. Guarda las entidades conocidas ya normalizadas en el directorio de la fase de limpieza (bronze) y si is_cleaned es cierto, las marca como entidades limpias.
         1) _**read_entities**( entity_type )_. Lee las entidades conocidas guardadas por el proceso anterior que sean del tipo indicado por el parámetro. 
         1) _**read_raw_reviewed_ship_entries**( force_all )_. Lee las entradas de barcos de la muestra desambiguada manualmente,  guardadas durante la fase de ingestión (ingest). Además, _force_all_ permite seleccionar si se requieren todas la entidades o solo la que no se han limpiado todavía.
         1) _**save_reviewed_ship_entries**( is_cleaned )_.  Guarda entradas de barcos de la muestra desambiguada manualmente en el directorio de la fase de limpieza (bronze) y si is_cleaned es cierto, las marca como registros limpios.
         1) _**read_reviewed_ship_entries**()_. Lee las entradas de barcos de la muestra desambiguada manualmente  guardadas por el proceso anterior.
         1) _**read_raw_reviewed_cargo_ship_entries**( force_all )_. Lee las cargas de barcos entrados de la muestra desambiguada manualmente,  guardadas durante la fase de ingestión (ingest). Además, _force_all_ permite seleccionar si se requieren todas la entidades o solo la que no se han limpiado todavía.
         1) _**save_reviewed_cargo_ship_entries**( is_cleaned )_. Guarda las cargas de barcos entrados de la muestra desambiguada manualmente en el directorio de la fase de limpieza (bronze) y si is_cleaned es cierto, las marca como registros limpios.
         1) _**read_reviewed_cargo_ship_entries**()_.  Lee las cargas de barcos entrados de la muestra desambiguada manualmente guardadas por el proceso anterior.
         
3) Fase de desambiguación:
    1) _BoatFactDisambiguation_ (EN PROCESO)

