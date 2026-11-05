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
