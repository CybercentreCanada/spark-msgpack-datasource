# [Français](README.fr.md)

# MessagePack for Spark

A Spark Datasource implementation for MessagePack.

* [sql-data-sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
* [msgpack](https://msgpack.org/index.html)
* [msgpack-java](https://github.com/msgpack/msgpack-java)

## Read and Write MessagePack data:

```scala
// read
val df =  spark.read.format("messagepack").load("path/to/messagepack")

// write
df.write.format("messagepack").save("path/to/output/raw/messagepack")
```


## Options
| Options                 | Type    | Default | Description                                                                                                                                                           |
|:------------------------|:--------|:--------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| deserializer.lenient    | Boolean | false   | When enabled, deserialization errors resulting from incompatibilities between the raw data and the schame will return null.                                           |
| deserializer.trace_path | Boolean | false   | If an exception occurs while deserializing the values against the specified schema, the message will indicate the exact location in the data where the error occured. |
| schema.max_sample_files | Number  | 10      | The maximum number of files read during schema inference.  Set to 0 for no limit.                                                                                     |
| schema.max_sample_rows  | Number  | 10000   | The maximum number of row to sample in each file during schema inference.  Set to 0 for no limit.                                                                     |


### Example:
```scala
// If a deserialization error occurs, simply return null instead of throwing an error.
val df =  spark.read.format("messagepack").option("deserializer.lenient", true).load("path/to/messagepack")

// If a deserialization error occurs, the error message will include the xpath within the raw data where the problem occured.
val df =  spark.read.format("messagepack").option("deserializer.trace_path", true).load("path/to/messagepack")

// Process all files and all rows during schema inference.
val df =  spark.read.format("messagepack").option("schema.max_sample_files", 0).option("schema.max_sample_rows", 0).load("path/to/messagepack")
```

## SQL Extension

We expose our spark sql expressions through spark's native extensions API.

| Spark extension property | MessagePack extension implementation               |
|:-------------------------|:---------------------------------------------------|
| spark.sql.extensions     | org.apache.spark.sql.msgpack.MessagePackExtensions |

### Expressions
####  from_msgpack
Simillarly to spark's native `from_json` expression, you can convert one msgpack map raw data into a spark row.

As an example, assuming that you have a dataframe or a table named 'my_table' with the following structure:
```
+--------------------+
| msgpack_raw        |
+--------------------+
|[87 A2 6B 31 A2 7...|
+--------------------+
```

And given that the schema of the raw data is:
```
root
 |-- f1: string (nullable = true)
```

You can convert the raw data like this
```scala
val spark = SparkSession.builder.getOrCreate()
val schemaStr = "{"type":"struct","fields":[{"name":"f1","type":"string","nullable":true,"metadata":{}}]}"
val df = spark.sql(s"select from_msgpack(msgpack_raw, '${schemaStr}') as decoded from my_table")
df.select("decoded.*").show()
```

This will yield a decoded output:
```
+---+
| f1|
+---+
| v1|
+---+
```

## Write your own deserializers for msgpack extension types

You can provide custom deserializers for each of your msgpack extension type

* For each extension type you require a custom deserializer, simply implement the ExtensionDeserializer trait:

```scala
trait ExtensionDeserializer extends Serializable {

    def extensionType(): Int

    def sqlType(): DataType

    def deserialize(data: Array[Byte]): Any

}
```

* Then implement a deserializer provider:

```scala
trait MessagePackExtensionDeserializerProvider {

    def get(): ExtensionDeserializers

}
```

*  Finally, register your application's deserializer provider by specifying your provider implementation into:

```
src/main/resources/META-INF/services/org.apache.spark.sql.sources.MessagePackExtensionDeserializerProvider
```

Below is shown the default deserializer implementation:
 ```scala
class ExtensionDeserializerProvider extends MessagePackExtensionDeserializerProvider {
    override def get(): ExtensionDeserializers = {
        val deserializers = new ExtensionDeserializers();
        for(i <- 0 to 127) {
            deserializers.set(new DefaultDeserializer(i))
        }
        deserializers
    }
}
```
[ExtensionDeserializerProvider.scala](src/main/scala/org/apache/spark/sql/msgpack/extensions/ExtensionDeserializerProvider.scala)

## Contributions

We accept, encourage, and appreciate contributions to this project.  Please send us a pull-request and we will review and get in touch with you.
