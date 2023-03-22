# [English](README.md)

# MessagePack pour Spark

Une implémentation Spark Datasource pour MessagePack.

* [sql-data-sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
* [msgpack](https://msgpack.org/index.html)
* [msgpack-java](https://github.com/msgpack/msgpack-java)

## Coordonnées Maven
```xml
<dependency>
    <groupId>io.github.cybercentrecanada</groupId>
    <artifactId>spark-msgpack-datasource_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```
https://central.sonatype.com/artifact/io.github.cybercentrecanada/spark-msgpack-datasource_2.12/0.0.1

## Lire et écrire les données msgpack:
```scala

// lire
val df =  spark.read.format("messagepack").load("path/to/messagepack")

// écrire
df.write.format("messagepack").save("path/to/output/raw/messagepack")
```

## Options
| Option                  | Type    | Valeur par défaut | Description                                                                                                                                                                              |
|:------------------------|:--------|:------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| deserializer.lenient    | Boolean | false             | Si une exception se produit lors de la désérialisation des valeurs par rapport au schéma spécifié, le message indiquera l'emplacement exact dans les données où l'erreur s'est produite. |
| deserializer.trace_path | Boolean | false             | Si une exception se produit lors de la désérialisation des valeurs par rapport au schéma spécifié, le message indiquera l'emplacement exact dans les données où l'erreur s'est produite. |
| schema.max_sample_files | Number  | 10                | Le nombre maximal de fichiers lus pendant l'inférence de schéma. Mettre à 0 pour aucune limite.                                                                                          |
| schema.max_sample_rows  | Number  | 10000             | Le nombre maximal de lignes à échantillonner dans chaque fichier lors de l'inférence de schéma. Mettre à 0 pour aucune limite.                                                           |

### Example:
```scala
// Si une erreur de désérialisation se produit, renvoyez simplement null au lieu de générer une erreur.
val df =  spark.read.format("messagepack").option("deserializer.lenient", true).load("path/to/messagepack")

// Si une erreur de désérialisation se produit, le message d'erreur inclura le xpath dans les données brutes où le problème s'est produit.
val df =  spark.read.format("messagepack").option("deserializer.trace_path", true).load("path/to/messagepack")

// Traitez tous les fichiers et toutes les lignes pendant l'inférence de schéma.
val df =  spark.read.format("messagepack").option("schema.max_sample_files", 0).option("schema.max_sample_rows", 0).load("path/to/messagepack")
```

## Extensions SQL

Nous exposons nos expressions Spark SQL via l'API d'extensions natives de Spark.

| Propriété d'extension Spark | Implémentation de l'extension MessagePack               |
|:-------------------------|:---------------------------------------------------|
| spark.sql.extensions     | org.apache.spark.sql.msgpack.MessagePackExtensions |

### Expressions
####  from_msgpack
De la même manière que pour créer une expression `from_json` native, vous pouvez convertir les données brutes d'une carte msgpack en une ligne Spark.

Par exemple, supposons que vous ayez une trame de données ou une table nommée 'my_table' avec la structure suivante :

```
+--------------------+
| msgpack_raw        |
+--------------------+
|[87 A2 6B 31 A2 7...|
+--------------------+
```

Et étant donné que le schéma des données brutes est 
```
root
 |-- f1: string (nullable = true)
```

Vous pouvez convertir les données brutes comme ceci:
```scala
val spark = SparkSession.builder.getOrCreate()
val schemaStr = "{"type":"struct","fields":[{"name":"f1","type":"string","nullable":true,"metadata":{}}]}"
val df = spark.sql(s"select from_msgpack(msgpack_raw, '${schemaStr}') as decoded from my_table")
df.select("decoded.*").show()
```

Cela donnera une sortie décodée :
```
+---+
| f1|
+---+
| v1|
+---+
```

## Écrivez vos propres désérialiseurs pour les types d'extension msgpack

Vous pouvez fournir des désérialiseurs personnalisés pour chacun de vos types d'extension msgpack

* Pour chaque type d'extension dont vous avez besoin d'un désérialiseur personnalisé, implémentez simplement le trait ExtensionDeserializer

```scala
trait ExtensionDeserializer extends Serializable {

    def extensionType(): Int

    def sqlType(): DataType

    def deserialize(data: Array[Byte]): Any

}
```

* Implémentez ensuite un fournisseur de désérialisation:

```scala
trait MessagePackExtensionDeserializerProvider {

    def get(): ExtensionDeserializers

}
```

* Enfin, enregistrez le fournisseur de désérialisation de votre application en spécifiant l'implémentation de votre fournisseur dans:

```
src/main/resources/META-INF/services/org.apache.spark.sql.sources.MessagePackExtensionDeserializerProvider
```

Vous trouverez ci-dessous l'implémentation par défaut du désérialiseur:


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
Nous acceptons, encourageons et apprécions les contributions à ce projet. Veuillez nous envoyer une pull-request et nous l'examinerons et vous contacterons.
