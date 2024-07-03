###
**项目名称**：SparkLineageListener
SparkLineageListener是一个基于Spark SQL的血缘关系监听器，通过监听Spark SQL的执行过程，解析出血缘关系，可准确到字段级别，同时也可分析出同层级关联表的关系，数据结果异步输出到kafka

**项目背景**：
在大数据分析场景中，血缘关系是数据分析的重要组成部分，通过血缘关系，可以更好地理解数据之间的依赖关系，从而更好地进行数据分析。
另外，在建模方面，血缘关系也有着重要的作用，通过分析血缘关系，可以更好地理解数据之间的关联关系，从而更好地进行建模。现有的开源工具仅支持上下游的血缘关系的解析，不具备同层级关联表的解析能力，因此需要开发新的工具来支持同层级关联表的解析。


**项目特征**：
- 基于实现QueryExecutionListener接口监听Spark sql，对spark logical plan进行解析，提取出血缘关系；
- 支持查询字段类型，Project、Aggregate、Window等，函数见下表；
- 支持多种查询，包括子查询，join，union，with；
- 支持多种关联关系，包括同层级关联表的关系，不同层级关联表的关系；


#### 字段支持函数：
| Expression Type                | 作用                                               | 函数示例                |
|--------------------------------|----------------------------------------------------|------------------------|
| `Nvl`                          | 处理空值（null）                                   | `coalesce`             | 
| `ComplexTypeMergingExpression` | 处理复杂类型（如数组、结构体）的合并操作           | `array_union`, `struct`| 
| `UnaryExpression`              | 一元表达式，对一个操作数进行计算                   | `abs`, `sqrt`, `upper` | 
| `BinaryExpression`             | 二元表达式，对两个操作数进行计算                   | `+`, `-`, `*`, `/`     | 
| `TernaryExpression`            | 三元表达式，对三个操作数进行计算                   | `CASE WHEN`            | 
| `AggregateExpression`          | 聚合表达式，用于执行聚合操作                       | `SUM`, `AVG`, `MAX`, `MIN` |
| `AggregateFunction`            | 实现聚合逻辑的具体函数，通常与`AggregateExpression`一起使用 | `count`, `sum`, `avg`, `max`, `min`    |
| `LeafExpression`               | 叶子表达式，没有子表达式，通常是列引用或常量值     | 列名、常量值          |

PS:非上述子类函数已做反射处理，已try-catch处理，但不保证完全正确；若有其他函数，可以自行添加到函数列表中；暂不支持UDF函数。

**项目示例**：
- sql
```
WITH
  combined AS (
    SELECT id,value,'sourceA' AS source FROM tableA
    UNION ALL
    SELECT id, description AS value,'sourceB' AS source FROM tableB
  )
SELECT
  combined.id,
  combined.value,
  combined.source,
  c.detail,
  cast(d.detail as string) as d_detail,
  rank() OVER (PARTITION BY combined.id ORDER BY combined.value) as rank
FROM
  combined
  LEFT JOIN tableC c ON combined.source = c.detail
  Left JOIN (
    select tableA.id as id,'detail' as detail,substr(tableA.value, 1, 2) as newValue from tableA
      left join tableB on tableA.id = tableB.id
  ) d ON combined.id = d.id
  LEFT JOIN (
    select sum(tableA.value) as totalValue,tableA.id as id from tableA group by tableA.id
  ) f ON f.id = c.id
WHERE
  combined.id IN (
    SELECT id FROM tableA WHERE value = 'A'
    UNION
    SELECT id FROM tableB WHERE description = 'Desc2'
  )
```
- 解析结果：
```
    {
  "dataSource": "Spark",
  "relationShips": [
    {
      "firstColumnName": "id",
      "firstOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tablea"
        }
      ],
      "secondColumnName": "id",
      "secondOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tablec"
        }
      ]
    },
    {
      "firstColumnName": "id",
      "firstOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tablea"
        },
        {
          "columnName": "id",
          "tableName": "tableb"
        },
        {
          "columnName": "id",
          "tableName": "tablea"
        }
      ],
      "secondColumnName": "id",
      "secondOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tablea"
        }
      ]
    },
    {
      "firstColumnName": "source",
      "firstOriginColumnInfos": [
        {
          "databaseName": "user_defined_database", # 该关联字段为自定义 自定义数据库
          "tableName": "user_defined_table" #该关联字段为自定义 自定义表
        },
        {
          "databaseName": "user_defined_database",
          "tableName": "user_defined_table"
        }
      ],
      "secondColumnName": "detail",
      "secondOriginColumnInfos": [
        {
          "columnName": "detail",
          "tableName": "tablec"
        }
      ]
    },
    {
      "firstColumnName": "id",
      "firstOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tablea"
        }
      ],
      "secondColumnName": "id",
      "secondOriginColumnInfos": [
        {
          "columnName": "id",
          "tableName": "tableb"
        }
      ]
    }
  ],
  "dependentTables": [
    
  ]
}
```
**项目使用说明**：
- 0、环境配置：
  -  kafka.properties配置：
    ```
        bootstrap.servers=
        topic=
    ```

  -  log4j.properties配置：
    ```
        log4j.logger.com.sparklistener=INFO,console,file
        log4j.appender.file=org.apache.log4j.FileAppender
        log4j.appender.file.File=./file.log
        log4j.appender.file.layout=org.apache.log4j.PatternLayout
        log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
    ```

- 1、打包、上传到hdfs、或者本地：
```
    mvn clean package
```
- 2、spark-defaults.conf配置：
```
    spark.sql.queryExecutionListeners=com.sparklistener.CustomQueryExecutionListener
    hdfs://emr-cluster/extra_jars/spark-lineage-listener-1.0.3-RELEASE.jar
```
- 3、提交spark任务，查看日志，消费kafka数据。
### jupyter使用 kernel:
    可以查看jupyter的kernel配置

**待完成项**：
- 1、由于解析的是逻辑执行计划，在同一任务中如createOrReplaceTempViews时，会生成LogicalRDD，不能解析到数据源，正在开发
- 2、处理 where 条件中的子查询

