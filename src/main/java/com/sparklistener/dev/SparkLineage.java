package com.sparklistener.dev;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;

import com.google.gson.Gson;
import com.sparklistener.extract.QueryPlanCompress;
import com.sparklistener.message.MessageProducer;
import com.sparklistener.project.CompressLayerResult;
import com.sparklistener.project.QueryAnalyzeResult;
import com.sparklistener.project.TiledLineageTable;
import com.sparklistener.project.TiledRelation;
import com.sparklistener.project.TiledTable;
import com.sparklistener.tile.PlanLayerTile;

public class SparkLineage {


    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Get Lineage Information")
                .master("local")
                .getOrCreate();


        SqlUtils.createOrReplaceTempViews(spark);
        String sql = "WITH combined AS (" +
                " SELECT id, value, 'sourceA' AS source FROM tableA" +
                " UNION ALL" +
                " SELECT id, description AS value, 'sourceB' AS source FROM tableB" +
                ")" +
                "SELECT combined.id, combined.value, combined.source, c.detail,cast(d.detail as string) as d_detail,rank() OVER (PARTITION BY combined.id ORDER BY combined.value) as rank"
                +
                " FROM combined" +
                " LEFT JOIN tableC c ON combined.source = c.detail" +
                " Left JOIN (select tableA.id as id, 'detail' as detail,substr(tableA.value,1,2) as newValue from tableA left join tableB on tableA.id = tableB.id) d ON combined.id = d.id "
                +
                " LEFT JOIN (select sum(tableA.value) as totalValue,tableA.id as id from tableA group by tableA.id) f  ON f.id = c.id "
                +
                " WHERE combined.id IN (" +
                " SELECT id FROM tableA WHERE value = 'A'" +
                " UNION" +
                " SELECT id FROM tableB WHERE description = 'Desc2'" +
                ")";
        // nvl,常量，时间函数
        // String sql = " SELECT cast(nvl(a.id,b.id) as string),123 as value,current_date() as date FROM tableA a " +  
        // " left join tableB  b on a.id = b.id";
        
        // case when, if 
        // String sql = "SELECT sum(if(a.id = 1,1,2)) as num,case when a.id = 1 then a.id  when a.id = 2 then a.id +1 else  a.id + 3 end as value FROM tableA a group by a.id";

        // String sql = "SELECT    count(distinct case when a.id = 1 then a.id  when a.id = 2 then a.id +1 else  a.id + 3 end) from tableA a" ; // value FROM tableA a ";
        // String sql = "SELECT   * from  (select current_date as date from tableB) b left join (select current_date as date from tableB) c on b.date = c.date" ; // value FROM tableA a ";



        // round
        // String sql = "SELECT current_date as date,round(sum(a.value),2) as value FROM tableA a group by a.id";
        // window function
        // String sql = "SELECT sum(a.value) over (partition by a.id order by a.value) as sum FROM tableA a";

        //concat_ws 这种没有父类的，直接强转处理，失败后再分析
        // String sql = "SELECT concat_ws(',',a.id,a.value) as value FROM tableA a";

        Dataset<Row> result = spark.sql(sql);

        // 获取查询执行对象
        QueryExecution qe = result.queryExecution();

        // 获取解析后的逻辑计划
        LogicalPlan analyzedPlan = qe.analyzed();


        System.out.println("Logical Plan:" + analyzedPlan.toString());
        // 提取血缘信息
        CompressLayerResult lineage = QueryPlanCompress.compressLayer(analyzedPlan);

        System.out.println("Lineage:" + lineage.toString());

        List<TiledRelation> tiledRelations = PlanLayerTile.tileQueryRelations(lineage);
        List<TiledTable> tiledTables = PlanLayerTile.tileQueryTables(lineage);
        TiledLineageTable tiledLineageTable = PlanLayerTile.tileQueryLineageTables(lineage);

        // 停止 SparkSessio ResultTable.printResultTable(lineage);
        
                QueryAnalyzeResult queryAnalyzeResult = QueryAnalyzeResult.builder()
                .dependentTables(tiledTables)
                .lineageTable(tiledLineageTable)
                .relationShips(tiledRelations)
                .build();

        Gson gson = new Gson();
        MessageProducer.send(gson.toJson(queryAnalyzeResult));

        spark.stop();
    }

}