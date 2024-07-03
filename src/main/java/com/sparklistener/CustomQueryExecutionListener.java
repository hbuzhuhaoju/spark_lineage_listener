package com.sparklistener;

import java.util.List;

import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
// 
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.sparklistener.extract.QueryPlanCompress;
import com.sparklistener.message.MessageProducer;
import com.sparklistener.project.CompressLayerResult;
import com.sparklistener.project.QueryAnalyzeResult;
import com.sparklistener.project.TiledLineageTable;
import com.sparklistener.project.TiledRelation;
import com.sparklistener.project.TiledTable;
import com.sparklistener.tile.PlanLayerTile;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomQueryExecutionListener implements QueryExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(CustomQueryExecutionListener.class);

    // @Setter
    // @Getter
    // private Map<String,CompressLayerResult> viewOrTableLayerMap = new HashMap<>();
    
    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {

        RuntimeConfig runtimeConfig = qe.sparkSession().conf();
        
        LogicalPlan executedPlan = qe.analyzed();

        String appId = runtimeConfig.get("spark.app.id");
        String appName = runtimeConfig.get("spark.app.name");
        try {
            logger.info("appId:" + appId + ", appName:" + appName + ", executedPlan:\n" + executedPlan.toString());

            CompressLayerResult lineage = QueryPlanCompress.compressLayer(executedPlan);

            // CompressLayerResult.printLayerResult(lineage);

            List<TiledRelation> tiledRelations = PlanLayerTile.tileQueryRelations(lineage);
            List<TiledTable> tiledTables = PlanLayerTile.tileQueryTables(lineage);
            TiledLineageTable tiledLineageTable = PlanLayerTile.tileQueryLineageTables(lineage);

            QueryAnalyzeResult queryAnalyzeResult = QueryAnalyzeResult.builder()
                    .dependentTables(tiledTables)
                    .lineageTable(tiledLineageTable)
                    .relationShips(tiledRelations)
                    .build();

            QueryAnalyzeResult.addAuxiliaryInfo(queryAnalyzeResult, "appId", appId, "appName", appName);

            Gson gson = new Gson();
            MessageProducer.send(gson.toJson(queryAnalyzeResult));

            // if(CompressLayerResult.isViewLayer(lineage)){
            //     viewOrTableLayerMap.put(lineage.getAliasOrTableName(), lineage);
            // }

        } catch (Exception e) {
            logger.error("appId:" + appId + ", appName:" + appName +"，log Query failed: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
        String originalSql = qe.logical().toString();
        System.out.println("Query failed: " + originalSql);
        logger.error("log Query failed: {}", originalSql, exception);

        LogicalPlan executedPlan = qe.analyzed();

        System.out.println("executedPlan: " + executedPlan.toString());

    }

    
}
