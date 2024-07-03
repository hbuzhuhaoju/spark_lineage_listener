package com.sparklistener.project;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sparklistener.project.enums.NodeName;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class CompressLayerResult {
        private static final Logger logger = LoggerFactory.getLogger(CompressLayerResult.class);


        private String nodeName;

        private String aliasOrTableName;

        private String databaseName;

        private List<Column> columns;

        private List<CompressLayerResult> layerResults;

        private List<Relationship> relationships;

        @AllArgsConstructor
        @NoArgsConstructor
        @Data
        @Builder
        public static class Relationship {

                // private String firstTable;

                private String firstColumn;

                private Long firstExprId;

                // private String secondTable;

                private String secondColumn;

                private Long secondExprId;

        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        @Builder
        public static class Column {

                private String name;
                private String database;
                // private String tableName;
                private Long exprId;
                // private Long originExprId;
                private List<Long> originExprIds;
        
        }

        public static boolean isViewLayer(CompressLayerResult resultTable) {
                return resultTable.getNodeName().equals(NodeName.CREATE_VIEW);
        }

        public static void printLayerResult(CompressLayerResult resultTable) {
                printLayerResult(resultTable, 0);
        }

        private static void printLayerResult(CompressLayerResult resultTable, int level) {
                if (resultTable == null) {
                        return;
                }

                // 打印当前层级的前缀
                String prefix = repeatString(" ", level * 2);

                // 打印 ResultTable 的基本信息
                logger.info(prefix + "ResultTable:");
                logger.info(prefix + "  Node Name: " + resultTable.getNodeName());
                logger.info(prefix + "  Alias or Table Name: " + resultTable.getAliasOrTableName());

                // 打印 columns 列表
                List<CompressLayerResult.Column> columns = resultTable.getColumns();
                if (columns != null && !columns.isEmpty()) {
                        logger.info(prefix + "  Columns:");
                        for (CompressLayerResult.Column column : columns) {
                                logger.info(prefix + "    Column:");
                                // logger.info(prefix + "      TableName: " + column.getTableName());
                                logger.info(prefix + "      Name: " + column.getName());
                                logger.info(prefix + "      ExprId: " + column.getExprId());
                                logger.info(prefix + "      OriginExprIds: " + column.getOriginExprIds());
                        }
                }

                // 打印 tables 列表
                List<CompressLayerResult> tables = resultTable.getLayerResults();
                if (tables != null && !tables.isEmpty()) {
                        logger.info(prefix + "  Tables:");
                        for (CompressLayerResult table : tables) {
                                printLayerResult(table, level + 1);
                        }
                }

                // 打印 relationships 列表
                List<CompressLayerResult.Relationship> relationships = resultTable.getRelationships();
                if (relationships != null && !relationships.isEmpty()) {
                        logger.info(prefix + "  Relationships:");
                        for (CompressLayerResult.Relationship relationship : relationships) {
                                logger.info(prefix + "    Relationship:");
                                // logger.info(prefix + "      First Table: " + relationship.getFirstTable());
                                logger.info(prefix + "      First Column: " + relationship.getFirstColumn());
                                logger.info(prefix + "      First ExprId: " + relationship.getFirstExprId());
                                // logger.info(prefix + "      Second Table: " + relationship.getSecondTable());
                                logger.info(prefix + "      Second Column: " + relationship.getSecondColumn());
                                logger.info(prefix + "      Second ExprId: " + relationship.getSecondExprId());
                        }
                }
        }


        private static String repeatString(String str, int count) {
                return String.join("", Collections.nCopies(count, str));
        }
}
