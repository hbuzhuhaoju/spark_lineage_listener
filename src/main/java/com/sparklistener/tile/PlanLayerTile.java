package com.sparklistener.tile;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.sparklistener.project.CompressLayerResult;
import com.sparklistener.project.CompressLayerResult.Column;
import com.sparklistener.project.CompressLayerResult.Relationship;
import com.sparklistener.project.TiledLineageTable;
import com.sparklistener.project.TiledOriginColumnInfo;
import com.sparklistener.project.TiledRelation;
import com.sparklistener.project.TiledTable;
import com.sparklistener.project.enums.ColumnUserDefined;
import com.sparklistener.project.enums.NodeName;

public class PlanLayerTile {

    public static TiledLineageTable tileQueryLineageTables(CompressLayerResult resultTable) {

        if (!NodeName.INSERT_INTO_TABLE.equals(resultTable.getNodeName())) {
            return null;
        } else {
            List<TiledLineageTable.Column> tiledColumns = resultTable.getColumns().stream()
                    .map(column -> {
                        if (column.getOriginExprIds().isEmpty()) {
                            return new TiledLineageTable.Column(column.getName(), Collections.emptyList());
                        } else {
                            List<TiledOriginColumnInfo> columnInfos = findSourceColumnByExprIds(resultTable,
                                    column.getOriginExprIds());
                            return new TiledLineageTable.Column(column.getName(), columnInfos);
                        }

                    }).collect(Collectors.toList());

            TiledLineageTable tiledLineageTable = TiledLineageTable.builder()
                    .databaseName(resultTable.getDatabaseName())
                    .tableName(resultTable.getAliasOrTableName())
                    .columns(tiledColumns)
                    .build();
            return tiledLineageTable;

        }
    }

    public static List<TiledTable> tileQueryTables(CompressLayerResult resultTable) {

        List<TiledTable> tiledTables = new ArrayList<>();

        if (StringUtils.isNotEmpty(resultTable.getDatabaseName())) {
            TiledTable tiledTable = TiledTable.builder()
                    .databaseName(resultTable.getDatabaseName())
                    .tableName(resultTable.getAliasOrTableName())
                    .build();

            tiledTables.add(tiledTable);
        }
        
        // else if(viewOrTableLayerMap.containsKey(resultTable.getAliasOrTableName())){
        //     CompressLayerResult viewOrTableLayer = viewOrTableLayerMap.get(resultTable.getAliasOrTableName());
        //     tiledTables.addAll(tileQueryTables(viewOrTableLayer, viewOrTableLayerMap));
        // }

        if (resultTable.getLayerResults() != null && resultTable.getLayerResults().size() > 0) {
            resultTable.getLayerResults().stream().forEach(layer -> {
                tiledTables.addAll(tileQueryTables(layer));
            });
        }

        return tiledTables;
    }

    public static List<TiledRelation> tileQueryRelations(CompressLayerResult resultTable) {

        List<TiledRelation> tiledRelations = new ArrayList<>();

        List<Relationship> relationships = resultTable.getRelationships();

        if (relationships != null && relationships.size() > 0) {
            relationships.forEach(relationship -> {
                // String firstTable = relationship.getFirstTable();
                String firstColumn = relationship.getFirstColumn();
                Long firstExprId = relationship.getFirstExprId();

                List<TiledOriginColumnInfo> firstColumnInfos = findSourceColumnByExprIds(resultTable,
                        Arrays.asList(firstExprId));

                // String secondTable = relationship.getSecondTable();
                String secondColumn = relationship.getSecondColumn();
                Long secondExprId = relationship.getSecondExprId();

                List<TiledOriginColumnInfo> secondColumnInfos = findSourceColumnByExprIds(resultTable,
                        Arrays.asList(secondExprId));

                if (firstColumnInfos.size() == 0 || secondColumnInfos.size() == 0) {
                    throw new IllegalArgumentException("Cannot find source table for " + firstColumn
                            + "#" + firstExprId + " or " + secondColumn + "#" + secondExprId);
                }

                TiledRelation tiledRelation = TiledRelation.builder()
                        .firstColumnName(firstColumn)
                        .firstOriginColumnInfos(firstColumnInfos)
                        .secondColumnName(secondColumn)
                        .secondOriginColumnInfos(secondColumnInfos)
                        .build();

                tiledRelations.add(tiledRelation);
            });
        }
        if (resultTable.getLayerResults() != null && resultTable.getLayerResults().size() > 0) {
            resultTable.getLayerResults().stream().forEach(layer -> {
                tiledRelations.addAll(tileQueryRelations(layer));
            });
        }

        return tiledRelations;
    }

    // TODO 返回的是表名.字段名
    private static List<TiledOriginColumnInfo> findSourceColumnByExprIds(CompressLayerResult resultTable,
            List<Long> exprIds) {

        List<TiledOriginColumnInfo> result = new ArrayList<>();

        List<Column> columns = resultTable.getColumns();
        List<Long> unionExprIds = new ArrayList<>();

        if (columns != null && columns.size() > 0) {
            Map<Long, Column> columnMap = columns.stream()
                    .collect(Collectors.toMap(Column::getExprId, Function.identity(),
                            (existing, replacement) -> existing));

            Map<Long, Column> originColumnMap = columns.stream()
                    .filter(column -> CollectionUtils.isNotEmpty(column.getOriginExprIds()))
                    .flatMap(column -> column.getOriginExprIds().stream()
                            .map(id -> new AbstractMap.SimpleEntry<>(id, column)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (existing, replacement) -> existing));

            for (Long exprId : exprIds) {

                Column column = columnMap.get(exprId) == null ? originColumnMap.get(exprId) : columnMap.get(exprId);
                if (column != null) {
                    if (resultTable.getNodeName().equals(NodeName.SUB_QUERY)) {
                        String tableName = resultTable.getAliasOrTableName();
                        String databaseName = resultTable.getDatabaseName();
                        // // 处理查询中的是从临时视图中获取的字段
                        // if (StringUtils.isEmpty(databaseName) && viewOrTableLayerMap.containsKey(tableName)) {
                        //     CompressLayerResult viewOrTableLayer = viewOrTableLayerMap.get(tableName);
                        //     result.addAll(findSourceColumnFromViewOrTable(viewOrTableLayer, column.getName(),
                        //             viewOrTableLayerMap));

                        // } else {
                            TiledOriginColumnInfo relationShip = TiledOriginColumnInfo.builder()
                                    .columnName(column.getName())
                                    .tableName(tableName)
                                    .databaseName(databaseName)
                                    .build();
                            result.add(relationShip);
                        // }

                    } else if (resultTable.getNodeName().equals(NodeName.UNION)
                            || resultTable.getNodeName().equals(NodeName.PROJECT)
                            || resultTable.getNodeName().equals(NodeName.AGGREGATE)
                            || resultTable.getNodeName().equals(NodeName.WINDOW)) {

                        if (CollectionUtils.isNotEmpty(column.getOriginExprIds())) {
                            unionExprIds.addAll(column.getOriginExprIds());
                        }
                    }
                }
            }
        }

        exprIds = CollectionUtils.isNotEmpty(unionExprIds) ? unionExprIds : exprIds;

        // //下一轮剔除用户自定义字段
        exprIds.stream().filter(exprId -> exprId.equals(ColumnUserDefined.USER_DEFINED_COLUMN.getExprId()))
                .forEach(exprId -> {
                    if (exprId.equals(ColumnUserDefined.USER_DEFINED_COLUMN.getExprId())) {
                        TiledOriginColumnInfo relationShip = TiledOriginColumnInfo.builder()
                                .tableName(ColumnUserDefined.USER_DEFINED_COLUMN.getTableName())
                                .databaseName(ColumnUserDefined.USER_DEFINED_COLUMN.getDatabaseName())
                                .build();
                        result.add(relationShip);
                    }
                });

        exprIds = exprIds.stream().filter(exprId -> !exprId.equals(ColumnUserDefined.USER_DEFINED_COLUMN.getExprId()))
                .collect(Collectors.toList());

        List<CompressLayerResult> layers = resultTable.getLayerResults();
        if (layers != null && layers.size() != 0) {

            for (CompressLayerResult layer : layers) {
                result.addAll(findSourceColumnByExprIds(layer, exprIds));
            }
        }
        return result;
    }

    // private static List<TiledOriginColumnInfo> findSourceColumnFromViewOrTable(CompressLayerResult compressLayerResult,
    //         String columnName,
    //         Map<String, CompressLayerResult> viewOrTableLayerMap) {

    //     List<Column> columns = compressLayerResult.getColumns();

    //     if (columns != null && columns.size() > 0) {
    //         Map<String, Column> columnMap = columns.stream()
    //                 .collect(Collectors.toMap(Column::getName, Function.identity(),
    //                         (existing, replacement) -> existing));

    //         if (columnMap.containsKey(columnName)) {
    //             Column column = columnMap.get(columnName);
    //             return findSourceColumnByExprIds(compressLayerResult, Arrays.asList(column.getExprId()),
    //                     viewOrTableLayerMap);
    //         }
    //     } else {
    //         for (CompressLayerResult childLayer : compressLayerResult.getLayerResults()) {
    //             List<TiledOriginColumnInfo> result = findSourceColumnFromViewOrTable(childLayer, columnName,
    //                     viewOrTableLayerMap);
    //             if (result.size() > 0) {
    //                 return result;
    //             }
    //         }
    //     }
    //     return Collections.emptyList();
    // }

}
