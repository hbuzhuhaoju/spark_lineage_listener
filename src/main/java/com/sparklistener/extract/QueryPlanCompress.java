package com.sparklistener.extract;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.BinaryOperator;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.ComplexTypeMergingExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Nvl;
import org.apache.spark.sql.catalyst.expressions.TernaryExpression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.slf4j.Logger;
// 
import org.slf4j.LoggerFactory;

import com.sparklistener.CustomQueryExecutionListener;
import com.sparklistener.project.CompressLayerResult;
import com.sparklistener.project.CompressLayerResult.Column;
import com.sparklistener.project.CompressLayerResult.CompressLayerResultBuilder;
import com.sparklistener.project.CompressLayerResult.Relationship;
import com.sparklistener.project.enums.ColumnUserDefined;
import com.sparklistener.project.enums.NodeName;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class QueryPlanCompress {
    private static final Logger logger = LoggerFactory.getLogger(CustomQueryExecutionListener.class);

    public static CompressLayerResult compressLayer(LogicalPlan plan) {
        System.out.println("Extract Lineage for Plan:" + plan.nodeName());

        switch (plan.nodeName()) {
            case NodeName.UNION:
                return extractUnionPlan(plan);
            case NodeName.JOIN:
                return extractJoinPlan(plan);
            case NodeName.SUB_QUERY:
                return extractViewOrSubQueryPlan(plan);
            case NodeName.CREATE_VIEW:
                return extractViewOrSubQueryPlan(plan);
            case NodeName.PROJECT:
                return extractProjectOrAggregatePlanOrWindow(plan);
            case NodeName.AGGREGATE:
                return extractProjectOrAggregatePlanOrWindow(plan);
            case NodeName.WINDOW:
                return extractProjectOrAggregatePlanOrWindow(plan);
            case NodeName.INSERT_HADOOP_FS_RELATION_COMMAND:
                return extractInsertIntoCommandPlan(plan);
            case NodeName.INSERT_INTO_HIVE_TABLE:
                return extractInsertIntoCommandPlan(plan);

            default: // 降层级
                List<LogicalPlan> children = JavaConverters.seqAsJavaList(plan.children());
                if (children.size() > 1) {
                    throw new RuntimeException("LogicalPlan has more than one child" + plan.nodeName());
                }
                // List<LayerResult> otherTables = JavaConverters.seqAsJavaList(plan.children())
                // .stream().map(child -> extractLineage(child)).collect(Collectors.toList());
                // return
                // LayerResult.builder().nodeName(plan.nodeName()).layerResults(otherTables).build();
                return compressLayer(children.get(0));
        }
    }

    public static CompressLayerResult extractJoinPlan(LogicalPlan plan) {
        Join join = (Join) plan;
        // 获取子查询
        LogicalPlan left = join.left();
        LogicalPlan right = join.right();

        Expression condition = join.condition().get();
        List<Relationship> relationships = new ArrayList<Relationship>();

        extractConditionRelationShip(condition, relationships);

        CompressLayerResult leftTable = compressLayer(left);
        CompressLayerResult rightTable = compressLayer(right);

        List<CompressLayerResult> tables = Arrays.asList(leftTable, rightTable);

        return CompressLayerResult.builder()
                .nodeName(join.nodeName())
                .layerResults(tables)
                .relationships(relationships)
                .build();
    }

    public static void extractConditionRelationShip(Expression condition, List<Relationship> relationships) {

        if (condition instanceof BinaryOperator) {
            BinaryOperator binaryOperator = (BinaryOperator) condition;
            Expression left = binaryOperator.left();
            Expression right = binaryOperator.right();

            if (condition instanceof BinaryComparison) {

                List<Column> leftColumns = extractConditionRelationColumn(left);
                List<Column> rightColumns = extractConditionRelationColumn(right);

                for (Column leftColumn : leftColumns) {
                    for (Column rightColumn : rightColumns) {
                        Relationship relationship = Relationship.builder()
                                // .firstTable(leftColumn.getTableName())
                                .firstColumn(leftColumn.getName())
                                .firstExprId(leftColumn.getExprId())
                                // .secondTable(rightColumn.getTableName())
                                .secondColumn(rightColumn.getName())
                                .secondExprId(rightColumn.getExprId())
                                .build();
                        relationships.add(relationship);
                    }
                }

            } else {
                extractConditionRelationShip(left, relationships);
                extractConditionRelationShip(right, relationships);
            }
        }

    }

    // TODO
    // extractOriginExprIdAndTableName
    // 与extractOriginExprIdAndTableName 重复
    private static List<Column> extractConditionRelationColumn(Expression conditionAttr) {

        List<Column> resultColumns = new ArrayList<>();
        if (conditionAttr instanceof Coalesce) {
            Coalesce coalesce = (Coalesce) conditionAttr;
            JavaConverters.seqAsJavaList(coalesce.children()).forEach(child -> {
                resultColumns.addAll(extractConditionRelationColumn(child));
            });
        } else if (conditionAttr instanceof AttributeReference) {
            AttributeReference attributeReference = (AttributeReference) conditionAttr;
            // String tableName =
            // JavaConverters.seqAsJavaList(attributeReference.qualifier()).get(0);
            String columnName = attributeReference.name();
            Long exprId = attributeReference.exprId().id();

            Column column = Column.builder().name(columnName).exprId(exprId)
                    // .tableName(tableName)
                    .build();
            resultColumns.add(column);
        }
        return resultColumns;

    }
    // Filter filter = (Filter) plan;
    // Expression condition = filter.condition();

    // public static CompressLayerResult extractSubQueryAliasPlan(LogicalPlan plan)
    // {

    // SubqueryAlias subQueryAlias = (SubqueryAlias) plan;

    // CompressLayerResultBuilder layerResultBuilder =
    // CompressLayerResult.builder();
    // layerResultBuilder.nodeName(subQueryAlias.nodeName());

    // String subQueryAliasName = subQueryAlias.identifier().name();
    // layerResultBuilder.aliasOrTableName(subQueryAliasName);

    // return layerResultBuilder.build();
    // }

    public static CompressLayerResult extractViewOrSubQueryPlan(LogicalPlan plan) {

        CompressLayerResultBuilder layerResultBuilder = CompressLayerResult.builder();
        layerResultBuilder.nodeName(plan.nodeName());

        LogicalPlan childLogicalPlan = null;
        if (plan instanceof CreateViewCommand) {
            CreateViewCommand createViewCommand = (CreateViewCommand) plan;
            String viewName = createViewCommand.name().identifier(); // createViewCommand.identifier().name();
            layerResultBuilder.aliasOrTableName(viewName);
            childLogicalPlan = createViewCommand.child();
        } else if (plan instanceof SubqueryAlias) {
            SubqueryAlias subQueryAlias = (SubqueryAlias) plan;
            String subQueryAliasName = subQueryAlias.identifier().name();
            layerResultBuilder.aliasOrTableName(subQueryAliasName);

            childLogicalPlan = subQueryAlias.child();
        }

        if (childLogicalPlan instanceof HiveTableRelation) {

            HiveTableRelation hiveTableRelation = (HiveTableRelation) childLogicalPlan;
            String databaseName = hiveTableRelation.tableMeta().identifier().database().get();

            List<Column> columns = JavaConverters.seqAsJavaList(hiveTableRelation.output())
                    .stream().map(expression -> {
                        if (expression instanceof AttributeReference) {
                            AttributeReference attribute = (AttributeReference) expression;
                            String columnName = attribute.name();
                            Long exprId = attribute.exprId().id();
                            return Column.builder().name(columnName).exprId(exprId).build();
                        }
                        return null;
                    }).collect(Collectors.toList());

            layerResultBuilder.columns(columns);
            layerResultBuilder.databaseName(databaseName);

        } else if (childLogicalPlan instanceof LogicalRelation) {
            LogicalRelation logicalRelation = (LogicalRelation) childLogicalPlan;

            String databaseName = null;
            if (logicalRelation.catalogTable().isDefined()) {
                CatalogTable catalogTable = logicalRelation.catalogTable().get();
                databaseName = catalogTable.database();
            }else if (logicalRelation.relation() != null) {
               if(logicalRelation.relation() instanceof JDBCRelation){
                JDBCRelation jdbcRelation = (JDBCRelation) logicalRelation.relation();
                databaseName = jdbcRelation.jdbcOptions().url().split("/")[3];
                layerResultBuilder.aliasOrTableName(jdbcRelation.jdbcOptions().tableOrQuery());
               }
            }

            List<Column> columns = JavaConverters.seqAsJavaList(logicalRelation.output())
                    .stream().map(expression -> {
                        if (expression instanceof AttributeReference) {
                            AttributeReference attribute = (AttributeReference) expression;
                            String columnName = attribute.name();
                            Long exprId = attribute.exprId().id();
                            return Column.builder().name(columnName).exprId(exprId).build();
                        }
                        return null;
                    }).collect(Collectors.toList());

            layerResultBuilder.columns(columns);
            layerResultBuilder.databaseName(databaseName);

        } else if (childLogicalPlan instanceof LeafNode) {
            LeafNode leafNode = (LeafNode) childLogicalPlan;
            List<Column> columns = JavaConverters.seqAsJavaList(leafNode.output())
                    .stream().map(expression -> {
                        if (expression instanceof AttributeReference) {
                            AttributeReference attribute = (AttributeReference) expression;
                            String columnName = attribute.name();
                            Long exprId = attribute.exprId().id();
                            return Column.builder().name(columnName).exprId(exprId).build();
                        }
                        return null;
                    }).collect(Collectors.toList());
                    

            layerResultBuilder.columns(columns);

        } else {
            CompressLayerResult chiResultTable = compressLayer(childLogicalPlan);
            layerResultBuilder.layerResults(Arrays.asList(chiResultTable));
        }

        return layerResultBuilder.build();

    }

    /*
     * 提取Union计划的血缘信息
     * 同级别的子查询获取
     * 
     * @param union
     * 
     * @param lineage
     * 
     */
    public static CompressLayerResult extractUnionPlan(LogicalPlan plan) {

        // 包含了多个子查询
        Union union = (Union) plan;
        List<LogicalPlan> children = JavaConverters.seqAsJavaList(union.children());
        List<CompressLayerResult> resultTables = children.stream().map(child -> compressLayer(child))
                .collect(Collectors.toList());

        List<Column> columns = new ArrayList<Column>();
        List<Column> firstColumns = resultTables.get(0).getColumns();

        for (int index = 0; index < firstColumns.size(); index++) {
            Column firstColumn = firstColumns.get(index);
            List<Long> exprIds = new ArrayList<Long>();
            for (CompressLayerResult resultTable : resultTables) {
                Column column = resultTable.getColumns().get(index);
                exprIds.add(column.getExprId());
            }
            Column newColumn = Column.builder()
                    .name(firstColumn.getName())
                    .exprId(firstColumn.getExprId())
                    // .originExprId(firstColumn.getOriginExprId())
                    .originExprIds(exprIds)
                    .build();

            columns.add(newColumn);
        }
        return CompressLayerResult.builder()
                .nodeName(plan.nodeName())
                .columns(columns)
                .layerResults(resultTables).build();
    }

    public static CompressLayerResult extractProjectOrAggregatePlanOrWindow(LogicalPlan plan) {

        List<NamedExpression> projectList = new ArrayList<NamedExpression>();
        if (plan instanceof Project) {
            Project project = (Project) plan;
            projectList = JavaConverters.seqAsJavaList(project.projectList());
        } else if (plan instanceof Aggregate) {
            Aggregate aggregate = (Aggregate) plan;
            projectList = JavaConverters.seqAsJavaList(aggregate.aggregateExpressions());
        } else if (plan instanceof Window) {
            Window project = (Window) plan;
            projectList = JavaConverters.seqAsJavaList(project.windowExpressions());
        }

        List<Column> columns = projectList.stream().map(expression -> {
            Long exprId = expression.exprId().id();
            String columnName = expression.name();

            List<Pair<Long, String>> originExprIdAndTableNamePairs = new ArrayList<>();
            if (expression instanceof UnaryExpression) {
                UnaryExpression unaryExpression = (UnaryExpression) expression;
                Expression childExpression = unaryExpression.child();
                originExprIdAndTableNamePairs = extractOriginExprIdAndTableName(childExpression);

            } else if (expression instanceof AttributeReference) {
                AttributeReference attribute = (AttributeReference) expression;
                originExprIdAndTableNamePairs = extractOriginExprIdAndTableName(attribute);
            }

            List<Long> originExprIds = originExprIdAndTableNamePairs.stream().map(Pair::getLeft)
                    .collect(Collectors.toList());

            return Column.builder()
                    // .tableName(tableName)
                    .name(columnName)
                    .exprId(exprId)
                    .originExprIds(originExprIds)
                    .build();

        }).collect(Collectors.toList());

        List<CompressLayerResult> tables = JavaConverters.seqAsJavaList(plan.children())
                .stream().map(child -> compressLayer(child)).collect(Collectors.toList());

        return CompressLayerResult.builder().nodeName(plan.nodeName()).layerResults(tables).columns(columns).build();
    }

    // private static Pair<Long, String>
    // extractOriginExprIdAndTableName(AttributeReference attribute) {

    // Long originExprId = attribute.exprId().id();

    // String tableName = null;
    // Seq<String> qualifier = attribute.qualifier();
    // if (qualifier.length() >= 1) {
    // tableName = qualifier.apply(0);
    // }
    // return Pair.of(originExprId, tableName);
    // }

    private static List<Pair<Long, String>> extractOriginExprIdAndTableName(Expression expression) {

        List<Pair<Long, String>> result = new ArrayList<>();

        if (expression instanceof AttributeReference) {
            AttributeReference attribute = (AttributeReference) expression;
            Long originExprId = attribute.exprId().id();

            String tableName = null;
            Seq<String> qualifier = attribute.qualifier();
            if (qualifier.length() >= 1) {
                tableName = qualifier.apply(0);
            }
            result.add(Pair.of(originExprId, tableName));
            // } else if (expression instanceof CaseWhen) {
            // CaseWhen caseWhen = (CaseWhen) expression;
            // JavaConverters.seqAsJavaList(caseWhen.children()).forEach(child -> {
            // result.addAll(extractOriginExprIdAndTableName(child));
            // });
            // } else if (expression instanceof If) {
            // CaseWhen caseWhen = (CaseWhen) expression;
            // JavaConverters.seqAsJavaList(caseWhen.children()).forEach(child -> {
            // result.addAll(extractOriginExprIdAndTableName(child));
            // });
        } else if (expression instanceof Nvl) {
            Nvl nvl = (Nvl) expression;
            Expression left = nvl.left();
            Expression right = nvl.right();

            result.addAll(extractOriginExprIdAndTableName(left));
            result.addAll(extractOriginExprIdAndTableName(right));
        } else if (expression instanceof ComplexTypeMergingExpression) {
            try {
                Method method = expression.getClass().getMethod("children");
                method.setAccessible(true);
                Seq<Expression> children = (Seq<Expression>) method.invoke(expression);
                for (Expression child : JavaConversions.seqAsJavaList(children)) {
                    result.addAll(extractOriginExprIdAndTableName(child));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (expression instanceof UnaryExpression) {
            UnaryExpression unaryExpression = (UnaryExpression) expression;
            result.addAll(extractOriginExprIdAndTableName(unaryExpression.child()));
        } else if (expression instanceof BinaryExpression) {
            // 忽略两元比较
            if (!(expression instanceof BinaryComparison)) {
                BinaryExpression binaryExpression = (BinaryExpression) expression;
                Expression left = binaryExpression.left();
                Expression right = binaryExpression.right();
                result.addAll(extractOriginExprIdAndTableName(left));
                result.addAll(extractOriginExprIdAndTableName(right));
            }
        } else if (expression instanceof TernaryExpression) {
            TernaryExpression ternaryExpression = (TernaryExpression) expression;
            List<Expression> children = JavaConverters.seqAsJavaList(ternaryExpression.children());
            for (Expression child : children) {
                result.addAll(extractOriginExprIdAndTableName(child));
            }

        } else if (expression instanceof AggregateExpression) {
            AggregateExpression aggregateExpression = (AggregateExpression) expression;
            List<Expression> children = JavaConverters.seqAsJavaList(aggregateExpression.children());
            for (Expression child : children) {
                result.addAll(extractOriginExprIdAndTableName(child));
            }
        } else if (expression instanceof AggregateFunction) {
            AggregateFunction aggregateFunction = (AggregateFunction) expression;
            List<Expression> children = JavaConverters.seqAsJavaList(aggregateFunction.children());
            for (Expression child : children) {
                result.addAll(extractOriginExprIdAndTableName(child));
            }
        } else if (expression instanceof LeafExpression) {
            // LeafExpression leafExpression = (LeafExpression) expression;
            ColumnUserDefined columnUserDefined = ColumnUserDefined.USER_DEFINED_COLUMN;
            result.add(Pair.of(columnUserDefined.getExprId(), columnUserDefined.getTableName()));

            // String constantValue = expression.nodeName();
            // if (expression instanceof Literal) {
            // Literal literal = (Literal) expression;
            // Object value = literal.value();
            // constantValue = value.toString();
            // }
            // ColumnUserDefined columnUserDefined = ColumnUserDefined.USER_DEFINED_COLUMN;
            // result.add(Pair.of(columnUserDefined.getExprId(),constantValue));
        } else {
            // 强转获取
            try {
                Method method = expression.getClass().getMethod("children");
                method.setAccessible(true);
                Seq<Expression> children = (Seq<Expression>) method.invoke(expression);
                for (Expression child : JavaConversions.seqAsJavaList(children)) {
                    result.addAll(extractOriginExprIdAndTableName(child));
                }
            } catch (Exception e) {
                logger.error("暂不支持的表达式类型：{}" + expression.nodeName(), e);
            }
        }

        return result;

    }

    public static CompressLayerResult extractInsertIntoCommandPlan(LogicalPlan plan) {

        Option<CatalogTable> catalogTableOpt = Option.empty();
        if (plan instanceof InsertIntoHadoopFsRelationCommand) {
            InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand = (InsertIntoHadoopFsRelationCommand) plan;
            catalogTableOpt = insertIntoHadoopFsRelationCommand.catalogTable();
        } else if (plan instanceof InsertIntoHiveTable) {
            InsertIntoHiveTable insertIntoHiveTable = (InsertIntoHiveTable) plan;
            CatalogTable catalogTable = insertIntoHiveTable.table();
            catalogTableOpt = Option.apply(catalogTable);
        }

        List<LogicalPlan> children = JavaConverters.seqAsJavaList(plan.children());
        CompressLayerResult childCompressLayerResult = compressLayer(children.get(0));

        if (catalogTableOpt.isDefined()) {
            CatalogTable catalogTable = catalogTableOpt.get();
            String databaseName = catalogTable.database();
            String tableName = catalogTable.identifier().table();
            childCompressLayerResult.setDatabaseName(databaseName);
            childCompressLayerResult.setAliasOrTableName(tableName);

            childCompressLayerResult.setNodeName(NodeName.INSERT_INTO_TABLE);
        }

        return childCompressLayerResult;
    }

}
