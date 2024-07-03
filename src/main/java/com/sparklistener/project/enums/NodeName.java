package com.sparklistener.project.enums;

// TODO 处理成枚举，将 PlanLayerTile 中的方法改为内置
public class NodeName {
    
    public static final String AGGREGATE = "Aggregate";
    public static final String PROJECT = "Project";
    public static final String JOIN = "Join";
    public static final String UNION = "Union";
    public static final String SUB_QUERY = "SubqueryAlias";
    public static final String WINDOW = "Window";
    public static final String INSERT_INTO_TABLE = "InsertIntoTable";
    public static final String INSERT_HADOOP_FS_RELATION_COMMAND = "InsertIntoHadoopFsRelationCommand";
    public static final String INSERT_INTO_HIVE_TABLE = "InsertIntoHiveTable";
    public static final String CREATE_VIEW = "CreateViewCommand";

}
