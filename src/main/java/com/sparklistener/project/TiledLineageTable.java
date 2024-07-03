package com.sparklistener.project;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TiledLineageTable {

    private String tableName;

    private String databaseName;

    private List<Column> columns;

    @Data
    @AllArgsConstructor
    public static class Column {
        
        private String columnName;
        List<TiledOriginColumnInfo> originColumnInfo;
    }
    
}
