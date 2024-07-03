package com.sparklistener.project;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TiledOriginColumnInfo {
    private String columnName;
    private String databaseName;
    private String tableName;
}
   
