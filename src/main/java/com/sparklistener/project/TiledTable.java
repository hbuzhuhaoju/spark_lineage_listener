package com.sparklistener.project;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class TiledTable {

    private String databaseName;
    private String tableName;
    
}
