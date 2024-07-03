package com.sparklistener.project.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ColumnUserDefined {
    
    USER_DEFINED_COLUMN(-999L,"user_defined_database","user_defined_table");

    private final Long exprId;
    private final String databaseName;
    private final String tableName;
    
}