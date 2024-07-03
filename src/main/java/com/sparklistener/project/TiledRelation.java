package com.sparklistener.project;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@Builder
@ToString
public class TiledRelation {
    
    private String firstColumnName;
    private List<TiledOriginColumnInfo> firstOriginColumnInfos;

    private String secondColumnName;
    private List<TiledOriginColumnInfo> secondOriginColumnInfos;

}
