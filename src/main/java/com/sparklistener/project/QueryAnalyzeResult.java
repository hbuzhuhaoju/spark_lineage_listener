package com.sparklistener.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class QueryAnalyzeResult {

    @Builder.Default
    private String  dataSource = "Spark";
    
    private TiledLineageTable lineageTable;
    private List<TiledRelation>   relationShips;
    private List<TiledTable> dependentTables;   

    private Map<String,String> auxiliaryInfo;


    public static QueryAnalyzeResult addAuxiliaryInfo(QueryAnalyzeResult queryAnalyzeResult,String ...args){
        Map<String,String> map = new HashMap<>();
        for(int i=0;i<args.length;i+=2){
            map.put(args[i],args[i+1]);
        }
        queryAnalyzeResult.setAuxiliaryInfo(map);
        return queryAnalyzeResult;
    }
}
