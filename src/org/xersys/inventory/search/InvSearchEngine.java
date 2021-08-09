package org.xersys.inventory.search;

import org.xersys.commander.iface.XSearch;
import org.json.simple.JSONObject;
import org.xersys.commander.iface.XNautilus;

public class InvSearchEngine implements XSearch{
    private final int DEFAULT_LIMIT = 50;
    
    private XNautilus _nautilus;
    
    private String _key;
    private String _filter;
    private int _max;
    private boolean _exact;
    
    private InvSearchFactory _instance;
    
    public enum Type{
        searchInvItemSimple,
        searchInvItemComplex,
        searchInvBranchSimple,
        searchInvBranchComplex
    }
    
    public InvSearchEngine(XNautilus foValue){
        _nautilus = foValue;
        
        _key = "";
        _filter = "";
        _max = DEFAULT_LIMIT;
        _exact = false;
    }
    
    @Override
    public void setKey(String fsValue) {
        _key = fsValue;
    }

    @Override
    public void setFilter(String fsValue) {
        _filter = fsValue;
    }

    @Override
    public void sethMax(int fnValue) {
        _max = fnValue;
    }

    @Override
    public void setExact(boolean fbValue) {
        _exact = fbValue;
    }

    public JSONObject Search(Enum foFactory, Object foValue) {
        _instance = new InvSearchFactory(_nautilus, _key, _filter, _max, _exact);
        
        JSONObject loJSON = null;
        String lsColName;
        
        if (foFactory == Type.searchInvItemSimple){
            lsColName = "sBarCodex»sDescript»sStockIDx";
            loJSON = _instance.searchItem((String) foValue, lsColName);
            if ("success".equals((String) loJSON.get("result"))) {
                loJSON.put("headers", "Part Number»Description»ID");
                loJSON.put("colname", lsColName);
            }
        } else if (foFactory == Type.searchInvItemComplex){
            lsColName = "sBarCodex»sDescript»nUnitPrce»nSelPrce1»sBrandCde»sModelCde»sColorCde»sCategrCd»sInvTypCd»sStockIDx";
            loJSON = _instance.searchItem((String) foValue, lsColName);
            if ("success".equals((String) loJSON.get("result"))) {
                loJSON.put("headers", "Part Number»Description»Inv. Price»SRP»Brand»Model»Color»Category»Inv. Type»ID");
                loJSON.put("colname", lsColName);
            }
        } else if (foFactory == Type.searchInvBranchSimple){
            lsColName = "sBarCodex»sDescript»sStockIDx";
            loJSON = _instance.searchBranchInventory((String) foValue, lsColName);
            if ("success".equals((String) loJSON.get("result"))) {
                loJSON.put("headers", "Part Number»Description»ID");
                loJSON.put("colname", lsColName);
            }
        } else if (foFactory == Type.searchInvBranchComplex){
            lsColName = "sBarCodex»sDescript»nQtyOnHnd»nSelPrce1»sBrandCde»sModelCde»sColorCde»sStockIDx»nUnitPrce";
            loJSON = _instance.searchBranchInventory((String) foValue, lsColName);
            if ("success".equals((String) loJSON.get("result"))) {
                loJSON.put("headers", "Part Number»Description»On Hand»SRP»Brand»Model»Color»ID»Unit Price");
                loJSON.put("colname", lsColName);
            }
        }
        
        return loJSON;
    }
}
