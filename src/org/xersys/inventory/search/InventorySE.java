package org.xersys.inventory.search;

import org.json.simple.JSONObject;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XNeoSearch;

public class InventorySE implements XNeoSearch{
    private final int DEFAULT_LIMIT = 50;
    
    private XNautilus _nautilus;
    
    private Object _type;
    
    private String _value;
    private String _key;
    private String _filter;
    private int _max;
    private boolean _exact;
    
    public InventorySE(XNautilus foValue){
        _nautilus = foValue;
        
        _type = null;
        _value = "";
        _key = "";
        _filter = "";
        _max = DEFAULT_LIMIT;
        _exact = false;
    }
    
    @Override
    public void setSearchType(Object foValue){
        _type = foValue;
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
    public void setMax(int fnValue) {
        _max = fnValue;
    }

    @Override
    public void setExact(boolean fbValue) {
        _exact = fbValue;
    }

    @Override
    public JSONObject Search(Object foValue) {
        InventorySF _instance = new InventorySF(_nautilus, _key, _filter, _max, _exact);
        
        JSONObject loJSON = null;
        String lsColName;
        
        InventorySF.Type loType = (InventorySF.Type) _type;
        
        if (null != loType)switch (loType) {
            case searchInvItemSimple:
                lsColName = "sBarCodex»sDescript»sStockIDx";
                loJSON = _instance.searchItem(loType, (String) foValue, lsColName);
                if ("success".equals((String) loJSON.get("result"))) {
                    loJSON.put("headers", "Part Number»Description»ID");
                    loJSON.put("colname", lsColName);
                }
                break;
            case searchInvItemComplex:
                lsColName = "sBarCodex»sDescript»nUnitPrce»nSelPrce1»sBrandCde»sModelCde»sColorCde»sCategrCd»sInvTypCd»sStockIDx";
                loJSON = _instance.searchItem(loType, (String) foValue, lsColName);
                if ("success".equals((String) loJSON.get("result"))) {
                    loJSON.put("headers", "Part Number»Description»Inv. Price»SRP»Brand»Model»Color»Category»Inv. Type»ID");
                    loJSON.put("colname", lsColName);
                }
                break;
            case searchInvBranchSimple:
                lsColName = "sBarCodex»sDescript»sStockIDx";
                loJSON = _instance.searchItem(loType, (String) foValue, lsColName);
                if ("success".equals((String) loJSON.get("result"))) {
                    loJSON.put("headers", "Part Number»Description»ID");
                    loJSON.put("colname", lsColName);
                }
                break;
            case searchInvBranchComplex:
                lsColName = "sBarCodex»sDescript»nQtyOnHnd»nSelPrce1»sBrandCde»sModelCde»sColorCde»sStockIDx»nUnitPrce";
                loJSON = _instance.searchItem(loType, (String) foValue, lsColName);
                if ("success".equals((String) loJSON.get("result"))) {
                    loJSON.put("headers", "Part Number»Description»On Hand»SRP»Brand»Model»Color»ID»Unit Price");
                    loJSON.put("colname", lsColName);
                }
                break;
            default:
                break;
        }
        
        return loJSON;
    }
}
