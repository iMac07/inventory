package org.xersys.imbentaryo.search;

import java.sql.ResultSet;
import org.json.simple.JSONObject;
import org.xersys.kumander.iface.XNautilus;
import org.xersys.kumander.util.MiscUtil;
import org.xersys.kumander.util.SQLUtil;


public class InvSearchFactory{
    private XNautilus _nautilus;
    
    private String _key;
    private String _filter;
    private int _max;
    private boolean _exact;
    
    public InvSearchFactory(XNautilus foNautilus, String fsKey, String fsFilter, int fnMax, boolean fbExact){
        _nautilus = foNautilus;
        _key = fsKey;
        _filter = fsFilter;
        _max = fnMax;
        _exact = fbExact;
    }
    
    public JSONObject searchItem(String fsValue, String fsFields){
        JSONObject loJSON = new JSONObject();
        
        if (_nautilus == null){
            loJSON.put("result", "error");
            loJSON.put("message", "Application driver is not set.");
            return loJSON;
        }
        
        String lsSQL = getSQ_Inventory();
        
        //are we searching with an exact value
        if (_exact)
            lsSQL = MiscUtil.addCondition(lsSQL, _key + " = " + SQLUtil.toSQL(fsValue));
        else
            lsSQL = MiscUtil.addCondition(lsSQL, _key + " LIKE " + SQLUtil.toSQL(fsValue + "%"));
        
        //add filter on query
        if (!_filter.isEmpty()) lsSQL = MiscUtil.addCondition(lsSQL, _filter);
        
        //add order by and limit on query
        lsSQL = lsSQL + " ORDER BY " + _key + " LIMIT " + _max;
        
        ResultSet loRS = _nautilus.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            loJSON.put("result", "error");
            loJSON.put("message", "No record found.");
            return loJSON;
        }
        
        loJSON.put("result", "success");
        loJSON.put("payload", MiscUtil.RS2JSON(loRS, fsFields));
        
        //close resultset
        MiscUtil.close(loRS);
        return loJSON;
    }
    
    public JSONObject searchBranchInventory(String fsValue, String fsFields){
        JSONObject loJSON = new JSONObject();
        
        if (_nautilus == null){
            loJSON.put("result", "error");
            loJSON.put("message", "Application driver is not set.");
            return loJSON;
        }
        
        String lsSQL = getSQ_Inv_Master();
        
        //are we searching with an exact value
        if (_exact)
            lsSQL = MiscUtil.addCondition(lsSQL, _key + " = " + SQLUtil.toSQL(fsValue));
        else
            lsSQL = MiscUtil.addCondition(lsSQL, _key + " LIKE " + SQLUtil.toSQL(fsValue + "%"));
        
        //add filter on query
        if (!_filter.isEmpty()) lsSQL = MiscUtil.addCondition(lsSQL, _filter);
        
        //add order by and limit on query
        lsSQL = lsSQL + "ORDER BY " + _key + " LIMIT " + _max;
        
        ResultSet loRS = _nautilus.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            loJSON.put("result", "error");
            loJSON.put("message", "No record found.");
            return loJSON;
        }
        
        loJSON.put("result", "success");
        loJSON.put("payload", MiscUtil.RS2JSON(loRS, fsFields));
        
        //close resultset
        MiscUtil.close(loRS);
        return loJSON;
    }
    
    
    private String getSQ_Inventory(){
        return "SELECT" +
                    "  sStockIDx" +
                    ", sBarCodex" +
                    ", sDescript" +
                    ", sBriefDsc" +
                    ", sAltBarCd" +
                    ", sCategrCd" +
                    ", sBrandCde" +
                    ", sModelCde" +
                    ", sColorCde" +
                    ", sInvTypCd" +
                    ", nUnitPrce" +
                    ", nSelPrce1" +
                    ", cComboInv" +
                    ", cWthPromo" +
                    ", cSerialze" +
                    ", cInvStatx" +
                    ", sSupersed" +
                    ", cRecdStat" +
                " FROM Inventory";
    
    }
    
    private String getSQ_Inv_Master(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.sBarCodex" +
                    ", a.sDescript" +
                    ", a.sBriefDsc" +
                    ", a.sAltBarCd" +
                    ", a.sCategrCd" +
                    ", a.sBrandCde" +
                    ", a.sModelCde" +
                    ", a.sColorCde" +
                    ", a.sInvTypCd" +
                    ", a.nUnitPrce" +
                    ", a.nSelPrce1" +
                    ", a.cComboInv" +
                    ", a.cWthPromo" +
                    ", a.cSerialze" +
                    ", a.cInvStatx" +
                    ", a.sSupersed" +
                    ", b.sBranchCd" +
                    ", b.sLocatnCd" +
                    ", b.nBinNumbr" +
                    ", b.dAcquired" +
                    ", b.dBegInvxx" +
                    ", b.nBegQtyxx" +
                    ", b.nQtyOnHnd" +
                    ", b.nMinLevel" +
                    ", b.nMaxLevel" +
                    ", b.nAvgMonSl" +
                    ", b.nAvgCostx" +
                    ", b.cClassify" +
                    ", b.nBackOrdr" +
                    ", b.nResvOrdr" +
                    ", b.nFloatQty" +
                    ", b.cRecdStat" +
                    ", b.dDeactive" +
                " FROM Inventory a" +
                    ", Inv_Master b" +
                " WHERE a.sStockIDx = b.sStockIDx";
    }
}
