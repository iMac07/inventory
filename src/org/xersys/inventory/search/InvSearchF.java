package org.xersys.inventory.search;

import java.sql.ResultSet;
import java.util.ArrayList;
import org.json.simple.JSONObject;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.iSearch;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class InvSearchF implements iSearch{
    private final int DEFAULT_MAX_RESULT = 25;
    
    private XNautilus _app = null;  
    private String _message = "";
    private boolean _initialized = false;
    
    ArrayList<String> _filter;
    ArrayList<Object> _filter_value;
    
    ArrayList<String> _filter_list;
    ArrayList<String> _filter_description;
    
    ArrayList<String> _fields;
    ArrayList<String> _fields_descript;
    
    SearchType _search_type;
    String _search_key;
    Object _search_value;
    boolean _search_exact;
    int _search_result_max_row;
    
    public InvSearchF(XNautilus foApp, Object foValue){
        _app = foApp;
        _message = "";
        
        _search_type = (SearchType) foValue;
        
        if (_app != null && _search_type != null) {   
            _search_key = "";
            _search_value = null;
            _search_exact = false;
            _search_result_max_row = DEFAULT_MAX_RESULT;
            
            _filter = new ArrayList<>();
            _filter_value = new ArrayList<>();
            
            initFilterList();

            _initialized = true;
        }
    }

    /**
     * setKey(String fsValue)
     * \n
     * Set the field to use in searching
     * 
     * @param fsValue
     */
    @Override
    public void setKey(String fsValue) {
        _search_key = fsValue;
    }

    /**
     * setValue(Object foValue)
     * \n
     * Set the field value to use in searching
     * 
     * @param foValue
     */
    @Override
    public void setValue(Object foValue) {
        _search_value = foValue;
    }

    /**
     * setExact(boolean fbValue)
     * \n
     * Inform the object how the filter will be used on searching.
     * 
     * @param fbValue
     */
    @Override
    public void setExact(boolean fbValue) {
        _search_exact = fbValue;
    }
    
    /**
     * setMaxResult(int fnValue)
     * \n
     * Set the maximum row of results in searching
     * 
     * @param fnValue
     */
    @Override
    public void setMaxResult(int fnValue) {
        _search_result_max_row = fnValue;
    }
    
    /**
     * getValue()
     * \n
     * Get the search key value
     * 
     * @return 
     */
    @Override
    public Object getValue(){
        return _search_value;
    }
    
    /**
     * getMaxResult()
     * \n
     * Set the maximum row of results in searching
     * @return 
     */
    @Override
    public int getMaxResult() {
        return _search_result_max_row;
    }

    /**
     * getFilterListDescription(int fnRow)
     * \n
     * Get the description of filter fields.
     * 
     * @return ArrayList
     */
    @Override
    public ArrayList<String> getFilterListDescription() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return null;
        }
        
        return _filter_description;
    }
    
    /**
     * getColumns()
     * \n
     * Get fields to use in displaying results.
     * 
     * @return ArrayList
     */
    @Override
    public ArrayList<String> getColumns() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return null;
        }
        
        return _fields;
    }
    
    /**
     * getColumnNames()
     * \n
     * Get column names to use in displaying results.
     * 
     * @return ArrayList
     */
    @Override
    public ArrayList<String> getColumnNames() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return null;
        }
        
        return _fields_descript;
    }   

    /**
     * getFilter()()
     * \n
     * Get the list of fields and value the user set for filtering
     * 
     * @return ArrayList
     */
    @Override
    public ArrayList getFilter() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return null;
        }
        
        return _filter;
    }

    /**
     * addFilter(String fsField, Object foValue)
     * 
     * \n
     * Adds filter on searching
     * 
     * @param  fsField - field to filter
     * @param  foValue - field value
     * 
     * @return int - index of the field on the ArrayList
     * 
     * \n\t please see getFilterList() for available fields to use for filtering
     */
    @Override
    public int addFilter(String fsField, Object foValue) {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return -1;
        }
        
        if (_filter.isEmpty()){
            _filter.add(fsField);
            _filter_value.add(foValue);
            return _filter.size()-1;
        }
        
        for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++){
            if (_filter.get(lnCtr).toLowerCase().equals(fsField.toLowerCase())){
                _filter_value.set(lnCtr, foValue);
                return lnCtr;
            }
        }
            
        _filter.add(fsField);
        _filter_value.add(foValue);
        return _filter.size()-1;
    }
    
    /**
     * getFilterValue(String fsField)
     * \n
     * Get the value of a particular filter
     * 
     * @param fsField  - filter field to retrieve value
     * 
     * @return Object
     */
    @Override
    public Object getFilterValue(String fsField) {
        for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++){
            if (_filter.get(lnCtr).toLowerCase().equals(fsField.toLowerCase())){
                return _filter_value.get(lnCtr);
            }
        }
        
        return null;
    }
    
    /**
     * removeFilter(String fsField)
     * \n
     * Removes filter on searching
     * 
     * @param  fsField - filter field to remove in the in the ArrayList
     * 
     * @return Boolean
     * 
     * \n\t please see getFilterList() for available fields to use for filtering
     */
    @Override
    public boolean removeFilter(String fsField) {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return false;
        }
        
        if (!_filter.isEmpty()){        
            for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++){
                if (_filter.get(lnCtr).toLowerCase().equals(fsField.toLowerCase())){
                    _filter.remove(lnCtr);
                    _filter_value.remove(lnCtr);
                    return true;
                }
            }
        }
        
        _message = "Filter variable was empty.";
        return false;
    }
    
    /**
     * removeFilter()
     * \n
     * Removes all filter on searching
     * 
     * @return Boolean
     */
    @Override
    public boolean removeFilter() {
        _filter.clear();
        _filter_value.clear();
        return true;
    }

    
    /**
     * getMessage()
     * \n
     * Get the warning/error message from this object.
     * 
     * @return String
     */
    @Override
    public String getMessage() {
        return _message;
    }
    
    /**
     * Search()
     * \n
     * Execute search
     * 
     * @return JSONObject
     */
    @Override
    public JSONObject Search() {
        JSONObject loJSON = new JSONObject();
        
        if (!_initialized) {
            loJSON.put("result", "error");
            loJSON.put("message", "Object was not initialized.");
            return loJSON;
        }
        
        String lsSQL = "";
        
        //get the query for the particular search type
        if (null != _search_type)switch (_search_type) {
            case searchStocks:
            case searchStocksWithOtherInfo:
                lsSQL = getSQ_Inventory(); break;
            case searchBranchStocks:
            case searchBranchStocksWithOtherInfo:
                lsSQL = getSQ_Inv_Master(); break;
            case searchStocks4MCModel:
                lsSQL = getSQ_MC_Model(); break;
            case searchMCSerial:
                lsSQL = getSQ_MC_Serial(); break;
            case searchSPInvRequest:
                lsSQL = MiscUtil.addCondition(getSQ_SPInv_Request(), "sInvTypCd = 'SP'"); break;
            default:
                break;
        }
        
        if (lsSQL.isEmpty()){
            loJSON.put("result", "error");
            loJSON.put("message", "Query was not set for this type.");
            return loJSON;
        }
        
        //add condition
        if (_search_exact)
            lsSQL = MiscUtil.addCondition(lsSQL, _search_key + " = " + SQLUtil.toSQL(_search_value));
        else
            lsSQL = MiscUtil.addCondition(lsSQL, _search_key + " LIKE " + SQLUtil.toSQL("%" + _search_value + "%"));
        
        //add filter on query
        if (!_filter.isEmpty()){
            for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++){
                lsSQL = MiscUtil.addCondition(lsSQL, getFilterField(_filter.get(lnCtr)) + " LIKE " + SQLUtil.toSQL(_filter_value.get(lnCtr)));
            }
        }
        
        //add order by based on the search key
        lsSQL +=  " ORDER BY " + _search_key;
        
        //add the max row limit on query
        lsSQL +=  " LIMIT " + _search_result_max_row;
        
        try {
            ResultSet loRS = _app.executeQuery(lsSQL);
            //convert resultset to json array string
            lsSQL = MiscUtil.RS2JSON(loRS).toJSONString();
            //close the resultset
            MiscUtil.close(loRS);
            
            //assign the value to return
            loJSON.put("result", "success");
            loJSON.put("payload", lsSQL);
        } catch (Exception ex) {
            ex.printStackTrace();
            loJSON.put("result", "error");
            loJSON.put("result", "Exception detected.");
        }
        
        return loJSON;
    }
    
    private void initFilterList(){
        _filter_list = new ArrayList<>();
        _filter_description = new ArrayList<>();
        _fields = new ArrayList<>();
        _fields_descript = new ArrayList<>();
        
        if (null != _search_type)switch (_search_type) {
            case searchStocks:
            case searchStocksWithOtherInfo:
                _filter_list.add("a.sBrandCde"); _filter_description.add("Brand Code");
                _filter_list.add("a.sCategrCd"); _filter_description.add("Category Code");
                _filter_list.add("a.sColorCde"); _filter_description.add("Color Code");
                _filter_list.add("a.sInvTypCd"); _filter_description.add("Inv. Type Code");
                _filter_list.add("a.sModelCde"); _filter_description.add("Model Code");
                
                _fields.add("sBarCodex"); _fields_descript.add("Bar Code");
                _fields.add("sDescript"); _fields_descript.add("Description");
                _fields.add("nQtyOnHnd"); _fields_descript.add("On Hand");
                _fields.add("sBrandCde"); _fields_descript.add("Brand");
                _fields.add("sModelCde"); _fields_descript.add("Model");
                _fields.add("sColorCde"); _fields_descript.add("Color");
                break;
            case searchBranchStocks:
            case searchBranchStocksWithOtherInfo:
                _filter_list.add("a.sBrandCde"); _filter_description.add("Brand Code");
                _filter_list.add("a.sCategrCd"); _filter_description.add("Category Code");
                _filter_list.add("a.sColorCde"); _filter_description.add("Color Code");
                _filter_list.add("a.sInvTypCd"); _filter_description.add("Inv. Type Code");
                _filter_list.add("a.sModelCde"); _filter_description.add("Model Code");
                
                _fields.add("sBarCodex"); _fields_descript.add("Bar Code");
                _fields.add("sDescript"); _fields_descript.add("Description");
                _fields.add("nQtyOnHnd"); _fields_descript.add("On Hand");
                _fields.add("sBrandCde"); _fields_descript.add("Brand");
                _fields.add("sModelCde"); _fields_descript.add("Model");
                _fields.add("sColorCde"); _fields_descript.add("Color");
                break;
            case searchStocks4MCModel:
                _fields.add("sBarCodex"); _fields_descript.add("Bar Code");
                _fields.add("sDescript"); _fields_descript.add("Description");
                _fields.add("xBrandNme"); _fields_descript.add("Brand");
                _fields.add("xModelNme"); _fields_descript.add("Model");

                _filter_list.add("b.sDescript"); _filter_description.add("Brand");
                break;
            case searchMCSerial:
                _fields.add("sStockIDx"); _fields_descript.add("ID");
                _fields.add("sSerial01"); _fields_descript.add("Engine No.");
                _fields.add("sSerial02"); _fields_descript.add("Frame No.");
                _fields.add("xBrandNme"); _fields_descript.add("Brand");
                _fields.add("xModelNme"); _fields_descript.add("Model");

                _filter_list.add("c.sDescript"); _filter_description.add("Brand");
                _filter_list.add("d.sDescript"); _filter_description.add("Model");
                break;
            case searchSPInvRequest:
                _fields.add("sTransNox"); _fields_descript.add("Trans. No.");
                _fields.add("sRemarksx"); _fields_descript.add("Remarks");
                _fields.add("dTransact"); _fields_descript.add("Date");
                _fields.add("sReferNox"); _fields_descript.add("Refer. No.");

                _filter_list.add("sReferNox"); _filter_description.add("Refer. No.");
            default:
                break;
        }
    }
    
    private String getFilterField(String fsValue){
        String lsField = "";
        
        for(int lnCtr = 0; lnCtr <= _filter_description.size()-1; lnCtr++){
            if (_filter_description.get(lnCtr).toLowerCase().equals(fsValue.toLowerCase())){
                lsField = _filter_list.get(lnCtr);
                break;
            }
        }
        
        return lsField;
    }
    
    private String getSQ_Inventory(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.sBarCodex" +
                    ", a.sDescript" +
                    ", a.sBriefDsc" +
                    ", a.sAltBarCd" +
                    ", IFNULL(a.sCategrCd, '') sCategrCd" +
                    ", IFNULL(a.sBrandCde, '') sBrandCde" +
                    ", IFNULL(a.sModelCde, '') sModelCde" +
                    ", IFNULL(a.sColorCde, '') sColorCde" +
                    ", IFNULL(a.sInvTypCd, '') sInvTypCd" +
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
                    ", IFNULL(b.nQtyOnHnd, 0) nQtyOnHnd" +
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
                    " LEFT JOIN Inv_Master b ON a.sStockIDx = b.sStockIDx" +
                        " AND b.sBranchCd = " + SQLUtil.toSQL((String) _app.getBranchConfig("sBranchCd"));
    }
    
    private String getSQ_Inv_Master(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.sBarCodex" +
                    ", a.sDescript" +
                    ", a.sBriefDsc" +
                    ", a.sAltBarCd" +
                    ", IFNULL(a.sCategrCd, '') sCategrCd" +
                    ", IFNULL(a.sBrandCde, '') sBrandCde" +
                    ", IFNULL(a.sModelCde, '') sModelCde" +
                    ", IFNULL(a.sColorCde, '') sColorCde" +
                    ", IFNULL(a.sInvTypCd, '') sInvTypCd" +
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
                    ", IFNULL(b.nQtyOnHnd, 0) nQtyOnHnd" +
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
                " WHERE a.sStockIDx = b.sStockIDx" +
                    " AND b.sBranchCd = " + SQLUtil.toSQL((String) _app.getBranchConfig("sBranchCd"));
    }
    
    private String getSQ_MC_Model(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.sBarCodex" +
                    ", a.sDescript" +
                    ", a.sBrandCde" +
                    ", a.sModelCde" +
                    ", a.sColorCde" +
                    ", b.sDescript xBrandNme" +
                    ", c.sDescript xModelNme" +
                    ", '' xColorNme" +
                " FROM Inventory a" +
                    ", Brand b" +
                    ", Model c" +
                " WHERE a.sBrandCde = b.sBrandCde" +
                    " AND a.sModelCde = c.sModelCde" +
                    " AND a.sInvTypCd = 'MC'";
    }
    
    private String getSQ_MC_Serial(){
        return "SELECT" +
                    "  a.sSerialID" +
                    ", a.sSerial01" +
                    ", a.sSerial02" +
                    ", a.sStockIDx" +
                    ", b.sDescript xBrandNme" +
                    ", c.sDescript xModelNme" +
                " FROM Inv_Serial a" +
                    ", Inventory b" +
                    ", Brand c" +
                    ", Model d" +
                " WHERE a.sStockIDx = b.sStockIDx" +
                    " AND b.sBrandCde = c.sBrandCde" +
                    " AND b.sModelCde = d.sModelCde" +
                    " AND b.sInvTypCd = 'MC'";
    }
    
    private String getSQ_SPInv_Request(){
        return "SELECT" +
                    "  sTransNox" +
                    ", sRemarksx" +
                    ", dTransact" +
                    ", sReferNox" +
                " FROM Inv_Request_Master";
    }
    
    //let outside objects can call this variable without initializing the class.
    public static enum SearchType{
        searchStocks,
        searchBranchStocks,
        searchStocksWithOtherInfo,
        searchBranchStocksWithOtherInfo,
        searchStocks4MCModel,
        searchMCSerial,
        searchSPInvRequest,
        searchSPInvRequestCancel
    }
}