package org.xersys.inventory.search;

import java.sql.ResultSet;
import java.util.ArrayList;
import org.json.simple.JSONObject;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.iSearch;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class InvSearchF implements iSearch{
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
            _search_result_max_row = 15;
            
            _filter = new ArrayList<>();
            _filter_value = new ArrayList<>();
            
            initFilterList();

            _initialized = true;
        }
    }
    
    /**
     * setType(Object foValue)
     * \n
     * Set the search type to use in this object
     * 
     * @param foValue
     * 
     * \n\t please see SearchType()
     */
//    @Override
//    public void setType(Object foValue) {
//        _search_type = (SearchType) foValue;
//        
//        if (_search_type != null) initFilterList();
//    }

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
     * String getFilterList()
     * \n
     * Get the allowable fields to be used search filtering
     * 
     * @return String
     */
    @Override
    public String getFilterList() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return "";
        }
        
        String lsValue = "Search available filter list to use:";
        
        for (int lnCtr = 0; lnCtr <= _filter_list.size()-1; lnCtr++) 
            lsValue += "\n\t" + _filter_list.get(lnCtr) + " - " + _filter_description.get(lnCtr);
        
        return lsValue;
    }
    
    /**
     * getFilterListCount()
     * \n
     * Get count of available filters
     * 
     * @return 
     */
    @Override
    public int getFilterListCount() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return 0;
        }
        
        return _filter_list.size();
    }

    /**
     * getFilterListDescription(int fnRow)
     * \n
     * Get the description of a particular filter.
     * 
     * @param fnRow - index of the filter on ArrayList
     * 
     * @return String
     */
    @Override
    public String getFilterListDescription(int fnRow) {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return "";
        }
        
        return _filter_description.get(fnRow);
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
     * String getFilter()()
     * \n
     * Get the list of fields and value the user set for filtering
     * 
     * @return String
     */
    @Override
    public String getFilter() {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return "";
        }
        
        String lsValue = "Search filter you defined:";
        
        for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++) 
            lsValue = "\n\t" + _filter.get(lnCtr) + " = " + _filter_value.get(lnCtr);
        
        return lsValue;
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
        
        //check if the field was already used
        for (int lnCtr = 0; lnCtr <= _filter.size()-1; lnCtr++){
            if (_filter.get(lnCtr).toLowerCase().equals(fsField.toLowerCase())){
                _filter.set(lnCtr, fsField);
                _filter_value.set(lnCtr, foValue);
                return lnCtr;
            }
        }
        
        //add new filter
        _filter.add(fsField);
        _filter_value.add(foValue);
        
        return _filter.size()-1;
    }
    
    /**
     * removeFilter(int fnRow)
     * \n
     * Removes filter on searching
     * 
     * @param  fnRow - index of filter field in the in the ArrayList
     * 
     * @return Boolean
     * 
     * \n\t please see getFilterList() for available fields to use for filtering
     */
    @Override
    public boolean removeFilter(int fnRow) {
        if (!_initialized) {
            _message = "Object was not initialized.";
            return false;
        }
        
        if (!_filter.isEmpty()){
            _filter.remove(fnRow);
            _filter_value.remove(fnRow);
            return true;
        }
        
        _message = "Filter variable was empty.";
        return false;
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
                lsSQL = getSQ_Inventory(); break;
            case searchStocksWithOtherInfo:
                break;
            case searchBranchStocks:
                break;
            case searchBranchStocksWithOtherInfo:
                break;
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
                lsSQL = MiscUtil.addCondition(lsSQL, _filter.get(lnCtr) + " = " + SQLUtil.toSQL(_filter_value.get(lnCtr)));
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
                _filter_list.add("sBarCodex"); _filter_description.add("Part No.");
                _filter_list.add("sBrandCde"); _filter_description.add("Brand Code");
                _filter_list.add("sCategrCd"); _filter_description.add("Category Code");
                _filter_list.add("sColorCde"); _filter_description.add("Color Code");
                _filter_list.add("sDescript"); _filter_description.add("Description");
                _filter_list.add("sInvTypCd"); _filter_description.add("Inv. Type Code");
                _filter_list.add("sModelCde"); _filter_description.add("Model Code");
                
                _fields.add("sBarCodex"); _fields_descript.add("Part No.");
                _fields.add("sDescript"); _fields_descript.add("Brand Code");
                _fields.add("sBrandCde"); _fields_descript.add("Model Code");
                _fields.add("sModelCde"); _fields_descript.add("Description");
                _fields.add("sColorCde"); _fields_descript.add("Color Code");
                break;
            case searchStocksWithOtherInfo:
                break;
            case searchBranchStocks:
                break;
            case searchBranchStocksWithOtherInfo:
                break;
            default:
                break;
        }
    }
    
    private String getSQ_Inventory(){
        return "SELECT" +
                    "  sStockIDx" +
                    ", sBarCodex" +
                    ", sDescript" +
                    ", sBriefDsc" +
                    ", sAltBarCd" +
                    ", IFNULL(sCategrCd, '') sCategrCd" +
                    ", IFNULL(sBrandCde, '') sBrandCde" +
                    ", IFNULL(sModelCde, '') sModelCde" +
                    ", IFNULL(sColorCde, '') sColorCde" +
                    ", IFNULL(sInvTypCd, '') sInvTypCd" +
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
    
    //let outside objects can call this variable without initializing the class.
    public static enum SearchType{
        searchStocks,
        searchBranchStocks,
        searchStocksWithOtherInfo,
        searchBranchStocksWithOtherInfo
    }
}