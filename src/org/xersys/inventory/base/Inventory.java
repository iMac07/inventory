package org.xersys.inventory.base;

import org.xersys.commander.iface.LRecordMas;
import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.InventoryStatus;
import org.xersys.commander.contants.RecordStatus;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XRecord;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;
import org.xersys.inventory.search.InvSearchF;
import org.xersys.parameters.search.ParamSearchF;

public class Inventory implements XRecord{    
    private final String MASTER_TABLE = "Inventory";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    private final boolean p_bWithParent;
    
    private final InvSearchF p_oStocks;
    private final ParamSearchF p_oBrand;
    private final ParamSearchF p_oModel;
    private final ParamSearchF p_oInvType;
    
    private InvMaster p_oInvMaster;
    private LRecordMas p_oListener;
    
    private String p_sMessagex;
    
    private int p_nEditMode;
    
    private CachedRowSet p_oMaster;
    
    
    
    public Inventory(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oInvMaster = new InvMaster(foNautilus, fsBranchCd, true);
        
        p_oBrand = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchBrand);
        p_oModel = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchModel);
        p_oInvType = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchInvType);
        p_oStocks = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchStocks);
        
        p_nEditMode = EditMode.UNKNOWN;
    }
    
    @Override
    public int getEditMode() {
        return p_nEditMode;
    }
    
    @Override
    public boolean NewRecord() {
        System.out.println(this.getClass().getSimpleName() + ".NewRecord()");
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        try {
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            addMaster();
            
            p_oInvMaster = new InvMaster(p_oNautilus, p_sBranchCd, true);
        } catch (SQLException ex) {
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.ADDNEW;
        return true;
    }

    @Override
    public boolean SaveRecord() {
        System.out.println(this.getClass().getSimpleName() + ".SaveRecord()");
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return false;
        }
        
        setMessage("");
        String lsSQL = "";
        
        
        if (!isEntryOK()) return false;

        try {
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if ("".equals((String) getMaster("sStockIDx"))){ //new record
                Connection loConn = getConnection();

                p_oMaster.updateObject("sStockIDx", MiscUtil.getNextCode(MASTER_TABLE, "sStockIDx", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xBrandNme;xModelNme;xInvTypNm");
            } else {//old record
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xBrandNme;xModelNme;xInvTypNm",
                            "sStockIDx = " + SQLUtil.toSQL(p_oMaster.getString("sStockIDx")));
                
                if (!lsSQL.equals("")){
                    if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                        if(!p_oNautilus.getMessage().isEmpty())
                            setMessage(p_oNautilus.getMessage());
                        else
                            setMessage("No record updated");
                    }
                }
            }            

            if (!lsSQL.isEmpty()){
                if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                    if(!p_oNautilus.getMessage().isEmpty())
                        setMessage(p_oNautilus.getMessage());
                    else
                        setMessage("No record updated");
                } 
            }
            
            if (!p_bWithParent){
                if (!p_sMessagex.isEmpty()){
                    p_oNautilus.rollbackTrans();
                    return false;
                }
            }
            
            if (p_nEditMode == EditMode.UPDATE){
                if (p_oInvMaster.getEditMode() == EditMode.ADDNEW){
                    p_oInvMaster.setMaster("sStockIDx", getMaster("sStockIDx"));
                    if (!p_oInvMaster.SaveRecord()){
                        setMessage(p_oInvMaster.getMessage());
                    }
                }
            }
            
            if (!p_bWithParent){
                if (!p_sMessagex.isEmpty())
                    p_oNautilus.rollbackTrans();
                else
                    p_oNautilus.commitTrans();        
            }
        } catch (SQLException ex) {
            if (!p_bWithParent) p_oNautilus.rollbackTrans();
            ex.printStackTrace();
            setMessage(ex.getMessage());
            return false;
        }
        
        if (p_sMessagex.isEmpty()){
            p_nEditMode = EditMode.UNKNOWN;
            return true;
        } else return false;
    }

    @Override
    public boolean UpdateRecord() {
        if (p_nEditMode == EditMode.READY){
            p_nEditMode = EditMode.UPDATE;
            return true;
        } else return false;
    }

    @Override
    public boolean OpenRecord(String fsValue) {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        setMessage("");       
        
        try {
            if (p_oMaster != null){
                p_oMaster.first();

                if (p_oMaster.getString("sStockIDx").equals(fsValue)){
                    p_nEditMode  = EditMode.READY;
                    return true;
                }
            }
            
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //open master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sStockIDx = " + SQLUtil.toSQL(fsValue));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            
            if (p_oMaster.size() == 1) {                     
                p_oInvMaster.OpenRecord(fsValue);
                
                p_nEditMode  = EditMode.READY;
                return true;
            }
            
            setMessage("No record loaded.");
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        p_nEditMode  = EditMode.UNKNOWN;
        return false;
    }

    @Override
    public boolean DeleteRecord(String fsTransNox) {
        return false;
    }

    @Override
    public boolean DeactivateRecord(String fsTransNox) {
        return false;
    }

    @Override
    public boolean ActivateRecord(String fsTransNox) {
        return false;
    }
    
    @Override
    public String getMessage() {
        return p_sMessagex;
    }

    @Override
    public void setListener(Object foListener) {
        p_oListener = (LRecordMas) foListener;
    }

    @Override
    public Object getMaster(String fsFieldNm){
        try {
            p_oMaster.first();
            switch (fsFieldNm){
                case "nBinNumbr":
                case "dAcquired":
                case "dBegInvxx":
                case "nBegQtyxx":
                    return p_oInvMaster.getMaster(fsFieldNm);
                default:
                    return p_oMaster.getObject(fsFieldNm);
            }
        } catch (SQLException ex) {
            return null;
        }
    }

    @Override
    public void setMaster(String fsFieldNm, Object foValue){
        String lsProcName = this.getClass().getSimpleName() + ".setMaster()";
        
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return;
        }
        
        try {
            switch (fsFieldNm){
                case "sCategrCd":
                    break;
                case "sBrandCde":
                    getBrand((String) foValue);
                    break;
                case "sModelCde":
                    getModel((String) foValue);
                    break;
                case "sColorCde":
                    break;
                case "sInvTypCd":
                    getInvType((String) foValue);
                    break;
                case "nUnitPrce":
                case "nSelPrce1":
                    p_oMaster.first();
                    if (!StringUtil.isNumeric(String.valueOf(foValue)))
                        p_oMaster.updateObject(fsFieldNm, 0.00);
                    else
                        p_oMaster.updateObject(fsFieldNm, foValue);
                    
                    p_oMaster.updateRow();
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
                    break;
                case "nBinNumbr":
                case "dAcquired":
                case "dBegInvxx":
                case "nBegQtyxx":
                    p_oInvMaster.setMaster(fsFieldNm, foValue);
                    if (p_oListener != null) p_oListener.MasterRetreive(fsFieldNm, p_oInvMaster.getMaster(fsFieldNm));
                    break;
                default:
                    p_oMaster.first();
                    p_oMaster.updateObject(fsFieldNm, foValue);
                    p_oMaster.updateRow();

                    if (p_oListener != null) p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
        }
    }
    
    public InvMaster getInvMaster(){
        return p_oInvMaster;
    }
    
    public JSONObject searchStocks(String fsKey, Object foValue, boolean fbExact){
        p_oStocks.setKey(fsKey);
        p_oStocks.setValue(foValue);
        p_oStocks.setExact(fbExact);
        
        return p_oStocks.Search();
    }
    
    public InvSearchF getSearchStocks(){
        return p_oStocks;
    }
    
    public JSONObject searchModel(String fsKey, Object foValue, boolean fbExact){
        p_oModel.setKey(fsKey);
        p_oModel.setValue(foValue);
        p_oModel.setExact(fbExact);
        
        if (((String) getMaster("sInvTypCd")).isEmpty())
            p_oModel.removeFilter("Inv. Type Code");
        else
            p_oModel.addFilter("Inv. Type Code", (String) getMaster("sInvTypCd"));

        if (((String) getMaster("sBrandCde")).isEmpty())
            p_oModel.removeFilter("Brand Code");
        else
            p_oModel.addFilter("Brand Code", (String) getMaster("sBrandCde"));
        
        return p_oModel.Search();
    }
    
    public ParamSearchF getSearchModel(){
        return p_oModel;
    }
    
    public JSONObject searchBrand(String fsKey, Object foValue, boolean fbExact){
        p_oBrand.setKey(fsKey);
        p_oBrand.setValue(foValue);
        p_oBrand.setExact(fbExact);
        
        if (((String) getMaster("sInvTypCd")).isEmpty())
            p_oBrand.removeFilter("Inv. Type Code");
        else
            p_oBrand.addFilter("Inv. Type Code", (String) getMaster("sInvTypCd"));

        return p_oBrand.Search();
    }
    
    public ParamSearchF getSearchBrand(){
        return p_oBrand;
    }
    
    public JSONObject searchInvType(String fsKey, Object foValue, boolean fbExact){
        p_oInvType.setKey(fsKey);
        p_oInvType.setValue(foValue);
        p_oInvType.setExact(fbExact);
        
        return p_oInvType.Search();
    }
    
    public ParamSearchF getSearchInvType(){
        return p_oInvType;
    }
    
    private void getInvType(String foValue){
        String lsProcName = this.getClass().getSimpleName() + ".getInvType()";
        
        JSONObject loJSON = searchInvType("sInvTypCd", foValue, true);
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                p_oMaster.first();
                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oMaster.updateObject("sInvTypCd", "");
                            p_oMaster.updateObject("xInvTypNm", "");
                            p_oMaster.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oMaster.updateObject("sInvTypCd", (String) loJSON.get("sInvTypCd"));
                            p_oMaster.updateObject("xInvTypNm", (String) loJSON.get("sDescript"));
                            p_oMaster.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oListener.MasterRetreive("sInvTypCd", "");
                    p_oListener.MasterRetreive("xInvTypNm", "");
                    p_oMaster.updateRow();
                }

                p_oListener.MasterRetreive("sInvTypCd", (String) getMaster("xInvTypNm"));
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            }
        }
    }
    
    private void getModel(String foValue){
        String lsProcName = this.getClass().getSimpleName() + ".getModel()";
        
        JSONObject loJSON = searchModel("a.sModelCde", foValue, true);
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                p_oMaster.first();
                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oMaster.updateObject("sModelCde", "");
                            p_oMaster.updateObject("xModelNme", "");
                            p_oMaster.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oMaster.updateObject("sModelCde", (String) loJSON.get("sModelCde"));
                            p_oMaster.updateObject("xModelNme", (String) loJSON.get("sModelNme"));
                            p_oMaster.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oListener.MasterRetreive("sModelCde", "");
                    p_oListener.MasterRetreive("xModelNme", "");
                    p_oMaster.updateRow();
                }

                p_oListener.MasterRetreive("sModelCde", (String) p_oMaster.getObject("xModelNme"));
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            }
        }
    }
    
    private void getBrand(String foValue) throws SQLException{ 
        String lsProcName = this.getClass().getSimpleName() + ".getBrand()";
        
        JSONObject loJSON = searchBrand("a.sBrandCde", foValue, true);
        
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                p_oMaster.first();
                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oMaster.updateObject("sBrandCde", "");
                            p_oMaster.updateObject("xBrandNme", "");
                            p_oMaster.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oMaster.updateObject("sBrandCde", (String) loJSON.get("sBrandCde"));
                            p_oMaster.updateObject("xBrandNme", (String) loJSON.get("sDescript"));
                            p_oMaster.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oListener.MasterRetreive("sBrandCde", "");
                    p_oListener.MasterRetreive("xBrandNme", "");
                    p_oMaster.updateRow();
                }

                p_oListener.MasterRetreive("sBrandCde", (String) p_oMaster.getObject("xBrandNme"));
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            }
        }
    }
    
    private Connection getConnection(){         
        Connection foConn;
        
        if (p_bWithParent){
            foConn = (Connection) p_oNautilus.getConnection().getConnection();
            
            if (foConn == null) foConn = (Connection) p_oNautilus.doConnect();
        } else 
            foConn = (Connection) p_oNautilus.doConnect();
        
        return foConn;
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
    }
    
    private String getSQ_Master(){
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
                    ", a.cRecdStat" +
                    ", a.dModified" +
                    ", IFNULL(b.sDescript, '') xBrandNme" +
                    ", IFNULL(c.sDescript, '') xModelNme" +
                    ", IFNULL(d.sDescript, '') xInvTypNm" +
                " FROM " + MASTER_TABLE + " a" +
                    " LEFT JOIN Brand b ON a.sBrandCde = b.sBrandCde" +
                    " LEFT JOIN Model c ON a.sModelCde = c.sModelCde" +
                    " LEFT JOIN Inv_Type d ON a.sInvTypCd = d.sInvTypCd";
    }
    
    private void addMaster() throws SQLException{
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);
        p_oMaster.updateObject("cComboInv", "0");
        p_oMaster.updateObject("cWthPromo", "0");
        p_oMaster.updateObject("cSerialze", "0");
        p_oMaster.updateObject("cInvStatx", InventoryStatus.ACTIVE);
        p_oMaster.updateObject("cRecdStat", RecordStatus.ACTIVE);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    private boolean isEntryOK(){
        try {
            p_oMaster.first();
            
            if (p_oMaster.getString("sBarCodex").length() > 20){
                setMessage("Bar code length is too long.");
                return false;
            }
            
            if (p_oMaster.getString("sDescript").length() > 64){
                setMessage("Description length is too long.");
                return false;
            }
            
            if (p_oMaster.getString("sBriefDsc").length() > 20){
                setMessage("Brief description length is too long.");
                return false;
            }
            
            if (p_oMaster.getString("sAltBarCd").length() > 20){
                setMessage("Alternate bar code length is too long.");
                return false;
            }
            
            if (p_oMaster.getString("sBarCodex").isEmpty()){
                setMessage("Bar code must not be empty.");
                return false;
            }
            
            if (p_oMaster.getString("sDescript").isEmpty()){
                setMessage("Description must not be empty.");
                return false;
            }
            
            if (p_oMaster.getString("sBriefDsc").isEmpty()){
                setMessage("Brief description must not be empty.");
                return false;
            }          

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return false;
        }
    }

    @Override
    public Object getMaster(int fnIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setMaster(int fnIndex, Object foValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}