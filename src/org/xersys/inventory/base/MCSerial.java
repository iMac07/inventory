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
import org.xersys.commander.contants.Logical;
import org.xersys.commander.contants.SerialLocation;
import org.xersys.commander.contants.UnitType;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XRecord;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.inventory.search.InvSearchF;

public class MCSerial implements XRecord{    
    private final String MASTER_TABLE = "Inv_Serial";
    private final String INV_TYPE = "MC";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    private final boolean p_bWithParent;
    
    private final InvSearchF p_oModel;
    private final InvSearchF p_oSerial;
    
    private LRecordMas p_oListener;
    
    private String p_sMessagex;
    
    private int p_nEditMode;
    
    private CachedRowSet p_oMaster;
    
    public MCSerial(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oModel = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchStocks4MCModel);
        p_oSerial = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchMCSerial);
        
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
        
            if (p_nEditMode == EditMode.ADDNEW){
                Connection loConn = getConnection();

                p_oMaster.updateObject("sSerialID", MiscUtil.getNextCode(MASTER_TABLE, "sSerialID", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xBrandNme;xModelNme;xColorNme");
            } else {//old record
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xBrandNme;xModelNme;xColorNme",
                            "sSerialID = " + SQLUtil.toSQL(p_oMaster.getString("sSerialID")));
                
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
        } else {
            setMessage("No record loaded.");
            return false;
        }
            
    }

    @Override
    public boolean OpenRecord(String fsValue) {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        setMessage("");       
        
        try {
//            if (p_oMaster != null){
//                p_oMaster.first();
//
//                if (p_oMaster.getString("sStockIDx").equals(fsValue)){
//                    p_nEditMode  = EditMode.READY;
//                    return true;
//                }
//            }
            
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //open master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sSerialID = " + SQLUtil.toSQL(fsValue));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            
            if (p_oMaster.size() == 1) {                                     
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
            return p_oMaster.getObject(fsFieldNm);
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
                case "sStockIDx":
                    getModel((String) foValue);
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
    
    public JSONObject searchSerial(String fsKey, Object foValue, boolean fbExact){
        p_oSerial.setKey(fsKey);
        p_oSerial.setValue(foValue);
        p_oSerial.setExact(fbExact);
        
        return p_oSerial.Search();
    }
    
    public InvSearchF getSearchSerial(){
        return p_oSerial;
    }
    
    public JSONObject searchModel(String fsKey, Object foValue, boolean fbExact){
        p_oModel.setKey(fsKey);
        p_oModel.setValue(foValue);
        p_oModel.setExact(fbExact);
        
        return p_oModel.Search();
    }
    
    public InvSearchF getSearchModel(){
        return p_oModel;
    }
    
    private void getModel(String foValue){
        String lsProcName = this.getClass().getSimpleName() + ".getModel()";
        
        JSONObject loJSON = searchModel("a.sStockIDx", foValue, true);
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                p_oMaster.first();
                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oMaster.updateObject("sStockIDx", "");
                            p_oMaster.updateObject("xBrandNme", "");
                            p_oMaster.updateObject("xModelNme", "");
                            p_oMaster.updateObject("xColorNme", "");
                            p_oMaster.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oMaster.updateObject("sStockIDx", (String) loJSON.get("sStockIDx"));
                            p_oMaster.updateObject("xBrandNme", (String) loJSON.get("xBrandNme"));
                            p_oMaster.updateObject("xModelNme", (String) loJSON.get("xModelNme"));
                            p_oMaster.updateObject("xColorNme", (String) loJSON.get("xColorNme"));
                            p_oMaster.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oMaster.updateObject("sStockIDx", "");
                    p_oMaster.updateObject("xBrandNme", "");
                    p_oMaster.updateObject("xModelNme", "");
                    p_oMaster.updateObject("xColorNme", "");
                    p_oMaster.updateRow();
                }

                p_oListener.MasterRetreive("xBrandNme", (String) p_oMaster.getObject("xBrandNme"));
                p_oListener.MasterRetreive("xModelNme", (String) p_oMaster.getObject("xModelNme"));
                p_oListener.MasterRetreive("xColorNme", (String) p_oMaster.getObject("xColorNme"));
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
                    "  a.sSerialID" +
                    ", a.sBranchCd" +
                    ", a.sSerial01" +
                    ", a.sSerial02" +
                    ", a.nUnitPrce" +
                    ", a.sStockIDx" +
                    ", a.cLocation" +
                    ", a.cSoldStat" +
                    ", a.cUnitType" +
                    ", a.sCompnyID" +
                    ", a.sWarranty" +
                    ", a.dModified" +
                    ", IFNULL(c.sDescript, '') xBrandNme" +
                    ", IFNULL(d.sDescript, '') xModelNme" +
                    ", IFNULL(e.sColorNme, '') xColorNme" +
                " FROM Inv_Serial a" +
                    " LEFT JOIN Inventory b ON a.sStockIDx = b.sStockIDx AND b.sInvTypCd = 'MC'" +
                    " LEFT JOIN Brand c ON b.sBrandCde = c.sBrandCde AND c.sInvTypCd = 'MC'" +
                    " LEFT JOIN Model d ON b.sModelCde = d.sModelCde AND d.sInvTypCd = 'MC'" +
                    " LEFT JOIN Color e ON b.sColorCde = e.sColorIDx";
    }
    
    private void addMaster() throws SQLException{
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);
        p_oMaster.updateObject("cLocation", SerialLocation.CUSTOMER);
        p_oMaster.updateObject("cSoldStat", Logical.YES);
        p_oMaster.updateObject("cUnitType", UnitType.LIVE);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    private boolean isEntryOK(){
        try {
            p_oMaster.first();            

            if (p_oMaster.getString("sSerial01").length() > 20){
                setMessage("Engine no. length is too long.");
                return false;
            }
            
            if (p_oMaster.getString("sSerial02").length() > 20){
                setMessage("Frame no. length is too long.");
                return false;
            }
            
//            if (p_oMaster.getString("sStockIDx").isEmpty()){
//                setMessage("Model must not be empty.");
//                return false;
//            }
            
            if (p_oMaster.getString("sSerial01").isEmpty()){
                setMessage("Engine no. must not be empty.");
                return false;
            }
            
            if (p_oMaster.getString("sSerial02").isEmpty()){
                setMessage("Frame no. must not be empty.");
                return false;
            }   
            
            p_oMaster.first();
            p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getBranchConfig("sBranchCd"));
            p_oMaster.updateRow();

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
