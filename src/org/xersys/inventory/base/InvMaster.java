package org.xersys.inventory.base;

import org.xersys.commander.iface.LRecordMas;
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
import org.xersys.commander.contants.RecordStatus;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.iface.XRecord;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;
import org.xersys.parameters.search.ParamSearchF;

public class InvMaster implements XRecord{    
    private final String MASTER_TABLE = "Inv_Master";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    private final boolean p_bWithParent;
    
    private LRecordMas p_oListener;
    
    private String p_sMessagex;
    
    private int p_nEditMode;
    
    private CachedRowSet p_oMaster;
    
    private final ParamSearchF p_oInvLocation;
    
    public InvMaster(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oInvLocation = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchInvLocation);
        
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
            ex.printStackTrace();
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
        
        String lsSQL = "";

        try {
            if (!isModified()) return true;
            
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            setMaster("dModified", p_oNautilus.getServerDate());
            
            if (p_nEditMode == EditMode.ADDNEW){                
                setMaster("nQtyOnHnd", p_oMaster.getObject("nBegQtyxx"));
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xLocatnNm");
            } else {                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xLocatnNm", 
                            "sStockIDx = " + SQLUtil.toSQL(p_oMaster.getString("sStockIDx")) +
                                " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd));
            }
            
            if (lsSQL.equals("")){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                
                setMessage("No record to update");
                return false;
            }
            
            if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                if(!p_oNautilus.getMessage().isEmpty())
                    setMessage(p_oNautilus.getMessage());
                else
                    setMessage("No record updated");
            } 

            if (!p_bWithParent) {
                if(!p_oNautilus.getMessage().isEmpty())
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
        
        p_nEditMode = EditMode.UNKNOWN;
        return true;
    }

    @Override
    public boolean UpdateRecord() {
        if (p_nEditMode == EditMode.READY){
            p_nEditMode = EditMode.UPDATE;
            return true;
        }
        
        return false;
    }

    @Override
    public boolean OpenRecord(String fsValue) {
        System.out.println(this.getClass().getSimpleName() + ".OpenRecord()");
        setMessage("");       
        
        try {            
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //open master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                        " sStockIDx = " + SQLUtil.toSQL(fsValue) +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd));
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
                case "sLocatnCd":
                    getInvLocation((String) foValue);
                    break;
                case "nBinNumbr":
                case "nBegQtyxx":
                case "nQtyOnHnd":
                case "nMinLevel":
                case "nMaxLevel":
                case "nBackOrdr":
                case "nResvOrdr":
                case "nFloatQty":
                    p_oMaster.first();
                    if (!StringUtil.isNumeric(String.valueOf(foValue)))
                        p_oMaster.updateObject(fsFieldNm, 0);
                    else
                        p_oMaster.updateObject(fsFieldNm, foValue);
                    
                    p_oMaster.updateRow();
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
                    break;
                case "nAvgMonSl":
                case "nAvgCostx":
                    p_oMaster.first();
                    if (!StringUtil.isNumeric(String.valueOf(foValue)))
                        p_oMaster.updateObject(fsFieldNm, 0.00);
                    else
                        p_oMaster.updateObject(fsFieldNm, foValue);
                    
                    p_oMaster.updateRow();
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(fsFieldNm, p_oMaster.getObject(fsFieldNm));
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
    
    public ResultSet getHistory(){
        if (p_nEditMode != EditMode.READY) return null;
        
        try {
            String lsSQL = getSQ_Ledger();
            lsSQL = "SELECT * FROM (" + lsSQL + ") x" +
                    " ORDER BY dTransact, sTransNox;";
            lsSQL = lsSQL.replace("xStockIDx", p_oMaster.getString("sStockIDx"));

            return  p_oNautilus.executeQuery(lsSQL);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.sBranchCd" +
                    ", a.sLocatnCd" +
                    ", a.nBinNumbr" +
                    ", a.dAcquired" +
                    ", a.dBegInvxx" +
                    ", a.nBegQtyxx" +
                    ", a.nQtyOnHnd" +
                    ", a.nMinLevel" +
                    ", a.nMaxLevel" +
                    ", a.nAvgMonSl" +
                    ", a.nAvgCostx" +
                    ", a.cClassify" +
                    ", a.nBackOrdr" +
                    ", a.nResvOrdr" +
                    ", a.nFloatQty" +
                    ", a.cRecdStat" +
                    ", a.dDeactive" +
                    ", a.dModified" +
                    ", IFNULL(b.sBriefDsc, '') xLocatnNm" +
                " FROM " + MASTER_TABLE + " a" +
                    " LEFT JOIN Inv_Location b ON a.sLocatnCd = b.sLocatnCd";
    }
    
    private String getSQ_Ledger(){
        return "SELECT" +
                    "  'Inventory Adjustment' xSourceNm" +
                    ", a.dTransact" + 
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", b.nDebitQty" +
                    ", b.nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" +	
                " FROM Inv_Adjustment_Master a" +
                    ", Inv_Adjustment_Detail b" +
                        " LEFT JOIN Inventory c ON b.sStockIDx = c.sStockIDx" +
                " WHERE a.sTransNox = b.sTransNox" +
                    " AND a.cTranStat IN ('2')" +
                    " AND b.sStockIDx = 'xStockIDx'" +
                " UNION" +
                " SELECT" +
                    "  'SP Retail' xSourceNm" +
                    ", a.dTransact" + 
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", 0 nDebitQty" +
                    ", b.nQuantity nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" +
                " FROM SP_Sales_Master a" +
                    ", SP_Sales_Detail b" +
                        " LEFT JOIN Inventory c ON b.sStockIDx = c.sStockIDx" +
                " WHERE a.sTransNox = b.sTransNox" +
                    " AND a.cTranStat NOT IN ('3')" +
                    " AND b.sStockIDx = 'xStockIDx'" +
                " UNION" +
                    " SELECT" +
                        "  'Job Order' xSourceNm" +
                        ", a.dTransact" + 
                        ", c.sBarCodex" +
                        ", c.sDescript" +
                        ", 0 nDebitQty" +
                        ", b.nQuantity nCredtQty" +
                        ", a.sJobDescr sRemarksx" +
                        ", b.sStockIDx" +
                        ", a.sTransNox" +
                        ", a.cTranStat" +
                " FROM Job_Order_Master a" +
                    ", Job_Order_Parts b" +
                        " LEFT JOIN Inventory c ON b.sStockIDx = c.sStockIDx" +
                " WHERE a.sTransNox = b.sTransNox" +
                    " AND a.cTranStat NOT IN ('3')" +
                    " AND b.sStockIDx = 'xStockIDx'" +
                " UNION" +
                " SELECT" +
                    "  'PO Receiving' xSourceNm" +
                    ", a.dTransact" + 
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", b.nQuantity nDebitQty" +
                    ", '0' nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" +
                " FROM PO_Receiving_Master a" +
                    ", PO_Receiving_Detail b" +
                        " LEFT JOIN Inventory c ON b.sStockIDx = c.sStockIDx" +
                " WHERE a.sTransNox = b.sTransNox" +
                    " AND a.cTranStat NOT IN ('0', '3')" +
                    " AND b.sStockIDx = 'xStockIDx'" +
                " UNION" +
                " SELECT" +
                    "  'PO Return' xSourceNm" +
                    ", a.dTransact" + 
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", 0 nDebitQty" + 
                    ", b.nQuantity nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" +
                " FROM PO_Return_Master a" +
                    ", PO_Return_Detail b" +
                        " LEFT JOIN Inventory c ON b.sStockIDx = c.sStockIDx" +
                " WHERE a.sTransNox = b.sTransNox" +
                    " AND a.cTranStat NOT IN ('0', '3')" +
                    " AND b.sStockIDx = 'xStockIDx'";
    }
    
    private void addMaster() throws SQLException{
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);
        p_oMaster.updateObject("sBranchCd", p_sBranchCd);
        p_oMaster.updateObject("dAcquired", p_oNautilus.getServerDate());
        p_oMaster.updateObject("dBegInvxx", p_oNautilus.getServerDate());
        p_oMaster.updateObject("cClassify", "F");
        p_oMaster.updateObject("cRecdStat", RecordStatus.ACTIVE);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }

    @Override
    public Object getMaster(int fnIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setMaster(int fnIndex, Object foValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public JSONObject searchInvLocation(String fsKey, Object foValue, boolean fbExact){
        p_oInvLocation.setKey(fsKey);
        p_oInvLocation.setValue(foValue);
        p_oInvLocation.setExact(fbExact);
        
        return p_oInvLocation.Search();
    }
    
    public ParamSearchF getSearchInvLocation(){
        return p_oInvLocation;
    }
    
    private void getInvLocation(String foValue){
        String lsProcName = this.getClass().getSimpleName() + ".getInvType()";
        
        JSONObject loJSON = searchInvLocation("sLocatnCd", foValue, true);
        if ("success".equals((String) loJSON.get("result"))){
            try {
                JSONParser loParser = new JSONParser();

                p_oMaster.first();
                try {
                    JSONArray loArray = (JSONArray) loParser.parse((String) loJSON.get("payload"));

                    switch (loArray.size()){
                        case 0:
                            p_oMaster.updateObject("sLocatnCd", "");
                            p_oMaster.updateObject("xLocatnNm", "");
                            p_oMaster.updateRow();
                            break;
                        default:
                            loJSON = (JSONObject) loArray.get(0);
                            p_oMaster.updateObject("sLocatnCd", (String) loJSON.get("sLocatnCd"));
                            p_oMaster.updateObject("xLocatnNm", (String) loJSON.get("sBriefDsc"));
                            p_oMaster.updateRow();
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                    p_oListener.MasterRetreive("sLocatnCd", "");
                    p_oListener.MasterRetreive("xLocatnNm", "");
                    p_oMaster.updateRow();
                }

                if (p_oListener != null) p_oListener.MasterRetreive("sLocatnCd", (String) getMaster("xLocatnNm"));
            } catch (SQLException ex) {
                ex.printStackTrace();
                setMessage("SQLException on " + lsProcName + ". Please inform your System Admin.");
            }
        }
    }
    
    public boolean isModified() throws SQLException{
        if (p_nEditMode == EditMode.ADDNEW)
            return true;
        else if (p_nEditMode == EditMode.UPDATE){
            p_oMaster.first();
            String lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xLocatnNm", 
                            "sStockIDx = " + SQLUtil.toSQL(p_oMaster.getString("sStockIDx")) +
                                " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd));
            return !lsSQL.isEmpty();
        } else
            return false;
    }
}
