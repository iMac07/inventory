package org.xersys.inventory.base;

import com.mysql.jdbc.Connection;
import com.sun.rowset.CachedRowSetImpl;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xersys.commander.contants.AccessLevel;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.RecordStatus;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.contants.UserLevel;
import org.xersys.commander.iface.LApproval;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.iface.XMasDetTrans;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.CommonUtil;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.inventory.search.InvSearchF;
import org.xersys.lib.pojo.Temp_Transactions;

public class InvPriceUpdate implements XMasDetTrans{
    private final String MASTER_TABLE = "Price_Change_Master";
    private final String DETAIL_TABLE = "Price_Change_Detail";
    private final String SOURCE_CODE = "SPPU";
    
    private final XNautilus p_oNautilus;
    private final boolean p_bWithParent;
    private final String p_sBranchCd;
    
    private LMasDetTrans p_oListener;
    private boolean p_bSaveToDisk;
    private boolean p_bWithUI;
    
    private String p_sOrderNox;
    private LApproval p_oApproval;
    
    private String p_sMessagex;
    
    private int p_nEditMode;
    private int p_nTranStat;
    
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oDetail;
    
    private final InvSearchF p_oSearchItem;
    private final InvSearchF p_oSearchTrans;
    
    private ArrayList<Temp_Transactions> p_oTemp;

    public InvPriceUpdate(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_bWithUI = true;
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        p_nEditMode = EditMode.UNKNOWN;
        
        p_oSearchItem = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchStocks);
        p_oSearchTrans = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvPriceChange);
        
        loadTempTransactions();
    }
    
    public InvPriceUpdate(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent, int fnTranStat){
        p_bWithUI = true;
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        p_nTranStat = fnTranStat;
        p_nEditMode = EditMode.UNKNOWN;
        
        p_oSearchItem = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchStocks);
        p_oSearchTrans = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvAdjustment);
        
        loadTempTransactions();
    }
    
    @Override
    public void setListener(LMasDetTrans foValue) {
        p_oListener = foValue;
    }
    
    public void setApprvListener(LApproval foValue){
        p_oApproval = foValue;
    }

    @Override
    public void setSaveToDisk(boolean fbValue) {
        p_bSaveToDisk = fbValue;
    }

    @Override
    public void setMaster(int fnIndex, Object foValue) {
        if (p_nEditMode != EditMode.ADDNEW){
            System.err.println("Transaction is not on update mode.");
            return;
        }
        
        try {
            p_oMaster.first();
            
            switch (fnIndex){
                case 5: //sReferNox
                case 6: //sRemarksx
                    p_oMaster.updateString(fnIndex, (String) foValue);
                    p_oMaster.updateRow();
                    break;
                case 4: //dEffectve
                    p_oMaster.updateObject(fnIndex, foValue);
                    p_oMaster.updateRow();
            }
            
            if (p_oListener != null) p_oListener.MasterRetreive(fnIndex, getMaster(fnIndex));
             
            saveToDisk(RecordStatus.ACTIVE, "");
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
        }
    }
    
    @Override
    public void setMaster(String fsFieldNm, Object foValue) {
        try {
            setMaster(MiscUtil.getColumnIndex(p_oMaster, fsFieldNm), foValue);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object getMaster(String fsFieldNm) {
        try {
            return getMaster(MiscUtil.getColumnIndex(p_oMaster, fsFieldNm));
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return null;
        }
    }

    @Override
    public Object getMaster(int fnIndex) {
        if (fnIndex == 0) return null;
        
        try {
            p_oMaster.first();
            return p_oMaster.getObject(fnIndex);
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return null;
        }
    }
    
    @Override
    public void setDetail(int fnRow, int fnIndex, Object foValue) {
        if (p_nEditMode != EditMode.ADDNEW){
            System.err.println("Transaction is not on update mode.");
            return;
        }
        
        try {            
            switch (fnIndex){
                case 3: //sStockIDx
                    getDetail(fnRow, fnIndex, foValue);                    
                    break;
                case 4: //nUnitPrce
                case 5: //nSelPrce1
                case 6: //nSelPrce2
                case 7: //nSelPrce3
                case 8: //nDiscLev1
                case 9: //nDiscLev2
                case 10: //nDiscLev3
                    if (foValue instanceof Double){
                        p_oDetail.absolute(fnRow);
                        p_oDetail.updateObject(fnIndex, foValue);
                        p_oDetail.updateRow();
                    }
                    
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, fnIndex, getDetail(fnRow, fnIndex));
                    break;
            }

            saveToDisk(RecordStatus.ACTIVE, "");            
        } catch (SQLException | ParseException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
        }
    }
    
    @Override
    public void setDetail(int fnRow, String fsFieldNm, Object foValue) {
        try {
            setDetail(fnRow, MiscUtil.getColumnIndex(p_oDetail, fsFieldNm), foValue);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object getDetail(int fnRow, String fsFieldNm) {
        try {
            return getDetail(fnRow, MiscUtil.getColumnIndex(p_oDetail, fsFieldNm));
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return null;
        }
    }

    @Override
    public Object getDetail(int fnRow, int fnIndex) {
        if (fnIndex == 0) return null;
        
        try {
            p_oDetail.absolute(fnRow);
            return p_oDetail.getObject(fnIndex);
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return null;
        }
    }

    @Override
    public String getMessage() {
        return p_sMessagex;
    }

    @Override
    public int getEditMode() {
        return p_nEditMode;
    }

    @Override
    public int getItemCount() {
        try {
            p_oDetail.last();
            return p_oDetail.getRow();
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return -1;
        }
    }

    public void setWithUI(boolean fbValue){
        p_bWithUI = fbValue;
    }
    
    public void setTranStat(int fnValue){
        p_nTranStat = fnValue;
    }
    
    @Override
    public boolean addDetail() {
        try {
            if (getItemCount() > 0) {
                if ("".equals((String) getDetail(getItemCount(), "sStockIDx"))){
                    saveToDisk(RecordStatus.ACTIVE, "");
                    return true;
                }
            }
            
            p_oDetail.last();
            p_oDetail.moveToInsertRow();

            MiscUtil.initRowSet(p_oDetail);

            p_oDetail.insertRow();
            p_oDetail.moveToCurrentRow();
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return false;
        }
        
        saveToDisk(RecordStatus.ACTIVE, "");
        return true;
    }

    @Override
    public boolean delDetail(int fnRow) {
        try {
            p_oDetail.absolute(fnRow);
            p_oDetail.deleteRow();
            
            return addDetail();
        } catch (SQLException e) {
            setMessage(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean NewTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".NewTransaction()");
        
        p_sOrderNox = "";
        
        try {
            createMaster();
            createDetail();
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.ADDNEW;
        
        saveToDisk(RecordStatus.ACTIVE, "");
        loadTempTransactions();
        
        return true;
    }

    @Override
    public boolean NewTransaction(String fsOrderNox) {
        System.out.println(this.getClass().getSimpleName() + ".NewTransaction(String fsOrderNox)");
        
        if (fsOrderNox.isEmpty()) return NewTransaction();
        
        p_sOrderNox = fsOrderNox;
        
        ResultSet loTran = null;
        boolean lbLoad = false;
        
        try {
            loTran = CommonUtil.getTempOrder(p_oNautilus, SOURCE_CODE, fsOrderNox);
            
            if (loTran.next()){
                lbLoad = toDTO(loTran.getString("sPayloadx"));
                refreshDetail();
            }            
        } catch (SQLException | ParseException ex) {
            setMessage(ex.getMessage());
            lbLoad = false;
        } finally {
            MiscUtil.close(loTran);
        }
        
        p_nEditMode = EditMode.ADDNEW;
        
        loadTempTransactions();
        
        return lbLoad;
    }

    @Override
    public boolean SaveTransaction(boolean fbConfirmed) {
        System.out.println(this.getClass().getSimpleName() + ".SaveTransaction()");
        
        setMessage("");
        
        if (p_nEditMode != EditMode.ADDNEW){
            System.err.println("Transaction is not on update mode.");
            return false;
        }
        
        if (!fbConfirmed){
            saveToDisk(RecordStatus.ACTIVE, "");
            return true;
        }        
        
        if (!isEntryOK()) return false;
        
        try {
            String lsSQL = "";
            
            if (!p_bWithParent) p_oNautilus.beginTrans();
        
            if (p_nEditMode == EditMode.ADDNEW){
                Connection loConn = getConnection();

                p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, loConn, p_sBranchCd));
                p_oMaster.updateObject("sModified", (String) p_oNautilus.getUserInfo("sUserIDxx"));
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);
                
                //save detail
                int lnCtr = 1;
                p_oDetail.beforeFirst();
                while (p_oDetail.next()){
                    if (!"".equals((String) p_oDetail.getObject("sStockIDx"))){
                        p_oDetail.updateObject("sTransNox", p_oMaster.getObject("sTransNox"));
                        p_oDetail.updateObject("nEntryNox", lnCtr);
                    
                        lsSQL = MiscUtil.rowset2SQL(p_oDetail, DETAIL_TABLE, "xBarCodex;xDescript;xQtyOnHnd;xBrandCde;xModelCde;xColorCde;xUnitPrce;xSelPrce1");

                        if(p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                            if(!p_oNautilus.getMessage().isEmpty())
                                setMessage(p_oNautilus.getMessage());
                            else
                                setMessage("No record updated");

                            if (!p_bWithParent) p_oNautilus.rollbackTrans();
                            return false;
                        } 
                        lnCtr++;
                    }
                }
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "");
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
            
            saveToDisk(RecordStatus.INACTIVE, (String) p_oMaster.getObject("sTransNox"));

            if (!p_bWithParent) {
                if(!p_sMessagex.isEmpty()){
                    p_oNautilus.rollbackTrans();
                    return false;
                } else
                    p_oNautilus.commitTrans();
            }    
        } catch (SQLException ex) {
            if (!p_bWithParent) p_oNautilus.rollbackTrans();
            ex.printStackTrace();
            setMessage(ex.getMessage());
            return false;
        }
        
        loadTempTransactions();
        p_nEditMode = EditMode.UNKNOWN;
        
        return true;
    }

    @Override
    public boolean SearchTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".SearchTransaction()");        
        return true;
    }

    @Override
    public boolean OpenTransaction(String fsTransNox) {
        System.out.println(this.getClass().getSimpleName() + ".OpenTransaction()");
        setMessage("");       
        
        try {            
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //open master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "sTransNox = " + SQLUtil.toSQL(fsTransNox));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            
            //open detailo record
            lsSQL = MiscUtil.addCondition(getSQ_Detail(), "a.sTransNox = " + SQLUtil.toSQL(fsTransNox));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oDetail = factory.createCachedRowSet();
            p_oDetail.populate(loRS);
            MiscUtil.close(loRS);
            
            if (p_oMaster.size() == 1) {                            
                p_nEditMode  = EditMode.READY;
                return true;
            }
            
            setMessage("No transction loaded.");
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        p_nEditMode  = EditMode.UNKNOWN;
        return false;
    }

    @Override
    public boolean UpdateTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".UpdateTransaction()");
        return false;
    }

    @Override
    public boolean CloseTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".CloseTransaction()");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to update.");
                return false;
            }
            
            //re-open the transaction assuming the possibility of multiple PC loading of txs
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                                "sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (!loRS.next()){
                setMessage("Transaction went missing.");
                return false;
            }
            
            if ((TransactionStatus.STATE_CANCELLED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to veriy cancelled transactons.");
                return false;
            }        

            if ((TransactionStatus.STATE_POSTED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to veriy posted transactons.");
                return false;
            }

            if ((TransactionStatus.STATE_CLOSED).equals(loRS.getString("cTranStat"))){
                p_nEditMode  = EditMode.UNKNOWN;
                return true;
            }
            
            //check if user is allowed
            if (!p_oNautilus.isUserAuthorized(p_oApproval, 
                    UserLevel.MANAGER + UserLevel.OWNER + UserLevel.MASTER, 
                    AccessLevel.INVENTORY)){
                setMessage(System.getProperty("sMessagex"));
                System.setProperty("sMessagex", "");
                return false;
            }
            
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  sApproved = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx")) +
                        ", dApproved= " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                        ", cTranStat = " + TransactionStatus.STATE_CLOSED +
                    " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                setMessage(p_oNautilus.getMessage());
                return false;
            }

            p_nEditMode  = EditMode.UNKNOWN;
            return true; 
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        return false; 
    }

    @Override
    public boolean CancelTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".CancelTransaction()");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to update.");
                return false;
            }
            
            //re-open the transaction assuming the possibility of multiple PC loading of txs
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                                "sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (!loRS.next()){
                setMessage("Transaction went missing.");
                return false;
            }

            if ((TransactionStatus.STATE_POSTED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to cancel posted transactons.");
                return false;
            }
            
            if ((TransactionStatus.STATE_CANCELLED).equals(loRS.getString("cTranStat"))){
                p_nEditMode  = EditMode.UNKNOWN;
                return true;
            }
            
            //check if user is allowed
            if (!p_oNautilus.isUserAuthorized(p_oApproval, 
                    UserLevel.MANAGER + UserLevel.OWNER + UserLevel.MASTER,
                    AccessLevel.INVENTORY)){
                setMessage(System.getProperty("sMessagex"));
                System.setProperty("sMessagex", "");
                return false;
            }

            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  cTranStat = " + TransactionStatus.STATE_CANCELLED +
                    " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                setMessage(p_oNautilus.getMessage());
                return false;
            }

            p_nEditMode  = EditMode.UNKNOWN;
            return true; 
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        return false;
    }

    @Override
    public boolean DeleteTransaction(String fsTransNox) {
        System.out.println(this.getClass().getSimpleName() + ".DeleteTransaction()");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to update.");
                return false;
            }
            
            //re-open the transaction assuming the possibility of multiple PC loading of txs
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                                "sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (!loRS.next()){
                setMessage("Transaction went missing.");
                return false;
            }
            
            if ((TransactionStatus.STATE_POSTED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to delete already posted transactions.");
                return false;
            }
            
            //check if user is allowed
            if (!p_oNautilus.isUserAuthorized(p_oApproval, 
                    UserLevel.MANAGER + UserLevel.OWNER, 
                    AccessLevel.INVENTORY)){
                setMessage(System.getProperty("sMessagex"));
                System.setProperty("sMessagex", "");
                return false;
            }

            if (!p_bWithParent) p_oNautilus.beginTrans();
           
            lsSQL = "DELETE FROM " + MASTER_TABLE +
                    " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                setMessage(p_oNautilus.getMessage());
                return false;
            }

            lsSQL = "DELETE FROM " + DETAIL_TABLE +
                    " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));

            if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                setMessage(p_oNautilus.getMessage());
                return false;
            }

            if (!p_bWithParent) p_oNautilus.commitTrans();

            p_nEditMode  = EditMode.UNKNOWN;

            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        return false;
    }

    @Override
    public boolean PostTransaction() {
        System.out.println(this.getClass().getSimpleName() + ".PostTransaction()");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to update.");
                return false;
            }
            
            //re-open the transaction assuming the possibility of multiple PC loading of txs
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                                "sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (!loRS.next()){
                setMessage("Transaction went missing.");
                return false;
            }
            
            if ((TransactionStatus.STATE_CANCELLED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to approve cancelled transactons.");
                return false;
            }        

            if ((TransactionStatus.STATE_POSTED).equals(loRS.getString("cTranStat"))){                
                p_nEditMode  = EditMode.UNKNOWN;
                return true;
            }

            if (!(TransactionStatus.STATE_CLOSED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to approve unverified transactons.");
                return false;
            }
            
            //check if user is allowed
            if (!p_oNautilus.isUserAuthorized(p_oApproval, 
                    UserLevel.MANAGER + UserLevel.OWNER + UserLevel.MASTER, 
                    AccessLevel.INVENTORY)){
                setMessage(System.getProperty("sMessagex"));
                System.setProperty("sMessagex", "");
                return false;
            }
            
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        " cTranStat = " + TransactionStatus.STATE_POSTED +
                    " WHERE sTransNox = " + SQLUtil.toSQL((String) p_oMaster.getObject("sTransNox"));

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                if (!p_bWithParent) p_oNautilus.rollbackTrans();
                setMessage(p_oNautilus.getMessage());
                return false;
            }

            p_nEditMode  = EditMode.UNKNOWN;
            return true; 
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ex.getMessage());
        }
        
        return false; 
    }

    @Override
    public ArrayList<Temp_Transactions> TempTransactions() {
        return p_oTemp;
    }
    
    public JSONObject searchStocks(String fsKey, Object foValue, boolean fbExact){
        p_oSearchItem.setKey(fsKey);
        p_oSearchItem.setValue(foValue);
        p_oSearchItem.setExact(fbExact);
        
        return p_oSearchItem.Search();
    }
    
    public InvSearchF getSearchStocks(){
        return p_oSearchItem;
    }
    
    public JSONObject searchTransaction(String fsKey, Object foValue, boolean fbExact){
        p_oSearchTrans.setKey(fsKey);
        p_oSearchTrans.setValue(foValue);
        p_oSearchTrans.setExact(fbExact);
        
        p_oSearchTrans.addFilter("Status", p_nTranStat);
        
        return p_oSearchTrans.Search();
    }
    
    public InvSearchF getSearchTransaction(){
        return p_oSearchTrans;
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  sTransNox" +
                    ", sBranchCd" +
                    ", dTransact" +
                    ", dEffectve" +
                    ", sReferNox" +
                    ", sRemarksx" +
                    ", sApproved" +
                    ", dApproved" +
                    ", cTranStat" +
                    ", sModified" +
                " FROM " + MASTER_TABLE;
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.nEntryNox" +
                    ", a.sStockIDx" +
                    ", a.nUnitPrce" +
                    ", a.nSelPrce1" +
                    ", a.nSelPrce2" +
                    ", a.nSelPrce3" +
                    ", a.nDiscLev1" +
                    ", a.nDiscLev2" +
                    ", a.nDiscLev3" +
                    ", b.sBarCodex xBarCodex" +
                    ", b.sDescript xDescript" +
                    ", IFNULL(c.nQtyOnHnd, 0) xQtyOnHnd" +
                    ", b.sBrandCde xBrandCde" + 
                    ", b.sModelCde xModelCde" +
                    ", b.sColorCde xColorCde" +
                    ", b.nUnitPrce xUnitPrce" +
                    ", b.nSelPrce1 xSelPrce1" +
                " FROM " + DETAIL_TABLE + " a" +
                    " LEFT JOIN Inventory b" +
                        " LEFT JOIN Inv_Master c" +
                            " ON b.sStockIDx = c.sStockIDx" +
                                " AND c.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                    " ON a.sStockIDx = b.sStockIDx";
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
    }
    
    private void saveToDisk(String fsRecdStat, String fsTransNox){
        if (p_bSaveToDisk && p_nEditMode == EditMode.ADDNEW){
            String lsPayloadx = toJSONString();
            
            if (p_sOrderNox.isEmpty()){
                p_sOrderNox = CommonUtil.getNextReference(p_oNautilus.getConnection().getConnection(), "xxxTempTransactions", "sOrderNox", "sSourceCd = " + SQLUtil.toSQL(SOURCE_CODE));
                CommonUtil.saveTempOrder(p_oNautilus, SOURCE_CODE, p_sOrderNox, lsPayloadx);
            } else
                CommonUtil.saveTempOrder(p_oNautilus, SOURCE_CODE, p_sOrderNox, lsPayloadx, fsRecdStat, fsTransNox);
        }
    }
    
    private void loadTempTransactions(){
        String lsSQL = "SELECT * FROM xxxTempTransactions" +
                        " WHERE cRecdStat = '1'" +
                            " AND sSourceCd = " + SQLUtil.toSQL(SOURCE_CODE);
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        Temp_Transactions loTemp;
        p_oTemp = new ArrayList<>();
        
        try {
            while(loRS.next()){
                loTemp = new Temp_Transactions();
                loTemp.setSourceCode(loRS.getString("sSourceCd"));
                loTemp.setOrderNo(loRS.getString("sOrderNox"));
                loTemp.setDateCreated(SQLUtil.toDate(loRS.getString("dCreatedx"), SQLUtil.FORMAT_TIMESTAMP));
                loTemp.setPayload(loRS.getString("sPayloadx"));
                p_oTemp.add(loTemp);
            }
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
        } finally {
            MiscUtil.close(loRS);
        }
    }
    
    private String toJSONString(){
        JSONParser loParser = new JSONParser();
        JSONArray laMaster = new JSONArray();
        JSONArray laDetail = new JSONArray();
        JSONObject loMaster;
        JSONObject loJSON;

        try {
            String lsValue = MiscUtil.RS2JSONi(p_oMaster).toJSONString();
            laMaster = (JSONArray) loParser.parse(lsValue);
            loMaster = (JSONObject) laMaster.get(0);
            
            lsValue = MiscUtil.RS2JSONi(p_oDetail).toJSONString();
            laDetail = (JSONArray) loParser.parse(lsValue);
 
            loJSON = new JSONObject();
            loJSON.put("master", loMaster);
            loJSON.put("detail", laDetail);
            
            return loJSON.toJSONString();
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        return "";
    }
    
    private boolean toDTO(String fsPayloadx){
        boolean lbLoad = false;
        
        if (fsPayloadx.isEmpty()) return lbLoad;
        
        JSONParser loParser = new JSONParser();
        
        JSONObject loJSON;
        JSONObject loMaster;
        JSONArray laDetail;
        
        try {
            createMaster();
            createDetail();

            loJSON = (JSONObject) loParser.parse(fsPayloadx);
            loMaster = (JSONObject) loJSON.get("master");
            laDetail = (JSONArray) loJSON.get("detail");
            
            int lnCtr;
            int lnRow;
            
            int lnKey;
            String lsKey;
            String lsIndex;
            Iterator iterator;

            lnRow = 1;
            for(iterator = loMaster.keySet().iterator(); iterator.hasNext();) {
                lsIndex = (String) iterator.next(); //string value of int
                lnKey = Integer.valueOf(lsIndex); //string to in
                lsKey = p_oMaster.getMetaData().getColumnLabel(lnKey); //int to metadata
                p_oMaster.absolute(lnRow);
                if (loMaster.get(lsIndex) != null){
                    switch(lsKey){
                        case "dTransact":
                            p_oMaster.updateObject(lnKey, SQLUtil.toDate((String) loMaster.get(lsIndex), SQLUtil.FORMAT_TIMESTAMP));
                            break;
                         case "dEffectve":
                            p_oMaster.updateObject(lnKey, SQLUtil.toDate((String) loMaster.get(lsIndex), SQLUtil.FORMAT_SHORT_DATE));
                            break;
                        default:
                            p_oMaster.updateObject(lnKey, loMaster.get(lsIndex));
                    }

                    p_oMaster.updateRow();
                }
            }
            
            lnRow = 1;
            for(lnCtr = 0; lnCtr <= laDetail.size()-1; lnCtr++){
                JSONObject loDetail = (JSONObject) laDetail.get(lnCtr);

                for(iterator = loDetail.keySet().iterator(); iterator.hasNext();) {
                    lsIndex = (String) iterator.next(); //string value of int
                    lnKey = Integer.valueOf(lsIndex); //string to int
                    p_oDetail.absolute(lnRow);
                    p_oDetail.updateObject(lnKey, loDetail.get(lsIndex));
                    p_oDetail.updateRow();
                }
                lnRow++;
                addDetail();
            }
        } catch (SQLException | ParseException ex) {
            setMessage(ex.getMessage());
            ex.printStackTrace();
            return false;
        }
        
        return true;
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
    
    private boolean isEntryOK(){
        try {
            //delete the last detail record if stock id
            int lnCtr = getItemCount();

            p_oDetail.absolute(lnCtr);
            if ("".equals((String) p_oDetail.getObject("sStockIDx"))){
                p_oDetail.deleteRow();
            }

            lnCtr = getItemCount();
            //validate if there is a detail record
            if (lnCtr <= 0) {
                setMessage("There is no item in this transaction.");
                addDetail(); //add detail to prevent error on the next attempt of saving
                return false;
            }

            //assign values to master record
            p_oMaster.first();
            p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getBranchConfig("sBranchCd")); 

            String lsSQL = "SELECT dCreatedx FROM xxxTempTransactions" +
                            " WHERE sSourceCd = " + SQLUtil.toSQL(SOURCE_CODE) +
                                " AND sOrderNox = " + SQLUtil.toSQL(p_sOrderNox);
            
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            while (loRS.next()){
                p_oMaster.updateObject("dTransact", loRS.getString("dCreatedx"));
            }
            
            MiscUtil.close(loRS);
            p_oMaster.updateRow();

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return false;
        }
    }
    
    private void createMaster() throws SQLException{
        RowSetMetaData meta = new RowSetMetaDataImpl();

        meta.setColumnCount(10);

        meta.setColumnName(1, "sTransNox");
        meta.setColumnLabel(1, "sTransNox");
        meta.setColumnType(1, Types.VARCHAR);
        meta.setColumnDisplaySize(1, 12);
        
        meta.setColumnName(2, "sBranchCd");
        meta.setColumnLabel(2, "sBranchCd");
        meta.setColumnType(2, Types.VARCHAR);
        meta.setColumnDisplaySize(2, 4);

        meta.setColumnName(3, "dTransact");
        meta.setColumnLabel(3, "dTransact");
        meta.setColumnType(3, Types.TIMESTAMP);

        meta.setColumnName(4, "dEffectve");
        meta.setColumnLabel(4, "dEffectve");
        meta.setColumnType(4, Types.DATE);
        
        meta.setColumnName(5, "sReferNox");
        meta.setColumnLabel(5, "sReferNox");
        meta.setColumnType(5, Types.VARCHAR);
        meta.setColumnDisplaySize(5, 12);
        
        meta.setColumnName(6, "sRemarksx");
        meta.setColumnLabel(6, "sRemarksx");
        meta.setColumnType(6, Types.VARCHAR);
        meta.setColumnDisplaySize(6, 64);
        
        meta.setColumnName(7, "sApproved");
        meta.setColumnLabel(7, "sApproved");
        meta.setColumnType(7, Types.VARCHAR);
        meta.setColumnDisplaySize(7, 12);
        
        meta.setColumnName(8, "dApproved");
        meta.setColumnLabel(8, "dApproved");
        meta.setColumnType(8, Types.TIMESTAMP);
                
        meta.setColumnName(9, "cTranStat");
        meta.setColumnLabel(9, "cTranStat");
        meta.setColumnType(9, Types.CHAR);
        meta.setColumnDisplaySize(9, 1);
        
        meta.setColumnName(10, "sModified");
        meta.setColumnLabel(10, "sModified");
        meta.setColumnType(10, Types.VARCHAR);
        meta.setColumnDisplaySize(10, 12);

        p_oMaster = new CachedRowSetImpl();
        p_oMaster.setMetaData(meta);
        
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);       
        
        p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, getConnection(), p_sBranchCd));
        p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());
        p_oMaster.updateObject("dEffectve", p_oNautilus.getServerDate());
        p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    private void createDetail() throws SQLException{
        RowSetMetaData meta = new RowSetMetaDataImpl();

        meta.setColumnCount(18);

        meta.setColumnName(1, "sTransNox");
        meta.setColumnLabel(1, "sTransNox");
        meta.setColumnType(1, Types.VARCHAR);
        meta.setColumnDisplaySize(1, 12);
        
        meta.setColumnName(2, "nEntryNox");
        meta.setColumnLabel(2, "nEntryNox");
        meta.setColumnType(2, Types.INTEGER);

        meta.setColumnName(3, "sStockIDx");
        meta.setColumnLabel(3, "sStockIDx");
        meta.setColumnType(3, Types.VARCHAR);
        meta.setColumnDisplaySize(3, 12);
        
        meta.setColumnName(4, "nUnitPrce");
        meta.setColumnLabel(4, "nUnitPrce");
        meta.setColumnType(4, Types.DOUBLE);
        
        meta.setColumnName(5, "nSelPrce1");
        meta.setColumnLabel(5, "nSelPrce1");
        meta.setColumnType(5, Types.DOUBLE);
        
        meta.setColumnName(6, "nSelPrce2");
        meta.setColumnLabel(6, "nSelPrce2");
        meta.setColumnType(6, Types.DOUBLE);
        
        meta.setColumnName(7, "nSelPrce3");
        meta.setColumnLabel(7, "nSelPrce3");
        meta.setColumnType(7, Types.DOUBLE);
        
        meta.setColumnName(8, "nDiscLev1");
        meta.setColumnLabel(8, "nDiscLev1");
        meta.setColumnType(8, Types.DOUBLE);
        
        meta.setColumnName(9, "nDiscLev2");
        meta.setColumnLabel(9, "nDiscLev2");
        meta.setColumnType(9, Types.DOUBLE);
        
        meta.setColumnName(10, "nDiscLev3");
        meta.setColumnLabel(10, "nDiscLev3");
        meta.setColumnType(10, Types.DOUBLE);
        
        meta.setColumnName(11, "xBarCodex");
        meta.setColumnLabel(11, "xBarCodex");
        meta.setColumnType(11, Types.VARCHAR);
        
        meta.setColumnName(12, "xDescript");
        meta.setColumnLabel(12, "xDescript");
        meta.setColumnType(12, Types.VARCHAR);
        
        meta.setColumnName(13, "xQtyOnHnd");
        meta.setColumnLabel(13, "xQtyOnHnd");
        meta.setColumnType(13, Types.INTEGER);
        
        meta.setColumnName(14, "xBrandCde");
        meta.setColumnLabel(14, "xBrandCde");
        meta.setColumnType(14, Types.VARCHAR);
        
        meta.setColumnName(15, "xModelCde");
        meta.setColumnLabel(15, "xModelCde");
        meta.setColumnType(15, Types.VARCHAR);
        
        meta.setColumnName(16, "xColorCde");
        meta.setColumnLabel(16, "xColorCde");
        meta.setColumnType(16, Types.VARCHAR);
        
        meta.setColumnName(17, "xUnitPrce");
        meta.setColumnLabel(17, "xUnitPrce");
        meta.setColumnType(17, Types.DOUBLE);
        
        meta.setColumnName(18, "xSelPrce1");
        meta.setColumnLabel(18, "xSelPrce1");
        meta.setColumnType(18, Types.DOUBLE);

        p_oDetail = new CachedRowSetImpl();
        p_oDetail.setMetaData(meta);
        
        p_oDetail.last();
        p_oDetail.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oDetail);       
        
        p_oDetail.insertRow();
        p_oDetail.moveToCurrentRow();
    }
    
    public void displayMasFields() throws SQLException{
        if (p_nEditMode != EditMode.ADDNEW) return;
        
        int lnRow = p_oMaster.getMetaData().getColumnCount();
        
        System.out.println("----------------------------------------");
        System.out.println("MASTER TABLE INFO");
        System.out.println("----------------------------------------");
        System.out.println("Total number of columns: " + lnRow);
        System.out.println("----------------------------------------");
        
        for (int lnCtr = 1; lnCtr <= lnRow; lnCtr++){
            System.out.println("Column index: " + (lnCtr) + " --> Label: " + p_oMaster.getMetaData().getColumnLabel(lnCtr));
            if (p_oMaster.getMetaData().getColumnType(lnCtr) == Types.CHAR ||
                p_oMaster.getMetaData().getColumnType(lnCtr) == Types.VARCHAR){
                
                System.out.println("Column index: " + (lnCtr) + " --> Size: " + p_oMaster.getMetaData().getColumnDisplaySize(lnCtr));
            }
        }
        
        System.out.println("----------------------------------------");
        System.out.println("END: MASTER TABLE INFO");
        System.out.println("----------------------------------------");
    }
    
    public void displayDetFields() throws SQLException{
        if (p_nEditMode != EditMode.ADDNEW) return;
        
        int lnRow = p_oDetail.getMetaData().getColumnCount();
        
        System.out.println("----------------------------------------");
        System.out.println("DETAIL TABLE INFO");
        System.out.println("----------------------------------------");
        System.out.println("Total number of columns: " + lnRow);
        System.out.println("----------------------------------------");
        
        for (int lnCtr = 1; lnCtr <= lnRow; lnCtr++){
            System.out.println("Column index: " + (lnCtr) + " --> Label: " + p_oDetail.getMetaData().getColumnLabel(lnCtr));
            if (p_oDetail.getMetaData().getColumnType(lnCtr) == Types.CHAR ||
                p_oDetail.getMetaData().getColumnType(lnCtr) == Types.VARCHAR){
                
                System.out.println("Column index: " + (lnCtr) + " --> Size: " + p_oDetail.getMetaData().getColumnDisplaySize(lnCtr));
            }
        }
        
        System.out.println("----------------------------------------");
        System.out.println("END: DETAIL TABLE INFO");
        System.out.println("----------------------------------------");
    }
    
    public boolean DeleteTempTransaction(Temp_Transactions foValue) {
        boolean lbSuccess =  CommonUtil.saveTempOrder(p_oNautilus, foValue.getSourceCode(), foValue.getOrderNo(), foValue.getPayload(), "0");
        loadTempTransactions();
        
        p_nEditMode = EditMode.UNKNOWN;
        return lbSuccess;
    }
    
    private void getDetail(int fnRow, int fnIndex, Object foValue) throws SQLException, ParseException{       
        JSONObject loJSON;
        JSONParser loParser = new JSONParser();
        
        switch(fnIndex){
            case 3: //sStockIDx
                loJSON = searchStocks("a.sStockIDx", foValue, true);
                
                if ("success".equals((String) loJSON.get("result"))){
                    loJSON = (JSONObject) ((JSONArray) loParser.parse((String) loJSON.get("payload"))).get(0);
                    
                    //check if the stock id was already exists
                    boolean lbExist = false;
                    
                    for (int lnCtr = 1; lnCtr <= getItemCount(); lnCtr ++){
                        p_oDetail.absolute(lnCtr);
                        if (((String) p_oDetail.getObject("sStockIDx")).equals((String) loJSON.get("sStockIDx"))){
                            fnRow = lnCtr;
                            lbExist = true;
                            break;
                        }
                    }
                    
                    p_oDetail.absolute(fnRow);
                    p_oDetail.updateObject("sStockIDx", (String) loJSON.get("sStockIDx"));
                    p_oDetail.updateObject("nUnitPrce", 0.00);
                    p_oDetail.updateObject("nSelPrce1", 0.00);
                    p_oDetail.updateObject("nSelPrce2", 0.00);
                    p_oDetail.updateObject("nSelPrce3", 0.00);
                    p_oDetail.updateObject("nDiscLev1", 0.00); //supervisor
                    p_oDetail.updateObject("nDiscLev2", 0.00); //manager
                    p_oDetail.updateObject("nDiscLev3", 0.00); //wholesale
                    
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xBarCodex"), (String) loJSON.get("sBarCodex"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xDescript"), (String) loJSON.get("sDescript"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xQtyOnHnd"), Integer.parseInt(String.valueOf(loJSON.get("nQtyOnHnd"))));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xBrandCde"), (String) loJSON.get("sBrandCde"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xModelCde"), (String) loJSON.get("sModelCde"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xColorCde"), (String) loJSON.get("sColorCde")); 
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xUnitPrce"), Double.valueOf(String.valueOf(loJSON.get("nUnitPrce")))); 
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xSelPrce1"), Double.valueOf(String.valueOf(loJSON.get("nSelPrce1")))); 
                    
                    p_oDetail.updateRow();    
                    if (!lbExist) addDetail();
                    
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "xBarCodex"), getDetail(fnRow, "xBarCodex"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "xDescript"), getDetail(fnRow, "xDescript"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "nCredtQty"), getDetail(fnRow, "nCredtQty"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "nDebitQty"), getDetail(fnRow, "nDebitQty"));
                }
        }
    }
    
    private void refreshDetail() throws ParseException, SQLException{
        JSONObject loJSON;
        JSONParser loParser = new JSONParser();
        
        for (int lnCtr = 1; lnCtr <= getItemCount(); lnCtr ++){
            if ("".equals((String) getDetail(lnCtr, "sStockIDx"))) break;
            
            loJSON = searchStocks("a.sStockIDx", (String) getDetail(lnCtr, "sStockIDx"), true);
            
            if ("success".equals((String) loJSON.get("result"))){
                loJSON = (JSONObject) ((JSONArray) loParser.parse((String) loJSON.get("payload"))).get(0);
                
                p_oDetail.absolute(lnCtr);

                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xBarCodex"), (String) loJSON.get("sBarCodex"));
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xDescript"), (String) loJSON.get("sDescript"));
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xQtyOnHnd"), Integer.parseInt(String.valueOf(loJSON.get("nQtyOnHnd"))));
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xBrandCde"), (String) loJSON.get("sBrandCde"));
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xModelCde"), (String) loJSON.get("sModelCde"));
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xColorCde"), (String) loJSON.get("sColorCde")); 
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xUnitPrce"), Double.valueOf(String.valueOf(loJSON.get("nUnitPrce")))); 
                p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xSelPrce1"), Double.valueOf(String.valueOf(loJSON.get("nSelPrce1")))); 

                p_oDetail.updateRow();   
            }
        }
    }
}
