package org.xersys.inventory.base;

import com.mysql.jdbc.Connection;
import com.sun.rowset.CachedRowSetImpl;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
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
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.RecordStatus;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.iface.XMasDetTrans;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.CommonUtil;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.inventory.search.InvSearchF;
import org.xersys.lib.pojo.Temp_Transactions;
import org.xersys.parameters.search.ParamSearchF;

public class InvTransfer implements XMasDetTrans{
    private final String MASTER_TABLE = "Inv_Transfer_Master";
    private final String DETAIL_TABLE = "Inv_Transfer_Detail";
    private final String SOURCE_CODE = "InvT";
    
    private final XNautilus p_oNautilus;
    private final boolean p_bWithParent;
    private final String p_sBranchCd;
    
    private LMasDetTrans p_oListener;
    private boolean p_bSaveToDisk;
    private boolean p_bSaveOrder;
    private boolean p_bWithUI;
    
    private String p_sOrderNox;
    private String p_sMessagex;
    
    private int p_nEditMode;
    private int p_nTranStat;
    
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oDetail;
    
    private final InvSearchF p_oSParts;
    private final InvSearchF p_oSTrans;
    private final InvSearchF p_oSRequest;
    private final ParamSearchF p_oSDestinat;
    
    private ArrayList<Temp_Transactions> p_oTemp;
    
    public InvTransfer(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_bWithUI = true;
        p_bSaveOrder = false;
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        p_nEditMode = EditMode.UNKNOWN;
        
        p_oSParts = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchBranchStocks);
        p_oSTrans = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvTransfer);
        p_oSRequest = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvRequest);
        p_oSDestinat = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchBranch);
        
        loadTempTransactions();
    }
    
    public InvTransfer(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent, int fnTranStat){
        p_bWithUI = true;
        p_bSaveOrder = false;
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        p_nTranStat = fnTranStat;
        p_nEditMode = EditMode.UNKNOWN;
        
        p_oSParts = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchBranchStocks);
        p_oSTrans = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvTransfer);
        p_oSRequest = new InvSearchF(p_oNautilus, InvSearchF.SearchType.searchSPInvRequest);
        p_oSDestinat = new ParamSearchF(p_oNautilus, ParamSearchF.SearchType.searchBranch);
        
        loadTempTransactions();
    }
    
    public void setWithUI(boolean fbValue){
        p_bWithUI = fbValue;
    }
    
    public void setSaveOrder(boolean fbValue){
        p_bSaveOrder = fbValue;
    }
    
    @Override
    public void setListener(LMasDetTrans foValue) {
        p_oListener = foValue;
    }

    @Override
    public void setSaveToDisk(boolean fbValue) {
        p_bSaveToDisk = fbValue;
    }

    public void setTranStat(int fnValue){
        p_nTranStat = fnValue;
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
    public void setMaster(int fnIndex, Object foValue) {
        try {
            switch (fnIndex){
                case 4://sDestinat
                    getMaster(fnIndex, foValue);
                    break;
                case 5://sRemarksx
                    p_oMaster.first();
                    p_oMaster.updateString(fnIndex, (String) foValue);
                    p_oMaster.updateRow();

                    if (p_oListener != null) p_oListener.MasterRetreive(fnIndex, getMaster(fnIndex));
                    break;
            }
            
            saveToDisk(RecordStatus.ACTIVE, "");
        } catch (ParseException | SQLException e) {
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
    public void setDetail(int fnRow, String fsFieldNm, Object foValue) {
        try {
            setDetail(fnRow, MiscUtil.getColumnIndex(p_oDetail, fsFieldNm), foValue);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void setDetail(int fnRow, int fnIndex, Object foValue) {
        try {
            switch (fnIndex){
                case 3://sStockIDx
                case 4: //sOrigIDxx
                case 5: //sOrderNox
                    getDetail(fnRow, fnIndex, foValue);
                    break;
                case 6://nQuantity
                     p_oDetail.absolute(fnRow);
                    try {
                        int x = Integer.parseInt(String.valueOf(foValue));
                        
                        p_oDetail.updateInt(fnIndex, x);
                    } catch (NumberFormatException e) {
                        p_oDetail.updateInt(fnIndex, 0);
                    }
                    
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, fnIndex, getDetail(fnRow, fnIndex));
                    break;
            }
            
            saveToDisk(RecordStatus.ACTIVE, "");
        } catch (SQLException | ParseException e) {
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
            }            
        } catch (SQLException ex) {
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
                p_oMaster.updateObject("sEntryByx", (String) p_oNautilus.getUserInfo("sUserIDxx"));
                p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
                p_oMaster.updateRow();
                
                if (!p_bWithParent) MiscUtil.close(loConn);
                
                //save detail
                int lnCtr = 1;
                p_oDetail.beforeFirst();
                while (p_oDetail.next()){
                    if (!"".equals((String) p_oDetail.getObject("sStockIDx"))){
                        p_oDetail.updateObject("sTransNox", p_oMaster.getObject("sTransNox"));
                        p_oDetail.updateObject("nEntryNox", lnCtr);
                    
                        lsSQL = MiscUtil.rowset2SQL(p_oDetail, DETAIL_TABLE, "xBarCodex;xDescript;xBarCodeX;xClientNm;xQtyOnHnd");

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
                
                lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "xBranchNm;xDestinat;xTruckNme");
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
            if (p_oMaster != null){
                p_oMaster.first();

                if (p_oMaster.getString("sTransNox").equals(fsTransNox)){
                    p_nEditMode  = EditMode.READY;
                    return true;
                }
            }
            
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //open master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sTransNox = " + SQLUtil.toSQL(fsTransNox));
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
                                "a.sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
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
                setMessage("Unable to approve posted transactons.");
                return false;
            }

            if ((TransactionStatus.STATE_CLOSED).equals(loRS.getString("cTranStat"))){
                p_nEditMode  = EditMode.UNKNOWN;
                return true;
            }
            
            if (!saveInvTrans()) return false;
            
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  cTranStat = " + TransactionStatus.STATE_CLOSED +
                        ", dModified= " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
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
        System.out.println(this.getClass().getSimpleName() + ".CloseTransaction()");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to update.");
                return false;
            }
            
            //re-open the transaction assuming the possibility of multiple PC loading of txs
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), 
                                "a.sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
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
                return false;
            }
            
            if ((TransactionStatus.STATE_CLOSED).equals(loRS.getString("cTranStat")))
                if (!delInvTrans()) return false;
            
            
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  cTranStat = " + TransactionStatus.STATE_CANCELLED +
                        ", dModified= " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
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
                                "a.sTransNox = " + SQLUtil.toSQL(getMaster("sTransNox")));
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (!loRS.next()){
                setMessage("Transaction went missing.");
                return false;
            }
            
            if ((TransactionStatus.STATE_POSTED).equals(loRS.getString("cTranStat"))){
                setMessage("Unable to delete already posted transactions.");
                return false;
            }

            if (!p_bWithParent) p_oNautilus.beginTrans();
            
            if ((TransactionStatus.STATE_CLOSED).equals(loRS.getString("cTranStat"))){
                if (!delInvTrans()) return false;
            }
           
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
        return false;
    }

    @Override
    public ArrayList<Temp_Transactions> TempTransactions() {    
        return p_oTemp;
    }
    
    public boolean DeleteTempTransaction(Temp_Transactions foValue) {
        boolean lbSuccess =  CommonUtil.saveTempOrder(p_oNautilus, foValue.getSourceCode(), foValue.getOrderNo(), foValue.getPayload(), "0");
        loadTempTransactions();
        
        p_nEditMode = EditMode.UNKNOWN;
        return lbSuccess;
    }
    
    private String getSQ_Master(){
        return " SELECT" +
                    "  a.sTransNox" +
                    ", a.sBranchCd" +
                    ", a.dTransact" +
                    ", a.sDestinat" +
                    ", a.sRemarksx" +
                    ", a.sTruckIDx" +
                    ", a.nFreightx" +
                    ", a.sReceived" +
                    ", a.dReceived" +
                    ", a.sApproved" +
                    ", a.sApprvCde" +
                    ", a.nTranTotl" +
                    ", a.nDiscount" +
                    ", a.cDeliverd" +
                    ", a.sDeliverd" +
                    ", a.nEntryNox" +
                    ", a.sOrderNox" +
                    ", a.sEntryByx" +
                    ", a.cTranStat" +
                    ", a.dCreatedx" +
                    ", a.dModified" +
                    ", b.sCompnyNm xBranchNm" +
                    ", c.sCompnyNm xDestinat" +
                    ", '' xTruckNme" +
                " FROM Inv_Transfer_Master a" +
                    " LEFT JOIN xxxSysClient b ON a.sBranchCd = b.sBranchCd" +
                    " LEFT JOIN xxxSysClient c ON a.sDestinat = c.sBranchCd";
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.nEntryNox" +
                    ", a.sStockIDx" +
                    ", a.sOrigIDxx" +
                    ", a.sOrderNox" +
                    ", a.nQuantity" +
                    ", a.nInvCostx" +
                    ", a.sRecvIDxx" +
                    ", a.sNotesxxx" +
                    ", b.sBarCodex xBarCodex" +
                    ", b.sDescript xDescript" +
                    ", c.sBarCodex xBarCodeX" +
                    ", d.sClientNm xClientNm" +
                    ", e.nQtyOnHnd xQtyOnHnd" +
                " FROM Inv_Transfer_Detail a" +
                    " LEFT JOIN Inventory b ON a.sStockIDx = b.sStockIDx" +
                    " LEFT JOIN Inventory c ON a.sOrigIDxx = c.sStockIDx" +
                    " LEFT JOIN Client_Master d ON a.sRecvIDxx = d.sClientID" +
                    " LEFT JOIN Inv_Master e ON a.sStockIDx = d.sStockIDx" +
                        " AND e.sBranchCd = " + SQLUtil.toSQL((String) p_oNautilus.getBranchConfig("sBranchC"));
    }
    
    private void createMaster() throws SQLException{
        RowSetMetaData meta = new RowSetMetaDataImpl();

        meta.setColumnCount(24);

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

        meta.setColumnName(4, "sDestinat");
        meta.setColumnLabel(4, "sDestinat");
        meta.setColumnType(4, Types.VARCHAR);
        meta.setColumnDisplaySize(4, 4);
        
        meta.setColumnName(5, "sRemarksx");
        meta.setColumnLabel(5, "sRemarksx");
        meta.setColumnType(5, Types.VARCHAR);
        meta.setColumnDisplaySize(5, 128);
        
        meta.setColumnName(6, "sTruckIDx");
        meta.setColumnLabel(6, "sTruckIDx");
        meta.setColumnType(6, Types.VARCHAR);
        meta.setColumnDisplaySize(6, 12);
        
        meta.setColumnName(7, "nFreightx");
        meta.setColumnLabel(7, "nFreightx");
        meta.setColumnType(7, Types.DOUBLE);
        
        meta.setColumnName(8, "sReceived");
        meta.setColumnLabel(8, "sReceived");
        meta.setColumnType(8, Types.VARCHAR);
        meta.setColumnDisplaySize(8, 12);
        
        meta.setColumnName(9, "dReceived");
        meta.setColumnLabel(9, "dReceived");
        meta.setColumnType(9, Types.TIMESTAMP);
        
        meta.setColumnName(10, "sApproved");
        meta.setColumnLabel(10, "sApproved");
        meta.setColumnType(10, Types.VARCHAR);
        meta.setColumnDisplaySize(10, 12);
        
        meta.setColumnName(11, "sApprvCde");
        meta.setColumnLabel(11, "sApprvCde");
        meta.setColumnType(11, Types.VARCHAR);
        meta.setColumnDisplaySize(11, 12);
        
        meta.setColumnName(12, "nTranTotl");
        meta.setColumnLabel(12, "nTranTotl");
        meta.setColumnType(12, Types.DOUBLE);
        
        meta.setColumnName(13, "nDiscount");
        meta.setColumnLabel(13, "nDiscount");
        meta.setColumnType(13, Types.DOUBLE);
        
        meta.setColumnName(14, "cDeliverd");
        meta.setColumnLabel(14, "cDeliverd");
        meta.setColumnType(14, Types.CHAR);
        meta.setColumnDisplaySize(14, 1);
        
        meta.setColumnName(15, "sDeliverd");
        meta.setColumnLabel(15, "sDeliverd");
        meta.setColumnType(15, Types.VARCHAR);
        meta.setColumnDisplaySize(15, 12);
        
        meta.setColumnName(16, "nEntryNox");
        meta.setColumnLabel(16, "nEntryNox");
        meta.setColumnType(16, Types.INTEGER);
        
        meta.setColumnName(17, "sOrderNox");
        meta.setColumnLabel(17, "sOrderNox");
        meta.setColumnType(17, Types.VARCHAR);
        meta.setColumnDisplaySize(17, 12);
        
        meta.setColumnName(18, "sEntryByx");
        meta.setColumnLabel(18, "sEntryByx");
        meta.setColumnType(18, Types.VARCHAR);
        meta.setColumnDisplaySize(18, 12);
        
        meta.setColumnName(19, "cTranStat");
        meta.setColumnLabel(19, "cTranStat");
        meta.setColumnType(19, Types.CHAR);
        meta.setColumnDisplaySize(19, 1);
        
        meta.setColumnName(20, "dCreatedx");
        meta.setColumnLabel(20, "dCreatedx");
        meta.setColumnType(20, Types.TIMESTAMP);
        
        meta.setColumnName(21, "dModified");
        meta.setColumnLabel(21, "dModified");
        meta.setColumnType(21, Types.TIMESTAMP);
        
        meta.setColumnName(22, "xBranchNm");
        meta.setColumnLabel(22, "xBranchNm");
        meta.setColumnType(22, Types.VARCHAR);
        
        meta.setColumnName(23, "xDestinat");
        meta.setColumnLabel(23, "xDestinat");
        meta.setColumnType(23, Types.VARCHAR);
        
        meta.setColumnName(24, "xTruckNme");
        meta.setColumnLabel(24, "xTruckNme");
        meta.setColumnType(24, Types.VARCHAR);
        
        p_oMaster = new CachedRowSetImpl();
        p_oMaster.setMetaData(meta);
        
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);       
        
        p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, getConnection(), p_sBranchCd));
        p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());
        p_oMaster.updateObject("cDeliverd", "0");
        p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    private void createDetail() throws SQLException{
        RowSetMetaData meta = new RowSetMetaDataImpl();

        meta.setColumnCount(14);

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
        
        meta.setColumnName(4, "sOrigIDxx");
        meta.setColumnLabel(4, "sOrigIDxx");
        meta.setColumnType(4, Types.VARCHAR);
        meta.setColumnDisplaySize(4, 12);
        
        meta.setColumnName(5, "sOrderNox");
        meta.setColumnLabel(5, "sOrderNox");
        meta.setColumnType(5, Types.VARCHAR);
        meta.setColumnDisplaySize(5, 12);
        
        meta.setColumnName(6, "nQuantity");
        meta.setColumnLabel(6, "nQuantity");
        meta.setColumnType(6, Types.INTEGER);
        
        meta.setColumnName(7, "nInvCostx");
        meta.setColumnLabel(7, "nInvCostx");
        meta.setColumnType(7, Types.DOUBLE);
        
        meta.setColumnName(8, "sRecvIDxx");
        meta.setColumnLabel(8, "sRecvIDxx");
        meta.setColumnType(8, Types.VARCHAR);
        meta.setColumnDisplaySize(8, 12);
        
        meta.setColumnName(9, "sNotesxxx");
        meta.setColumnLabel(9, "sNotesxxx");
        meta.setColumnType(9, Types.VARCHAR);
        meta.setColumnDisplaySize(9, 64);
        
        meta.setColumnName(10, "xBarCodex");
        meta.setColumnLabel(10, "xBarCodex");
        meta.setColumnType(10, Types.VARCHAR);
        
        meta.setColumnName(11, "xDescript");
        meta.setColumnLabel(11, "xDescript");
        meta.setColumnType(11, Types.VARCHAR);
        
        meta.setColumnName(12, "xBarCodeX");
        meta.setColumnLabel(12, "xBarCodeX");
        meta.setColumnType(12, Types.VARCHAR);
        
        meta.setColumnName(13, "xClientNm");
        meta.setColumnLabel(13, "xClientNm");
        meta.setColumnType(13, Types.VARCHAR);
        
        meta.setColumnName(14, "xQtyOnHnd");
        meta.setColumnLabel(14, "xQtyOnHnd");
        meta.setColumnType(14, Types.INTEGER);
        
        p_oDetail = new CachedRowSetImpl();
        p_oDetail.setMetaData(meta);
        
        p_oDetail.last();
        p_oDetail.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oDetail);       
        
        p_oDetail.insertRow();
        p_oDetail.moveToCurrentRow();
    }
    
    public void displayMasFields() throws SQLException{
        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) return;
        
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
        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) return;
        
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
                        case "dCreatedx":
                        case "dReceived":
                            p_oMaster.updateObject(lnKey, SQLUtil.toDate((String) loMaster.get(lsIndex), SQLUtil.FORMAT_TIMESTAMP));
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
    
    private boolean isEntryOK(){
        try {
            //delete the last detail record if stock id
            int lnCtr = getItemCount();

            p_oDetail.absolute(lnCtr);
            if ("".equals((String) p_oDetail.getObject("sStockIDx"))){
                p_oDetail.deleteRow();
            }

            //validate if there is a detail record
            if (getItemCount() <= 0) {
                setMessage("There is no item in this transaction");
                addDetail(); //add detail to prevent error on the next attempt of saving
                return false;
            }

            //assign values to master record
            p_oMaster.first();
            p_oMaster.updateObject("sBranchCd", (String) p_oNautilus.getBranchConfig("sBranchCd"));
            p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());      

            String lsSQL = "SELECT dCreatedx FROM xxxTempTransactions" +
                            " WHERE sSourceCd = " + SQLUtil.toSQL(SOURCE_CODE) +
                                " AND sOrderNox = " + SQLUtil.toSQL(p_sOrderNox);
            
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            while (loRS.next()){
                p_oMaster.updateObject("dCreatedx", loRS.getString("dCreatedx"));
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
    
    public JSONObject searchTransaction(String fsKey, Object foValue, boolean fbExact){
        p_oSTrans.setKey(fsKey);
        p_oSTrans.setValue(foValue);
        p_oSTrans.setExact(fbExact);

        p_oSTrans.addFilter("Source", p_sBranchCd);
        p_oSTrans.addFilter("Status", p_nTranStat);
        
        return p_oSTrans.Search();
    }
    
    public InvSearchF getSearchTransaction(){
        return p_oSTrans;
    }
    
    public JSONObject searchAcceptance(String fsKey, Object foValue, boolean fbExact){
        p_oSTrans.setKey(fsKey);
        p_oSTrans.setValue(foValue);
        p_oSTrans.setExact(fbExact);
        
        p_oSTrans.addFilter("Destination", p_sBranchCd);
        p_oSTrans.addFilter("Status", p_nTranStat);

        return p_oSTrans.Search();
    }
    
    public InvSearchF getSearchAcceptance(){
        return p_oSTrans;
    }
    
    public JSONObject searchDestination(String fsKey, Object foValue, boolean fbExact){
        p_oSDestinat.setKey(fsKey);
        p_oSDestinat.setValue(foValue);
        p_oSDestinat.setExact(fbExact);

        return p_oSDestinat.Search();
    }
    
    public ParamSearchF getSearchDestination(){
        return p_oSDestinat;
    }
    
    public JSONObject searchParts(String fsKey, Object foValue, boolean fbExact){
        p_oSParts.setKey(fsKey);
        p_oSParts.setValue(foValue);
        p_oSParts.setExact(fbExact);
        
        return p_oSParts.Search();
    }
    
    public InvSearchF getSearchParts(){
        return p_oSParts;
    }
    
    private void getMaster(int fnIndex, Object foValue) throws SQLException, ParseException{       
        JSONObject loJSON;
        JSONParser loParser = new JSONParser();
        
        switch(fnIndex){
            case 4: //sDestinat
                loJSON = searchDestination("sBranchCd", foValue, true);
                
                if ("success".equals((String) loJSON.get("result"))){
                    loJSON = (JSONObject) ((JSONArray) loParser.parse((String) loJSON.get("payload"))).get(0);
                    
                    p_oMaster.first();
                    p_oMaster.updateObject("sDestinat", (String) loJSON.get("sBranchCd"));
                    p_oMaster.updateObject("xDestinat", (String) loJSON.get("sCompnyNm"));
                    p_oMaster.updateRow();
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(MiscUtil.getColumnIndex(p_oMaster, "xDestinat"), getMaster("xDestinat"));
                }
                break;
        }
    }
    
    private void getDetail(int fnRow, int fnIndex, Object foValue) throws SQLException, ParseException{       
        JSONObject loJSON;
        JSONParser loParser = new JSONParser();
        
        switch(fnIndex){
            case 3: //sStockIDx
                loJSON = searchParts("a.sStockIDx", foValue, true);
                
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
                    p_oDetail.updateObject(3, (String) loJSON.get("sStockIDx"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "nQuantity"), Integer.parseInt(String.valueOf(p_oDetail.getObject(MiscUtil.getColumnIndex(p_oDetail, "nQuantity")))) + 1);
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xQtyOnHnd"), Integer.parseInt(String.valueOf(loJSON.get("nQtyOnHnd"))));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "nInvCostx"), (double) loJSON.get("nUnitPrce"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xBarCodex"), (String) loJSON.get("sBarCodex"));
                    p_oDetail.updateObject(MiscUtil.getColumnIndex(p_oDetail, "xDescript"), (String) loJSON.get("sDescript"));
                    p_oDetail.updateRow();                    
                    if (!lbExist) addDetail();
                    
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "xBarCodex"), getDetail(fnRow, "xBarCodex"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "xDescript"), getDetail(fnRow, "xDescript"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "nQuantity"), getDetail(fnRow, "nQuantity"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "nInvCostx"), getDetail(fnRow, "nInvCostx"));
                    if (p_oListener != null) p_oListener.DetailRetreive(fnRow, MiscUtil.getColumnIndex(p_oDetail, "xQtyOnHnd"), getDetail(fnRow, "xQtyOnHnd"));
                }
        }
    }
    
    public boolean AcceptDelivery(Date fdReceived){
        System.out.println(this.getClass().getSimpleName() + ".AcceptDelivery(Date fdReceived)");
        
        try {
            if (p_nEditMode != EditMode.READY){
                setMessage("No transaction to loaded.");
                return false;
            }

            p_oMaster.first();
            
            if (!(TransactionStatus.STATE_CLOSED).equals((String) p_oMaster.getObject("cTranStat"))){
                setMessage("Unable to received not printed transfers.");
                return false;
            }        
            
            InvTrans loTrans = new InvTrans(p_oNautilus, p_sBranchCd);
            
            String lsSQL;
            int lnRow = getItemCount();
            
            if (loTrans.InitTransaction()){
                for (int lnCtr = 0; lnCtr <= lnRow - 1; lnCtr++){
                    p_oDetail.absolute(lnCtr + 1);

                    loTrans.setMaster(lnCtr, "sStockIDx", p_oDetail.getString("sStockIDx"));
                    loTrans.setMaster(lnCtr, "sSupersed", p_oDetail.getString("sOrigIDxx"));
                    loTrans.setMaster(lnCtr, "nQuantity", p_oDetail.getInt("nQuantity"));

                    if (!p_oDetail.getString("sRecvIDxx").isEmpty() ||
                        !p_oDetail.getString("sNotesxxx").isEmpty()){
                        lsSQL = "UPDATE " + DETAIL_TABLE + " SET" +
                                    "  sRecvIDxx = " + SQLUtil.toSQL(p_oDetail.getString("sRecvIDxx")) +
                                    ", sNotesxxx = " + SQLUtil.toSQL(p_oDetail.getString("sNotesxxx")) +
                                " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sTransNox")) +
                                    " AND nEntryNox = " + p_oDetail.getInt("nEntryNox");

                        if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                            if (!p_bWithParent) p_oNautilus.rollbackTrans();
                            setMessage(p_oNautilus.getMessage());
                            return false;
                        }
                    }
                }
                
                if (!loTrans.AcceptDelivery(p_oMaster.getString("sTransNox"), 
                                            fdReceived, 
                                            EditMode.ADDNEW)){
                    setMessage(loTrans.getMessage());
                    return false;
                }
            }
            
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  sReceived = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx"))+
                        ", dReceived = " + SQLUtil.toSQL(fdReceived) +
                        ", cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_POSTED) +
                        ", dModified= " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
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
    
    private boolean saveInvTrans() throws SQLException{
        InvTrans loTrans = new InvTrans(p_oNautilus, p_sBranchCd);
        int lnRow = getItemCount();
        
        if (loTrans.InitTransaction()){
            p_oMaster.first();
            for (int lnCtr = 0; lnCtr <= lnRow-1; lnCtr++){
                p_oDetail.absolute(lnCtr + 1);
                loTrans.setMaster(lnCtr, "sStockIDx", p_oDetail.getString("sStockIDx"));
                loTrans.setMaster(lnCtr, "sSupersed", p_oDetail.getString("sOrigIDxx"));
                loTrans.setMaster(lnCtr, "nQuantity", p_oDetail.getInt("nQuantity"));
            }
            
            if (p_bSaveOrder) if (!saveIssuedOrder()) return false;
            
            if (!loTrans.Delivery(p_oMaster.getString("sTransNox"), 
                                    p_oMaster.getDate("dTransact"), 
                                    EditMode.ADDNEW)){
                setMessage(loTrans.getMessage());
                return false;
            }
            
            return true;
        }
        
        setMessage(loTrans.getMessage());
        return false;
    }
    
    private boolean delInvTrans() throws SQLException{
        InvTrans loTrans = new InvTrans(p_oNautilus, p_sBranchCd);
        int lnRow = getItemCount();
        
        if (loTrans.InitTransaction()){
            p_oMaster.first();
            for (int lnCtr = 0; lnCtr <= lnRow-1; lnCtr++){
                p_oDetail.absolute(lnCtr + 1);
                loTrans.setMaster(lnCtr, "sStockIDx", p_oDetail.getString("sStockIDx"));
                loTrans.setMaster(lnCtr, "nQuantity", p_oDetail.getInt("nQuantity"));
            }
            
            if (p_bSaveOrder) if (!delIssuedOrder()) return false;
            
            if (!loTrans.Delivery(p_oMaster.getString("sTransNox"), 
                                    p_oMaster.getDate("dTransact"), 
                                    EditMode.DELETE)){
                setMessage(loTrans.getMessage());
                return false;
            }
            
            return true;
        }
        
        setMessage(loTrans.getMessage());
        return false;
    }
    
    private boolean saveIssuedOrder(){
        //insert code for updating branch orders to warehouse
        return true;
    }
    
    private boolean delIssuedOrder(){
        //insert code for updating branch orders to warehouse
        return true;
    }
}
