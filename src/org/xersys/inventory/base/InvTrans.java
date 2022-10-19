package org.xersys.inventory.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.ErrorMessage;
import org.xersys.commander.contants.RecordStatus;
import org.xersys.commander.contants.SystemMessage;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class InvTrans {
    private final XNautilus p_oNautilus;
    
    private String p_sMesssage;
    private String p_sBranchCd;
    private String p_sSourceNo;
    private String p_sSourceCd;
    private String p_sClientID;
    private Date p_dTransact;
    private int p_nEditMode;
    
    private boolean p_bInitTran;
    private boolean p_bWarehous;
    
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oProcsd;
    
    public InvTrans(XNautilus foNautilus, String fsBranchCd){
        p_oNautilus = foNautilus;
        
        if (p_oNautilus != null){
            p_sSourceCd = "";
            p_sSourceNo = "";
            p_sBranchCd = fsBranchCd.isEmpty() ? (String) p_oNautilus.getBranchConfig("sBranchCd") : fsBranchCd;
            p_nEditMode = EditMode.UNKNOWN;
        }
    }
    
    public boolean InitTransaction(){
        if (!p_bInitTran){
            if (p_oNautilus == null){
                setMessage("Application driver is not set.");
                return false;
            }
            
            if (p_sBranchCd.isEmpty()) p_sBranchCd = (String) p_oNautilus.getBranchConfig("sBranchCd");
            p_bWarehous = String.valueOf(p_oNautilus.getBranchConfig("cWarehous")).equals("1");
            p_bInitTran = true;
        }
        
        if (!createMaster()) return false;
        
        return addMaster();
    }
    
    public void setMaster(int fnRow, String fsIndex, Object fsValue){
        if (!p_bInitTran) return;
        
        if (fnRow > getMasterCount()-1) addMaster();
        //if (fnRow == getMasterCount()) addMaster();
        
        try {
            p_oMaster.absolute(fnRow + 1);
            
            switch (fsIndex.toLowerCase()){
                case "sstockidx":
                    p_oMaster.updateString("sStockIDx", (String) fsValue); 
                    p_oMaster.updateRow(); break;
                case "nquantity":
                    p_oMaster.updateInt("nQuantity", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "nqtyonhnd":
                    p_oMaster.updateInt("nQtyOnHnd", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "nbackordr":
                    p_oMaster.updateInt("nBackOrdr", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "nresvordr":
                    p_oMaster.updateInt("nResvOrdr", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "sSupersed":
                    p_oMaster.updateString("sSupersed", (String) fsValue);
                    p_oMaster.updateRow(); break;
                case "nqtyorder":
                    p_oMaster.updateInt("nQtyOrder", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "nqtyissue":
                    p_oMaster.updateInt("nQtyIssue", (int) fsValue);
                    p_oMaster.updateRow(); break;
                case "nunitprce":
                    p_oMaster.updateInt("nUnitPrce", (int) fsValue);
                    p_oMaster.updateRow(); break;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    
    public boolean RetailOrder(String fsSourceNo,
                                Date fdTransDate,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.RETAIL_ORDER;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean PurchaseOrder(String fsSourceNo,
                                Date fdTransDate,
                                String fsSupplier,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.PURCHASE;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        p_sClientID = fsSupplier;
        
        return saveTransaction();
    }

    public boolean POReceiving(String fsSourceNo,
                                Date fdTransDate,
                                String fsSupplier,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.PURCHASE_RECEIVING;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        p_sClientID = fsSupplier;
        
        return saveTransaction();
    }
    
    public boolean POReturn(String fsSourceNo,
                                Date fdTransDate,
                                String fsSupplier,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.PURCHASE_RETURN;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        p_sClientID = fsSupplier;
        
        return saveTransaction();
    }
    
    public boolean Sales(String fsSourceNo,
                                Date fdTransDate,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.SALES;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean JobOrder(String fsSourceNo,
                                Date fdTransDate,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.JOB_ORDER;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean WholeSale(String fsSourceNo,
                                Date fdTransDate,
                                int fnUpdateMode){        
        p_sSourceCd = InvConstants.WHOLESALE;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean CreditMemo(String fsSourceNo,
                                    Date fdTransDate,
                                    int fnUpdateMode){        
        p_sSourceCd = InvConstants.CREDIT_MEMO;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean DebitMemo(String fsSourceNo,
                                    Date fdTransDate,
                                    int fnUpdateMode){        
        p_sSourceCd = InvConstants.DEBIT_MEMO;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean AcceptDelivery(String fsSourceNo,
                                    Date fdTransDate,
                                    int fnUpdateMode){        
        p_sSourceCd = InvConstants.ACCEPT_DELIVERY;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    public boolean Delivery(String fsSourceNo,
                                    Date fdTransDate,
                                    int fnUpdateMode){        
        p_sSourceCd = InvConstants.DELIVERY;
        p_sSourceNo = fsSourceNo;
        p_dTransact = fdTransDate;
        p_nEditMode = fnUpdateMode;
        
        return saveTransaction();
    }
    
    private boolean saveTransaction(){
        setMessage("");
        
        if (!(p_nEditMode == EditMode.ADDNEW ||
                p_nEditMode == EditMode.DELETE)){
            setMessage(SystemMessage.INVALID_UPDATE_MODE);
            return false;
        }
        
        if (!loadTransaction()){
            setMessage(SystemMessage.UNABLE_TO_LOAD_TRANS);
            return false;
        } 
        
        if (!processInventory()) return false;
        
        if (p_nEditMode == EditMode.DELETE) return DeleteTransaction();
        
        return saveDetail();
    }
    
    private boolean DeleteTransaction(){
        setMessage("");
        
        try {
            p_oProcsd.beforeFirst();
            
            while (p_oProcsd.next()){
                if (!delTransaction(p_oProcsd.getString("sStockIDx"), 
                                    p_oProcsd.getInt("nQtyInxxx"), 
                                    p_oProcsd.getInt("nQtyOutxx"), 
                                    p_oProcsd.getInt("nQtyIssue"), 
                                    p_oProcsd.getInt("nQtyOrder"))) return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return false;
        }
        
        return true;
    }
    
    private boolean delTransaction(String fsStockIDx,
                                    int fnQtyInxxx,
                                    int fnQtyOutxx,
                                    int fnQtyIssue,
                                    int fnQytOrder){
        
        String lsSQL = "UPDATE Inv_Master SET" +
                            "  nQtyOnHnd = nQtyOnHnd + " + (fnQtyOutxx - fnQtyInxxx) +
                            ", nBackOrdr = nBackOrdr - " + (fnQytOrder) +
                            ", nResvOrdr = nResvOrdr + " + (fnQtyIssue) +
                            ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                        " WHERE sStockIDx = " + SQLUtil.toSQL(fsStockIDx) +
                            " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd); 
        
        if (p_oNautilus.executeUpdate(lsSQL, "Inv_Master", p_sBranchCd, "") <= 0){
            setMessage(p_oNautilus.getMessage());
            return false;
        }
        
        return true;
    }
    
    private boolean loadTransaction(){
        return true;
    }
    
    private boolean processInventory(){
        int lnCtr, lnRow, lnMaster;
        
        String lsSQL;
        ResultSet loRS;
        
        try {
            createProcessed();
            lnMaster = getMasterCount();

            for (lnCtr = 0; lnCtr <= lnMaster-1; lnCtr++){
                p_oMaster.absolute(lnCtr + 1);
                lnRow = findOnProcInventory("sStockIDx", p_oMaster.getString("sStockIDx"));
                
                //new record
                if (lnRow == -1){
                    lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sStockIDx = " + SQLUtil.toSQL(p_oMaster.getString("sStockIDx")) +
                                                                    " AND a.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd));
                    
                    loRS = p_oNautilus.executeQuery(lsSQL);
                    
                    addProcessed();
                    p_oProcsd.absolute(getProcessedCount());
                    
                    if (!loRS.next()){
                        p_oProcsd.updateInt("nQtyOnHnd", 0);
                        p_oProcsd.updateInt("nBackOrdr", 0);
                        p_oProcsd.updateInt("nResvOrdr", 0);
                        p_oProcsd.updateInt("nFloatQty", 0);
                        p_oProcsd.updateString("cNewPartx", "1");
                        p_oProcsd.updateString("cRecdStat", RecordStatus.ACTIVE);
                    } else {
                        p_oProcsd.updateInt("nQtyOnHnd", loRS.getInt("nQtyOnHnd"));
                        p_oProcsd.updateInt("nBackOrdr", loRS.getInt("nBackOrdr"));
                        p_oProcsd.updateInt("nResvOrdr", loRS.getInt("nResvOrdr"));
                        p_oProcsd.updateInt("nFloatQty", loRS.getInt("nFloatQty"));
                        p_oProcsd.updateString("cNewPartx", "0");
                        p_oProcsd.updateString("cRecdStat", RecordStatus.ACTIVE);
                    }
                    
                    p_oProcsd.updateString("sStockIDx", p_oMaster.getString("sStockIDx"));      
                    p_oProcsd.updateInt("nQtyInxxx", 0);
                    p_oProcsd.updateInt("nQtyOutxx", 0);
                    p_oProcsd.updateInt("nQtyOrder", 0);
                    p_oProcsd.updateInt("nQtyIssue", 0);
                    
                }
                
                switch (p_sSourceCd){
                    case InvConstants.PURCHASE:
                        p_oProcsd.updateInt("nQtyOrder", p_oProcsd.getInt("nQtyOrder") + p_oMaster.getInt("nQtyOrder"));
                        p_oProcsd.updateInt("nQtyIssue", p_oProcsd.getInt("nQtyIssue") + p_oMaster.getInt("nQtyIssue"));
                        p_oProcsd.updateDouble("nUnitPrce", p_oMaster.getDouble("nUnitPrce"));
                        break;
                    case InvConstants.PURCHASE_RECEIVING:
                        p_oProcsd.updateInt("nQtyInxxx", p_oProcsd.getInt("nQtyInxxx") + p_oMaster.getInt("nQuantity"));
                        
                        if (p_oMaster.getString("sSupersed").isEmpty())
                            p_oProcsd.updateInt("nQtyOrder", p_oProcsd.getInt("nQtyOrder") - p_oMaster.getInt("nQuantity"));
                        
                        p_oProcsd.updateDouble("nUnitPrce", p_oMaster.getDouble("nUnitPrce"));
                        break;
                    case InvConstants.PURCHASE_RETURN:
                        p_oProcsd.updateInt("nQtyOutxx", p_oProcsd.getInt("nQtyOutxx") + p_oMaster.getInt("nQuantity"));
                        break;
                    case InvConstants.RETAIL_ORDER:
                        p_oProcsd.updateInt("nQtyIssue", p_oProcsd.getInt("nQtyIssue") - p_oMaster.getInt("nQuantity"));
                        break;
                    case InvConstants.SALES:
                    case InvConstants.JOB_ORDER:
                    case InvConstants.DELIVERY:
                        p_oProcsd.updateInt("nQtyOutxx", p_oProcsd.getInt("nQtyOutxx") + p_oMaster.getInt("nQuantity"));
                        
                        if (p_oMaster.getString("sSupersed").isEmpty())
                            p_oProcsd.updateInt("nQtyIssue", p_oProcsd.getInt("nQtyIssue") + p_oMaster.getInt("nResvOrdr"));
                        break;
                    case InvConstants.SALES_RETURN:
                        p_oProcsd.updateInt("nQtyInxxx", p_oProcsd.getInt("nQtyInxxx") + p_oMaster.getInt("nQuantity"));
                        break;
                    case InvConstants.CREDIT_MEMO:
                        p_oProcsd.updateInt("nQtyOutxx", p_oProcsd.getInt("nQtyOutxx") + p_oMaster.getInt("nQuantity"));
                        break;
                    case InvConstants.DEBIT_MEMO:
                        p_oProcsd.updateInt("nQtyInxxx", p_oProcsd.getInt("nQtyInxxx") + p_oMaster.getInt("nQuantity"));
                        break;
                }
                
                //commit update
                p_oProcsd.updateRow();
                
                if (!p_oMaster.getString("sSupersed").isEmpty()){
                    lnRow = findOnProcInventory("sStockIDx", p_oMaster.getString("sStockIDx"));
                    
                    if (lnRow == -1){
                        lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sStockIDx = " + SQLUtil.toSQL(p_oMaster.getString("sSupersed")) +
                                                                        " AND a.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd));

                        loRS = p_oNautilus.executeQuery(lsSQL);

                        addProcessed();
                        p_oProcsd.absolute(getProcessedCount());

                        if (!loRS.next()){
                            p_oProcsd.updateInt("nQtyOnHnd", 0);
                            p_oProcsd.updateInt("nBackOrdr", 0);
                            p_oProcsd.updateInt("nResvOrdr", 0);
                            p_oProcsd.updateInt("nFloatQty", 0);
                            p_oProcsd.updateString("cNewPartx", "0");
                            p_oProcsd.updateString("cRecdStat", RecordStatus.ACTIVE);
                        } else {
                            p_oProcsd.updateInt("nQtyOnHnd", loRS.getInt("nQtyOnHnd"));
                            p_oProcsd.updateInt("nBackOrdr", loRS.getInt("nBackOrdr"));
                            p_oProcsd.updateInt("nResvOrdr", loRS.getInt("nResvOrdr"));
                            p_oProcsd.updateInt("nFloatQty", loRS.getInt("nFloatQty"));
                            p_oProcsd.updateString("cNewPartx", "1");
                            p_oProcsd.updateString("cRecdStat", RecordStatus.ACTIVE);
                        }

                        p_oProcsd.updateString("sStockIDx", p_oMaster.getString("sStockIDx"));      
                        p_oProcsd.updateInt("nQtyInxxx", 0);
                        p_oProcsd.updateInt("nQtyOutxx", 0);
                        p_oProcsd.updateInt("nQtyOrder", 0);
                        p_oProcsd.updateInt("nQtyIssue", 0);
                    }
                    
                    switch (p_sSourceCd){
                        case InvConstants.SALES:
                        case InvConstants.JOB_ORDER:
                            p_oProcsd.updateInt("nQtyIssue", p_oProcsd.getInt("nQtyIssue") + p_oMaster.getInt("nResvOrdr"));
                            break;
                        case InvConstants.PURCHASE_RECEIVING:
                            p_oProcsd.updateInt("nQtyOrder", p_oProcsd.getInt("nQtyOrder") + p_oMaster.getInt("nQuantity"));
                            break;
                    }
                    
                    p_oProcsd.updateRow();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }
        
        
        return true;
    }
    
    private boolean saveDetail(){
        int lnCtr, lnRow, lnProc;
        int lnQtyOnHnd, lnBackOrdr, lnResvOrdr;
        boolean lbActivate = false;
        boolean lbNewInvxx = false;
        String lsSQL;
        
        try {
            lnProc = getProcessedCount();
            
            InvMaster loMaster = new InvMaster(p_oNautilus, p_sBranchCd, true);
            
            for (lnCtr = 0; lnCtr <= lnProc - 1; lnCtr++){
                p_oProcsd.absolute(lnCtr + 1);

                if (p_sSourceCd.equals(InvConstants.ACCEPT_DELIVERY) ||
                    p_sSourceCd.equals(InvConstants.ACCEPT_WARRANTY_TRANSFER) ||
                    p_sSourceCd.equals(InvConstants.BRANCH_ORDER) ||
                    p_sSourceCd.equals(InvConstants.BRANCH_ORDER_CONFIRM) ||
                    p_sSourceCd.equals(InvConstants.CUSTOMER_ORDER) ||
                    p_sSourceCd.equals(InvConstants.RETAIL_ORDER) ||
                    p_sSourceCd.equals(InvConstants.PURCHASE) ||
                    p_sSourceCd.equals(InvConstants.PURCHASE_RECEIVING)){

                    lbNewInvxx = p_oProcsd.getString("cNewPartx").equals("1");
                    lbActivate = p_oProcsd.getString("cRecdStat").equals(RecordStatus.INACTIVE);
                }

                if (lbNewInvxx){
                    lnQtyOnHnd = 0;
                    lnBackOrdr = p_oProcsd.getInt("nQtyOrder");
                    lnResvOrdr = Math.abs(p_oProcsd.getInt("nQtyIssue"));
                    
                    if (loMaster.NewRecord()){
                        loMaster.setMaster("sStockIDx", p_oProcsd.getString("sStockIDx"));
                        loMaster.setMaster("nQtyOnHnd", p_oProcsd.getInt("nQtyInxxx"));
                        loMaster.setMaster("nBegQtyxx", p_oProcsd.getInt("nQtyInxxx"));
                        loMaster.setMaster("nBackOrdr", lnBackOrdr);
                        loMaster.setMaster("nResvOrdr", lnResvOrdr);
                        loMaster.setMaster("dBegInvxx", p_dTransact);

                        if (p_oProcsd.getInt("nQtyInxxx") > 0) loMaster.setMaster("dAcquired", p_dTransact);
                        
                        if (!loMaster.SaveRecord()) {
                            setMessage(loMaster.getMessage());
                            return false;
                        }
                    }
                } else {
                    lnQtyOnHnd = p_oProcsd.getInt("nQtyOnHnd") + p_oProcsd.getInt("nQtyInxxx") - p_oProcsd.getInt("nQtyOutxx");
                    lnBackOrdr = p_oProcsd.getInt("nBackOrdr") + p_oProcsd.getInt("nQtyOrder");
                    System.out.println(p_oProcsd.getInt("nResvOrdr"));
                    System.out.println(p_oProcsd.getInt("nQtyIssue"));
                    lnResvOrdr = p_oProcsd.getInt("nResvOrdr") - p_oProcsd.getInt("nQtyIssue");
                    
                    if (loMaster.OpenRecord(p_oProcsd.getString("sStockIDx"))){
                        if (loMaster.UpdateRecord()){
                            if (lnBackOrdr < 0)
                                loMaster.setMaster("nBackOrdr", 0);
                            else
                                loMaster.setMaster("nBackOrdr", (int) loMaster.getMaster("nBackOrdr") + p_oProcsd.getInt("nQtyOrder"));

                            if (lnResvOrdr < 0)
                                loMaster.setMaster("nResvOrdr", 0);
                            else
                                loMaster.setMaster("nResvOrdr", (int) loMaster.getMaster("nResvOrdr") - p_oProcsd.getInt("nQtyIssue"));
                                
                            if (lbActivate) loMaster.setMaster("cRecdStat", RecordStatus.ACTIVE);

                            loMaster.setMaster("nQtyOnHnd", (int) loMaster.getMaster("nQtyOnHnd") + p_oProcsd.getInt("nQtyInxxx") - p_oProcsd.getInt("nQtyOutxx"));
                            
                            if (!loMaster.SaveRecord()) {
                                setMessage(loMaster.getMessage());
                                return false;
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }       
        
        return true;
    }
    
    private int findOnProcInventory(String fsFieldNm, String fsValue) throws SQLException{
        if (p_oProcsd == null) return -1;
        
        for (int lnCtr = 0; lnCtr <= getProcessedCount() - 1; lnCtr++){
            p_oProcsd.absolute(lnCtr + 1);
           
            if (p_oProcsd.getString(fsFieldNm).equals(fsValue)) return lnCtr;
        }
        
        return -1;
    }
    
    public String getMessage(){
        return p_sMesssage;
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sStockIDx" +
                    ", a.nQtyOnHnd" +
                    ", a.nBackOrdr" +
                    ", a.nResvOrdr" +
                    ", a.nFloatQty" +
                    ", a.dAcquired" +
                    ", a.cRecdStat" +
                    ", IFNULL(b.sSupersed, '') sSupersed" +
                    ", 0 nQuantity" +
                    ", 0 nQtyInxxx" +
                    ", 0 nQtyOutxx" +
                    ", 0 nQtyOrder" +
                    ", 0 nQtyIssue" +
                    ", '0' cNewPartx" +
                    ", IFNULL(b.nUnitPrce, 0.00) nUnitPrce" +
                " FROM Inv_Master a" +
                    " LEFT JOIN Inventory b ON a.sStockIDx = b.sStockIDx";
    }
    
    private boolean createMaster(){
        try {
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);                        
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }
        
        return true;
    }
    
    private boolean createProcessed(){
        try {
            String lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
            p_oProcsd = factory.createCachedRowSet();
            p_oProcsd.populate(loRS);
            MiscUtil.close(loRS);                        
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }
        
        return true;
    }
    
    private boolean addMaster(){
        try {
            p_oMaster.last();
            p_oMaster.moveToInsertRow();

            MiscUtil.initRowSet(p_oMaster);
            p_oMaster.insertRow();
            p_oMaster.moveToCurrentRow();
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }

        return true;
    }
    
    private int getMasterCount(){
        try {
            p_oMaster.last();
            return p_oMaster.getRow();
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return -1;
        }
    }
    
    private int getProcessedCount(){
        try {
            p_oProcsd.last();
            return p_oProcsd.getRow();
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return -1;
        }
    }
    
    private boolean addProcessed(){
        try {
            p_oProcsd.last();
            p_oProcsd.moveToInsertRow();

            MiscUtil.initRowSet(p_oProcsd);
            p_oProcsd.insertRow();
            p_oProcsd.moveToCurrentRow();
        } catch (SQLException ex) {
            ex.printStackTrace();
            setMessage(ErrorMessage.SQL);
            return false;
        }

        return true;
    }
    
    private void setMessage(String fsValue){
        p_sMesssage = fsValue;
    }
}
