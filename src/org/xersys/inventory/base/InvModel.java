package org.xersys.inventory.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.parser.ParseException;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class InvModel {
    private XNautilus p_oNautilus;
    private LMasDetTrans _listener;
    
    private String p_sMessagex;
    private boolean p_bLoaded;
    private int p_nEditMode;
    
    private CachedRowSet p_oModel;
    
    public InvModel(XNautilus foNautilus){
        p_oNautilus = foNautilus;
    }
    
    public void setListener(Object foListener) {
        _listener = (LMasDetTrans) foListener;
    }
    
    public CachedRowSet getData(){
        return p_oModel;
    }
    
    public boolean LoadRecord(CachedRowSet foValue) throws SQLException{
        System.out.println(this.getClass().getSimpleName() + ".LoadRecord(CachedRowSet foValue)");
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        p_oModel = foValue;
        
        AddItem();
        
        p_nEditMode = EditMode.UPDATE;
        
        p_bLoaded = true;
        return true;
    }
    
    public boolean LoadRecord(String fsClientID) throws SQLException{
        System.out.println(this.getClass().getSimpleName() + ".LoadRecord(String fsClientID)");
        
        if (p_oNautilus == null){
            p_sMessagex = "Application driver is not set.";
            return false;
        }
        
        String lsSQL;
        ResultSet loRS;
        
        RowSetFactory factory = RowSetProvider.newFactory();
        
        if (fsClientID.isEmpty()){
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oModel = factory.createCachedRowSet();
            p_oModel.populate(loRS);
            MiscUtil.close(loRS);
            
            AddItem();
            
            p_nEditMode = EditMode.ADDNEW;
        } else {
            loRS = p_oNautilus.executeQuery(MiscUtil.addCondition(getSQ_Master(), "sStockIDx = " + SQLUtil.toSQL(fsClientID)));
            
            p_oModel = factory.createCachedRowSet();
            p_oModel.populate(loRS);
            MiscUtil.close(loRS);
            
            AddItem();
            
            p_nEditMode = EditMode.UPDATE;
        }
        
        p_bLoaded = true;
        return true;
    }
    
    public int getItemCount(){
        try {
            p_oModel.last();
            return p_oModel.getRow();
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return -1;
        }
    }
    
    public boolean AddItem() throws SQLException{
        if (p_oModel.size()== 0){
            p_oModel.moveToInsertRow();
        
            MiscUtil.initRowSet(p_oModel);
            p_oModel.updateObject("nEntryNox", p_oModel.size() + 1);

            p_oModel.insertRow();
            p_oModel.moveToCurrentRow();
        } else {
            p_oModel.last();
        
            if (!((String) p_oModel.getObject("sModelCde")).isEmpty()){
                p_oModel.moveToInsertRow();

                MiscUtil.initRowSet(p_oModel);
                p_oModel.updateObject("nEntryNox", p_oModel.size() + 1);

                p_oModel.insertRow();
                p_oModel.moveToCurrentRow();
            }
        }

        return true;
    }
    
    public boolean RemoveItem(int fnRow) throws SQLException{
        if (!p_bLoaded){
            setMessage("No record was loaded.");
            return false;
        }
        
        if (fnRow < 0){
            setMessage("Invalid row item.");
            return false;
        }
        
        p_oModel.absolute(fnRow + 1);
        if (((String) p_oModel.getObject("sStockIDx")).isEmpty()){
            p_oModel.deleteRow();
            AddItem();
        }
            
        return true;
    }
    
    public void setDetail(int fnRow, String fsFieldNm, Object foValue) throws SQLException, ParseException {
        if (p_nEditMode != EditMode.ADDNEW &&
            p_nEditMode != EditMode.UPDATE){
            System.err.println("Transaction is not on update mode.");
            return;
        }
        
        p_oModel.absolute(fnRow + 1);
        p_oModel.updateObject(fsFieldNm, foValue);
        p_oModel.updateRow();

        AddItem();    

        _listener.DetailRetreive(fnRow, fsFieldNm, "");
    }

    public Object getDetail(int fnRow, String fsFieldNm) throws SQLException{
        p_oModel.absolute(fnRow + 1);
        return p_oModel.getObject(fsFieldNm);
    }
    
    public String getMessage(){
        return p_sMessagex;
    }
    
    private void setMessage(String fsValue){
        p_sMessagex = fsValue;
    }
        
    private String getSQ_Master(){
        return "SELECT" +
                    "  sStockIDx" +
                    ", nEntryNox" +
                    ", sModelCde" +
                " FROM Inv_Model" +
                " ORDER BY nEntryNox";
    }
}
