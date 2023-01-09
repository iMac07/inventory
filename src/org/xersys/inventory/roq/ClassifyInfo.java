package org.xersys.inventory.roq;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.inventory.base.InvMaster;

public class ClassifyInfo {
    private final String MASTER_TABLE = "Inv_Classification_Config";
    
    private final XNautilus p_oNautilus;
    private final String p_sInvTypCd;
    
    private String p_sMessage;
    
    private CachedRowSet p_oMaster;
    
    public ClassifyInfo(XNautilus foNautilus, String fsInvTypCd){
        p_oNautilus = foNautilus;
        p_sInvTypCd = fsInvTypCd;
    }
    
    public String getMessage(){
        return p_sMessage;
    }
    
    public Object getMaster(String fsFieldNm){
        try {
            return getMaster(MiscUtil.getColumnIndex(p_oMaster, fsFieldNm));
        } catch (SQLException e) {
            return null;
        }
    }
    
    public Object getMaster(int fnIndex) {
        try {
            p_oMaster.first();
            return p_oMaster.getObject(fnIndex);
        } catch (SQLException ex) {
            return null;
        }
    }
    
    public boolean LoadConfig(){
        if (p_oNautilus == null){
            p_sMessage = "Application driver is not set.";
            return false;
        }
        
        if (p_sInvTypCd.isEmpty()){
            p_sMessage = "Inventory type is not set.";
            return false;
        }
        
        try {
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "sDivision = " + SQLUtil.toSQL(p_sInvTypCd));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
        } catch (SQLException ex) {
            p_sMessage = ex.getMessage();
            return false;
        }
        
        return true;
    }
    
    
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  sDivision" +
                    ", nOrdrFreq" +
                    ", nSafetyCA" +
                    ", nSafetyCB" +
                    ", nSafetyCC" +
                    ", nSafetyCD" +
                    ", nVolRateA" +
                    ", nVolRateB" +
                    ", nVolRateC" +
                    ", nVolRateD" +
                    ", nMinStcCA" +
                    ", nMaxStcCA" +
                    ", nMinStcCB" +
                    ", nMaxStcCB" +
                    ", nMinStcCC" +
                    ", nMaxStcCC" +
                    ", nMinStcCD" +
                    ", nMaxStcCD" +
                    ", nMaxQtyCD" +
                    ", nMinStcCE" +
                    ", nMaxStcCE" +
                    ", nMinStcCF" +
                    ", nMaxStcCF" +
                    ", nMaxQtyCF" +
                    ", nPurcLdTm" +
                    ", nNoMonths" +
                    ", nNoMinMax" +
                    ", nStrtMnMx" +
                " FROM " + MASTER_TABLE;
    }
}
