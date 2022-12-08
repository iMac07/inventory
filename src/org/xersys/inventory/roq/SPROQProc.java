package org.xersys.inventory.roq;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.CommonUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringHelper;

public class SPROQProc {
    private final String MASTER_TABLE = "SP_Classification_Master";
    private final String DETAIL_TABLE = "SP_Classification_Detail";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    
    private String p_sMessage;
    private boolean p_bProcessed;
    private boolean p_bMinMax;
    
    private ClassifyInfo p_oInfo;
    
    private int p_nYear;
    private int p_nMonth;
    
    public SPROQProc(XNautilus foNautilus, String fsBranchCd){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
    }
    
    public String getMessage(){
        return p_sMessage;
    }
    
    public boolean ClassifyABC(){
        return ClassifyABC(p_oNautilus.getServerDate().getYear(), p_oNautilus.getServerDate().getMonth() - 1);
    }
    
    public boolean ClassifyABC(int fnYear, int fnMonth){
        p_nYear = fnYear;
        p_nMonth = fnMonth;
        
        //check if it was the first time that this branch will classify their inventory
        if (isFirstClassify()) return firstClassify();
        
        return true;
    }
    
    private boolean isFirstClassify(){
        return true;
    }
    
    private boolean firstClassify(){
        if (!getClassifyInfo()) return false;
        
        
        
        
        return true;
    }
    
    private boolean isComputeMinMax(){
        int lnMonth = (int) p_oInfo.getMaster("nStrtMnMx");
        
        while (lnMonth > p_nMonth){
            if (lnMonth  == p_nMonth){
                return true;
            }
            
            lnMonth += (int) p_oInfo.getMaster("nStrtMnMx");
        }
        return false;
    }
    
    private boolean checkPeriod(){
        String lsSQL = "SELECT sPeriodxx, sPostedxx" +
                        " FROM SP_Classification_Master" +
                        " WHERE sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " ORDER BY sPeriodxx DESC LIMIT 1";
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        try {
            Date ldDate = CommonUtil.dateAdd(new Date(p_oNautilus.getServerDate().getTime()), Calendar.MONTH, -1);
            
            if (!loRS.next()){
                int lnDate = Integer.parseInt(SQLUtil.dateFormat(new Date(p_oNautilus.getServerDate().getTime()), "yyyyMM"));
                int lxDate = Integer.parseInt(StringHelper.prepad(String.valueOf(p_nYear), 4, '0') +
                                StringHelper.prepad(String.valueOf(p_nMonth), 2, '0'));
                
                if (!(p_nYear != 0 && p_nMonth != 0 && lnDate > lxDate)){
                    p_nYear = Integer.parseInt(SQLUtil.dateFormat(ldDate, "yyyy"));
                    p_nMonth = Integer.parseInt(SQLUtil.dateFormat(ldDate, "MM"));
                }
            } else{
                if (Integer.parseInt(loRS.getString("sPeriodxx")) < Integer.parseInt(SQLUtil.dateFormat(ldDate, "yyyyMM"))){
                    String lsPeriod = loRS.getString("sPeriodxx").substring(0, 4) + "-" +
                                        loRS.getString("sPeriodxx").substring(4) + "-01";
                    ldDate = SQLUtil.toDate(lsPeriod, SQLUtil.FORMAT_SHORT_DATE);
                    ldDate = CommonUtil.dateAdd(ldDate, Calendar.MONTH, 1);
                    p_nYear = Integer.parseInt(SQLUtil.dateFormat(ldDate, "yyyy"));
                    p_nMonth = Integer.parseInt(SQLUtil.dateFormat(ldDate, "MM"));
                } else {
                    if (!loRS.getString("sPostedxx").isEmpty()){
                        p_sMessage = "SP Classification was already posted for the criteria month!";
                        return false;
                    }
                    p_bProcessed = true;
                }
            }
            
            
        } catch (SQLException e) {
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    private boolean getClassifyInfo(){
        p_oInfo = new ClassifyInfo(p_oNautilus, "SP");
        
        if (!p_oInfo.LoadConfig()){
            p_sMessage = p_oInfo.getMessage();
            return false;
        }
        
        return true;
    }
    
    private String getSQ_Unlock(){
        return "UNLOCK TABLES";
    }
    
    private String getSQ_Lock(){
        return "LOCK TABLES" + 
                "";
    }
}