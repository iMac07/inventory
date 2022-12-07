package org.xersys.inventory.roq;

import org.xersys.commander.iface.XNautilus;

public class SPROQProc {
    private final String MASTER_TABLE = "SP_Classification_Master";
    private final String DETAIL_TABLE = "SP_Classification_Detail";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    
    private int p_nYear;
    private int p_nMonth;
    
    public SPROQProc(XNautilus foNautilus, String fsBranchCd){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
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