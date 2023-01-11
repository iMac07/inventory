package org.xersys.inventory.roq;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import javax.xml.soap.Detail;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.CommonUtil;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringHelper;

public class SPROQProc {
    private final String MASTER_TABLE = "Inv_Classification_Master";
    private final String DETAIL_TABLE = "Inv_Classification_Detail";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    
    private String p_sMessage;
    
    private boolean p_bProcessed;
    private boolean p_bMinMax;
    private String p_asPeriod [];
    
    private ClassifyInfo p_oInfo;
    
    private int p_nYear;
    private int p_nMonth;
    
    private CachedRowSet p_oDetail;
    
    public SPROQProc(XNautilus foNautilus, String fsBranchCd){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
    }
    
    public String getMessage(){
        return p_sMessage;
    }
    
    private void initPeriod(){
        p_asPeriod = new String[(int) p_oInfo.getMaster("nNoMonths")];
        
        String lsDate = String.valueOf(p_nYear) + "-" + StringHelper.prepad(String.valueOf(p_nMonth), 2, '0') + "-01";
        Date ldDate = SQLUtil.toDate(lsDate, SQLUtil.FORMAT_SHORT_DATE);
        
        for (int lnCtr = 0; lnCtr <= p_asPeriod.length - 1; lnCtr++){
            p_asPeriod[lnCtr] = SQLUtil.dateFormat(ldDate, "yyyyMM");
            ldDate = CommonUtil.dateAdd(ldDate, Calendar.MONTH, -1);
        }
    }
    
    private boolean isComputeMinMax(){
        int lnMonth = (int) p_oInfo.getMaster("nNoMonths");
        
        while (lnMonth <= p_nMonth){
            if (lnMonth == p_nMonth) return true;
            
            lnMonth += (int) p_oInfo.getMaster("nNoMonths");
        }
        
        return false;
    }
    
    public boolean ClassifyABC(){
        return ClassifyABC(p_oNautilus.getServerDate().getYear(), p_oNautilus.getServerDate().getMonth() - 1);
    }
    
    public boolean ClassifyABC(int fnYear, int fnMonth){
        p_nYear = fnYear;
        p_nMonth = fnMonth;
        
        //get classification config
        getClassifyInfo();
        
        //initialize period
        initPeriod();
        
        if (!checkPeriod()) return false;
        
        //check if min max must be calculated
        p_bMinMax = isComputeMinMax();
        
        try {
            //check if it was the first time that this branch will classify their inventory
            //if (isFirstClassify()) return firstClassify();
            //return reclassify();
            return firstClassify();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
    }
    
    private boolean isFirstClassify(){
        String lsSQL = "SELECT sPeriodxx" +
                        " FROM " + MASTER_TABLE +
                        " WHERE sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND sInvTypCd = 'SP'" +
                        " ORDER BY sPeriodxx DESC" +
                        " LIMIT 1";
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);

        return MiscUtil.RecordCount(loRS) == 0;
    }
    
    private int getTotal() throws SQLException{
        String lsSQL;
        
        if (p_bProcessed){
            lsSQL = "SELECT SUM(nTotlSale) xTotlSold" +
                    " FROM SP_Classification_Master" +
                    " WHERE sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                       " AND sPeriodxx BETWEEN " + SQLUtil.toSQL(p_asPeriod[p_asPeriod.length - 1]) +
                          " AND " + SQLUtil.toSQL(p_asPeriod[0]);
        } else {
            lsSQL = "SELECT IFNULL(SUM(xTotlSold), 0) xTotlSold" +
                    " FROM (SELECT SUM(nTotlSale) xTotlSold" +
                            " FROM Inv_Classification_Master" +
                            " WHERE sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                               " AND sPeriodxx BETWEEN " + SQLUtil.toSQL(p_asPeriod[p_asPeriod.length - 1]) +
                                  " AND " + SQLUtil.toSQL(p_asPeriod[0]) +
                            " UNION " +
                            "SELECT SUM(nCredtQty - nDebitQty) xTotlSold" +
                            " FROM (" + getSQ_Sales(p_asPeriod[0]) + ") xxx ) xx";
        }
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        if (loRS.next())
            return loRS.getInt("xTotlSold");
        else
            return 0;
    }
    
    private String getSQ_CompDemand(){
        String lsSQL = "SELECT" +
                            "  a.sStockIDx" +
                            ", a.sBarCodex" +
                            ", a.sDescript" +
                            ", IF(b.dAcquired IS NULL, b.dBegInvxx, b.dAcquired) dAcquired" +
                            ", b.cClassify xClassify" +
                            ", b.nMinLevel xMinLevel" +
                            ", b.nMaxLevel xMaxLevel" +
                            ", b.nAvgMonSl xAvgMonSl" +
                            ", b.nBackOrdr" +
                            ", b.nResvOrdr" +
                            ", c.cClassify" +
                            ", c.nMinLevel" +
                            ", c.nMaxLevel" +
                            ", c.nTotlSumx" +
                            ", c.nTotlSumP" +
                            ", c.nAvgMonSl" +
                            ", b.nQtyOnHnd" ;

        String lsFrom = " FROM Inventory a" +
                            ", Inv_Master b";

        String lsWhere = " WHERE a.sStockIDx = b.sStockIDx" +
                            " AND b.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND DATE_FORMAT(IF(b.dAcquired IS NULL, b.dBegInvxx, b.dAcquired), '%Y%m') <= " + SQLUtil.toSQL(p_asPeriod[0]) +
                            " AND b.cRecdStat = '1'";
        
        String lsAlias = "c";
        String lsCompute = "";
        
        for (int lnCtr = 0; lnCtr < p_asPeriod.length - 1; lnCtr++){
            lsSQL += ", " + lsAlias + ".nSoldQtyx nSoldQty" + String.valueOf(lnCtr + 1);
            
            lsFrom += " LEFT JOIN Inv_Classification_Detail " + lsAlias +
                        " ON b.sStockIDx = " + lsAlias + ".sStockIDx" +
                            " AND b.sBranchCd = " + lsAlias + ".sBranchCd" +
                            " AND " + lsAlias + ".sPeriodxx = " + SQLUtil.toSQL(p_asPeriod[lnCtr]);
            if (p_asPeriod.length > lnCtr) {
                if (lnCtr  == 0){
                    lsCompute += ", IFNULL(" + lsAlias + ".nSoldQtyx, 0)";
                } else {
                    lsCompute += " + IFNULL(" + lsAlias + ".nSoldQtyx, 0)";
                }
                
            }
            
            lsAlias = CommonUtil.getNextLetter(lsAlias);
        }
        
        lsCompute += " xTotlSold";
        
        lsSQL = lsSQL + lsCompute + lsFrom + lsWhere + 
                " GROUP BY a.sStockIDx" +
                " ORDER BY xTotlSold DESC" +
                  ", nSoldQty1 DESC";
        
        return lsSQL;
    }
    
    private String getSQ_Demand4Class(){
        String lsSQL = "SELECT" +
                            "  a.sStockIDx" +
                            ", a.sBarCodex" +
                            ", a.sDescript" +
                            ", IF(b.dAcquired IS NULL, b.dBegInvxx, b.dAcquired) dAcquired" +
                            ", b.cClassify xClassify" +
                            ", b.nMinLevel xMinLevel" +
                            ", b.nMaxLevel xMaxLevel" +
                            ", b.nAvgMonSl xAvgMonSl" +
                            ", b.nBackOrdr" +
                            ", b.nResvOrdr" +
                            ", d.cClassify" +
                            ", d.nMinLevel" +
                            ", d.nMaxLevel" +
                            ", d.nTotlSumx" +
                            ", d.nTotlSumP" +
                            ", d.nAvgMonSl" +
                            ", b.nQtyOnHnd" +
                            ", IFNULL(SUM(c.nCredtQty - c.nDebitQty), 0) nSoldQty1";

        String lsFrom = " FROM Inventory a" +
                            ", Inv_Master b";

        String lsWhere = " WHERE a.sStockIDx = b.sStockIDx" +
                            " AND b.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND DATE_FORMAT(IF(b.dAcquired IS NULL, b.dBegInvxx, b.dAcquired), '%Y%m') <= " + SQLUtil.toSQL(p_asPeriod[0]) +
                            " AND b.cRecdStat = '1'";
        
        String lsLedger = getSQ_Sales(p_asPeriod[0]);
        lsFrom += " LEFT JOIN (" + lsLedger + ") c" +
                    " ON b.sStockIDx = c.sStockIDx";
        
        String lsAlias = "d";
        String lsCompute = ", IFNULL(SUM(c.nCredtQty - c.nDebitQty), 0)";
        
        for (int lnCtr = 1; lnCtr < p_asPeriod.length; lnCtr++){
            lsSQL += ", " + lsAlias + ".nSoldQtyx nSoldQty" + String.valueOf(lnCtr + 1);
            
            lsFrom += " LEFT JOIN Inv_Classification_Detail " + lsAlias +
                        " ON b.sStockIDx = " + lsAlias + ".sStockIDx" +
                            " AND b.sBranchCd = " + lsAlias + ".sBranchCd" +
                            " AND " + lsAlias + ".sPeriodxx = " + SQLUtil.toSQL(p_asPeriod[lnCtr]);
            if (p_asPeriod.length > lnCtr) {
                lsCompute += " + IFNULL(" + lsAlias + ".nSoldQtyx, 0)";
            }
            
            lsAlias = CommonUtil.getNextLetter(lsAlias);
        }
        
        lsCompute += " xTotlSold";
        
        lsSQL = lsSQL + lsCompute + lsFrom + lsWhere + 
                " GROUP BY a.sStockIDx" +
                " ORDER BY xTotlSold DESC" +
                  ", nSoldQty1 DESC";
        
        return lsSQL;
    }
    
    private boolean classifyParts() throws SQLException{
        int lnDivisor = getDivisor(p_oDetail.getString("dAcquired"));

        if (p_oDetail.getInt("xTotlSold") > 0){
            if (p_oDetail.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateA")))){
                p_oDetail.updateString("cClassify", "A");
            } else if (p_oDetail.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateB")))){
                p_oDetail.updateString("cClassify", "B");
            } else if (p_oDetail.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateC")))){
                p_oDetail.updateString("cClassify", "C");
            } else {
                p_oDetail.updateString("cClassify", "D");
            }
            p_oDetail.updateRow();

            try {
                if (p_oDetail.getString("cClassify").equals("D")){
                    p_oDetail.updateInt("nAvgMonSl", p_oDetail.getInt("xTotlSold") / lnDivisor);
                } else {
                    p_oDetail.updateInt("nAvgMonSl", Math.round((float) p_oDetail.getInt("xTotlSold") / (float) lnDivisor));
                }
            } catch (java.lang.ArithmeticException e) {
                p_oDetail.updateInt("nAvgMonSl", 0);
            }
            p_oDetail.updateRow();
        } else {
            if (lnDivisor == p_asPeriod.length){

            } else {

            }
            p_oDetail.updateString("cClassify", "E");
            p_oDetail.updateInt("nAvgMonSl", 0);
            p_oDetail.updateRow();
        }

        double lnValue;
        //compute min max
        switch (p_oDetail.getString("cClassify")){
            case "A":
            case "B":
            case "C":
                lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMinStcC" + p_oDetail.getString("cClassify"))));
                p_oDetail.updateInt("nMinLevel", Math.round(p_oDetail.getInt("nAvgMonSl") * (float) lnValue));

                lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxStcC" + p_oDetail.getString("cClassify"))));
                p_oDetail.updateInt("nMaxLevel", Math.round(p_oDetail.getInt("nAvgMonSl") * (float) lnValue));
                break;
            case "D":
            case "F":
                lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMinStcC" + p_oDetail.getString("cClassify"))));
                p_oDetail.updateInt("nMinLevel", Math.round(p_oDetail.getInt("nAvgMonSl") * (float) lnValue));

                lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxStcC" + p_oDetail.getString("cClassify"))));
                int lnTmpAMC = Math.round(p_oDetail.getInt("nAvgMonSl") * (float) lnValue);
                int lnTmpQty = (int) Math.round(Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxQtyC" + p_oDetail.getString("cClassify")))));

                if (lnTmpAMC > lnTmpQty)
                    p_oDetail.updateInt("nMaxLevel", lnTmpAMC);
                else
                    p_oDetail.updateInt("nMaxLevel", lnTmpQty);

                break;
            default:
                p_oDetail.updateInt("nMinLevel", 0);
                p_oDetail.updateInt("nMaxLevel", 0);
        }
        
        p_oDetail.updateRow();
        
        return true;
    }
    
    private boolean firstClassify() throws SQLException{        
        String lsSQL = "";
        
        if (p_bProcessed)
            lsSQL = getSQ_CompDemand();
        else
            lsSQL = getSQ_Demand4Class();
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) == 0){
            p_sMessage = "Unable to retreive spareparts for classification.";
            return false;
        }
        
        int lnTotal = getTotal();
        int lnRunTotal = 0;
        int lnPerTotal = 0;
        
        //resultset to cachedrowset
        RowSetFactory factory = RowSetProvider.newFactory();
        p_oDetail = factory.createCachedRowSet();
        p_oDetail.populate(loRS);
        MiscUtil.close(loRS);
        
        p_oNautilus.beginTrans();
        while (p_oDetail.next()){
            lnRunTotal += p_oDetail.getInt("xTotlSold");
            lnPerTotal += p_oDetail.getInt("nSoldQty1");
            
            p_oDetail.updateInt("nTotlSumx", lnRunTotal);
            p_oDetail.updateDouble("nTotlSumP", (float) lnRunTotal / lnTotal);
            p_oDetail.updateRow();
            
            if (classifyParts()){
                lsSQL = "INSERT INTO " + DETAIL_TABLE + " SET" +
                        "  sStockIDx = " + SQLUtil.toSQL(p_oDetail.getString("sStockIDx")) +
                        ", sInvTypCd = 'SP'" +
                        ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        ", sPeriodxx = " + SQLUtil.toSQL(p_asPeriod[0]) +
                        ", nSoldQtyx = " + p_oDetail.getInt("nSoldQty1") +
                        ", nTotlSold = " + p_oDetail.getInt("xTotlSold") +
                        ", nTotlSumx = " + p_oDetail.getInt("nTotlSumx") +
                        ", nTotlSumP = " + p_oDetail.getDouble("nTotlSumP") +
                        ", nAvgMonSl = " + p_oDetail.getInt("nAvgMonSl") +
                        ", nMinLevel = " + p_oDetail.getInt("nMinLevel") +
                        ", nMaxLevel = " + p_oDetail.getInt("nMaxLevel") +
                        ", cClassify = " + SQLUtil.toSQL(p_oDetail.getString("cClassify"));
                
                if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                    p_sMessage = p_oNautilus.getMessage();
                    p_oNautilus.rollbackTrans();
                    return false;
                }
            }
        }
        
        lsSQL = "INSERT INTO " + MASTER_TABLE + " SET" +
                "  sInvTypCd = 'SP'" +
                ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                ", sPeriodxx = " + SQLUtil.toSQL(p_asPeriod[0].replace("-", "")) +
                ", nTotlSale = " + lnRunTotal +
                ", cTranStat = '2'" +
                ", sProcessd = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx")) +
                ", dProcessd = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                ", sPostedxx = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx")) +
                ", dPostedxx = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate());

        if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
            p_sMessage = p_oNautilus.getMessage();
            p_oNautilus.rollbackTrans();
            return false;
        }
        
        p_oNautilus.commitTrans();
        
        return true;
    }
    
    private boolean reclassify(){
        if (!getClassifyInfo()) return false;
        
        String lsPeriod = String.valueOf(p_nYear) + "-" + StringHelper.prepad(String.valueOf(p_nMonth), 2, '0');
        String lsLstPer = String.valueOf(p_nYear) + "-" + StringHelper.prepad(String.valueOf(p_nMonth - 1), 2, '0');
        
        //get inventory for the period
        String lsSQL = "SELECT" +
                            "  a.sStockIDx" +
                            ", a.sBarCodex" +
                            ", a.sDescript" +
                            ", b.dBegInvxx" +
                            ", b.dAcquired" +
                            ", b.nQtyOnHnd" +
                            ", b.cRecdStat" +
                        " FROM Inventory a" +
                            ", Inv_Master b" +
                        " WHERE a.sStockIDx = b.sStockIDx" +
                            " AND b.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND b.dAcquired <= " + SQLUtil.toSQL(lsPeriod  + "-30") +
                        " ORDER BY  b.dAcquired DESC";
        
        ResultSet loRS = p_oNautilus.executeQuery(lsSQL);
        
        try {
            int lnTotalSold = 0;
            int lnTotalSale = 0;
            int lnItemSold = 0;
            int lnItemTotl = 0;
            
            lsSQL = "SELECT nTotlSale FROM " + MASTER_TABLE +
                    " WHERE sInvTypCd = 'SP'" +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " AND sPeriodxx = " + SQLUtil.toSQL(lsLstPer.replace("-", ""));
                
            ResultSet loSold = p_oNautilus.executeQuery(lsSQL);
            if (loSold.next()) lnTotalSale = loSold.getInt("nTotlSale");
            
            p_oNautilus.beginTrans();
            while (loRS.next()){       
                //get sales
                lsSQL = getSQ_Sales(p_asPeriod[0]);
                lsSQL = "SELECT SUM(nCredtQty) nTotlSold FROM (" + lsSQL + ") x" +
                        " GROUP BY x.sStockIDx";
                lsSQL = lsSQL.replace("xStockIDx", loRS.getString("sStockIDx"));
                lsSQL = lsSQL.replace("xTransact", lsPeriod + "%");
                
                loSold = p_oNautilus.executeQuery(lsSQL);
                
                if (loSold.next())
                    lnItemSold = loSold.getInt("nTotlSold");
                else
                    lnItemSold = 0;
                
                lnTotalSold += lnItemSold;
                
                lsSQL = "SELECT nTotlSold FROM " + DETAIL_TABLE +
                        " WHERE sInvTypCd = 'SP'" +
                            " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND sPeriodxx = " + SQLUtil.toSQL(lsLstPer.replace("-", "")) +
                            " AND sStockIDx = " + SQLUtil.toSQL(loRS.getString("sStockIDx"));
                
                loSold = p_oNautilus.executeQuery(lsSQL);
                if (loSold.next())
                    lnItemTotl = loSold.getInt("nTotlSold") + lnItemSold;
                else
                    lnItemTotl = lnItemSold;
                
                lsSQL = "INSERT INTO " + DETAIL_TABLE + " SET" +
                        "  sStockIDx = " + SQLUtil.toSQL(loRS.getString("sStockIDx")) +
                        ", sInvTypCd = 'SP'" +
                        ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        ", sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                        ", nSoldQtyx = " + lnItemSold +
                        ", nTotlSold = " + lnItemTotl +
                        ", nTotlSumx = 0" +
                        ", nTotlSumP = 0.00" +
                        ", nAvgMonSl = 0" +
                        ", nMinLevel = 0" +
                        ", nMaxLevel = 0" +
                        ", cClassify = 'F'";
                
                if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                    p_sMessage = p_oNautilus.getMessage();
                    p_oNautilus.rollbackTrans();
                    return false;
                }
            }
            
            //save master information
            lsSQL = "INSERT INTO " + MASTER_TABLE + " SET" +
                    "  sInvTypCd = 'SP'" +
                    ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                    ", sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                    ", nTotlSale = " + lnTotalSold +
                    ", cTranStat = '1'" +
                    ", sProcessd = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx")) +
                    ", dProcessd = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate());
            
            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = p_oNautilus.getMessage();
                p_oNautilus.rollbackTrans();
                return false;
            }
            
            //compute total sales
            lsSQL = "SELECT *" + 
                    " FROM " + DETAIL_TABLE +
                    " WHERE sInvTypCd = 'SP'" +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " AND sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                    " ORDER BY nTotlSold DESC, nSoldQtyx DESC";
            
            loRS = p_oNautilus.executeQuery(lsSQL);
            
            lnItemSold = lnTotalSale;
            while (loRS.next()){
                lnItemSold += loRS.getInt("nSoldQtyx");
                
                lsSQL = "UPDATE " + DETAIL_TABLE + " SET" +
                        "  nTotlSumx = " + lnItemSold +
                        ", nTotlSumP = " + (float) lnItemSold / (lnTotalSold + lnTotalSale)  +
                        " WHERE sInvTypCd = 'SP'" +
                            " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                            " AND sStockIDx = " + SQLUtil.toSQL(loRS.getString("sStockIDx")); 
                
                if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                    p_sMessage = p_oNautilus.getMessage();
                    p_oNautilus.rollbackTrans();
                    return false;
                }
            }
            
            //classify
            lsSQL = "SELECT *, b.dAcquired" + 
                    " FROM " + DETAIL_TABLE + " a" +
                        " LEFT JOIN Inv_Master b ON a.sStockIDx = b.sStockIDx AND a.sBranchCd = b.sBranchCd" +
                    " WHERE a.sInvTypCd = 'SP'" +
                        " AND a.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " AND a.sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                    " ORDER BY a.nTotlSold DESC";
            
            loRS = p_oNautilus.executeQuery(lsSQL);
            int lnDivisor;
            String lsClassify = "";
            int lnAveMonSl;
            int lnMinLevel;
            int lnMaxLevel;
            
            while (loRS.next()){
                lnDivisor = getDivisor(loRS.getString("dAcquired"));
                
                if (loRS.getInt("nTotlSold") > 0){
                    if (loRS.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateA")))){
                        lsClassify = "A";
                    } else if (loRS.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateB")))){
                        lsClassify = "B";
                    } else if (loRS.getDouble("nTotlSumP") <= Double.valueOf(String.valueOf(p_oInfo.getMaster("nVolRateC")))){
                        lsClassify = "C";
                    } else {
                        lsClassify = "D";
                    }
                    
                    try {
                        if (lsClassify.equals("D")){
                            lnAveMonSl = loRS.getInt("nTotlSold") / lnDivisor;
                        } else {
                            lnAveMonSl = Math.round((float) loRS.getInt("nTotlSold") / (float) lnDivisor);
                        }
                    } catch (java.lang.ArithmeticException e) {
                        lnAveMonSl = 0;
                    }
                } else {
                    if (lnDivisor == p_asPeriod.length){
                    
                    } else {
                    
                    }
                    
                    lsClassify = "E";
                    lnAveMonSl = 0;
                }
                
                double lnValue;
                //compute min max
                switch (lsClassify){
                    case "A":
                    case "B":
                    case "C":
                        lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMinStcC" + lsClassify)));
                        lnMinLevel = Math.round(lnAveMonSl * (float) lnValue);
                        
                        lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxStcC" + lsClassify)));
                        lnMaxLevel = Math.round(lnAveMonSl * (float) lnValue);
                        break;
                    case "D":
                    case "F":
                        lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMinStcC" + lsClassify)));
                        lnMinLevel = Math.round(lnAveMonSl * (float) lnValue);
                        
                        lnValue = Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxStcC" + lsClassify)));
                        int lnTmpAMC = Math.round(lnAveMonSl * (float) lnValue);
                        int lnTmpQty = (int) Math.round(Double.valueOf(String.valueOf(p_oInfo.getMaster("nMaxQtyC" + lsClassify))));
                        
                        if (lnTmpAMC > lnTmpQty)
                            lnMaxLevel = lnTmpAMC;
                        else
                            lnMaxLevel = lnTmpQty;
                                    
                        break;
                    default:
                        lnMinLevel = 0;
                        lnMaxLevel = 0;
                }
                
                
                lsSQL = "UPDATE " + DETAIL_TABLE + " SET" +
                        "  cClassify = " + SQLUtil.toSQL(lsClassify) +
                        ", nAvgMonSl = " + lnAveMonSl + 
                        ", nMinLevel = " + lnMinLevel +
                        ", nMaxLevel = " + lnMaxLevel +
                        " WHERE sInvTypCd = 'SP'" +
                            " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", "")) +
                            " AND sStockIDx = " + SQLUtil.toSQL(loRS.getString("sStockIDx")); 
                
                if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                    p_sMessage = p_oNautilus.getMessage();
                    p_oNautilus.rollbackTrans();
                    return false;
                }
            }
            
            //save master information
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  cTranStat = '2'" +
                        ", sPostedxx = " + SQLUtil.toSQL((String) p_oNautilus.getUserInfo("sUserIDxx")) +
                        ", dPostedxx = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    " WHERE sInvTypCd = 'SP'" +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " AND sPeriodxx = " + SQLUtil.toSQL(lsPeriod.replace("-", ""));
            
            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = p_oNautilus.getMessage();
                p_oNautilus.rollbackTrans();
                return false;
            }
            
            p_oNautilus.commitTrans();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        
        return true;
    }
    
    private int getDivisor(String fsAcquired){
        if (fsAcquired.equals("1900-01-01")){
            return 0;
        } else {
            Date ldAcquired = SQLUtil.toDate(fsAcquired.substring(0, 7) + "-01", SQLUtil.FORMAT_SHORT_DATE);
            Date ldPeriodxx = SQLUtil.toDate(String.valueOf(p_nYear) + "-" + StringHelper.prepad(String.valueOf(p_nMonth), 2, '0') + "-01", SQLUtil.FORMAT_SHORT_DATE);
            
            int lnDivisor = (int) CommonUtil.dateDiff(ldPeriodxx, ldAcquired) / 30;

            if (lnDivisor >= p_asPeriod.length)
                return p_asPeriod.length;
            else if (lnDivisor < 0)
                return 0;
            else
                return lnDivisor + 1;
        }
    }

    private boolean checkPeriod(){
        String lsSQL = "SELECT sPeriodxx, sPostedxx" +
                        " FROM " + MASTER_TABLE +
                        " WHERE sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                            " AND sInvTypCd = 'SP'" +
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
    
    private String getSQ_Sales(String fsPeriod){
        return "SELECT" +
                    "'SP Retail' xSourceNm" +
                    ", a.dTransact" +
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", 0 nDebitQty" +
                    ", b.nQuantity nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" + 
                " FROM" +
                    " SP_Sales_Master a" +
                    ", SP_Sales_Detail b" + 
                        " LEFT JOIN Inventory c" + 
                            " ON b.sStockIDx = c.sStockIDx" + 
                " WHERE a.sTransNox = b.sTransNox" + 
                    " AND a.cTranStat NOT IN ('3')" + 
                    " AND DATE_FORMAT(a.dTransact, '%Y%m') = " + SQLUtil.toSQL(fsPeriod) +
                " UNION" +
                " SELECT" +
                    "'SP Wholesale' xSourceNm" +
                    ", a.dTransact" +
                    ", c.sBarCodex" +
                    ", c.sDescript" +
                    ", 0 nDebitQty" +
                    ", b.nQuantity nCredtQty" +
                    ", a.sRemarksx" +
                    ", b.sStockIDx" +
                    ", a.sTransNox" +
                    ", a.cTranStat" + 
                " FROM" +
                    " SP_WholeSale_Master a" +
                    ", SP_WholeSale_Detail b" + 
                        " LEFT JOIN Inventory c" + 
                            " ON b.sStockIDx = c.sStockIDx" + 
                " WHERE a.sTransNox = b.sTransNox" + 
                    " AND a.cTranStat NOT IN ('3')" + 
                    " AND DATE_FORMAT(a.dTransact, '%Y%m') = " + SQLUtil.toSQL(fsPeriod) +
                " UNION" +
                " SELECT" + 
                    " 'Job Order' xSourceNm" +
                    " , a.dTransact" +
                    " , c.sBarCodex" +
                    " , c.sDescript" +
                    " , 0 nDebitQty" +
                    " , b.nQuantity nCredtQty" +
                    " , a.sJobDescr sRemarksx" +
                    " , b.sStockIDx" +
                    " , a.sTransNox" +
                    " , a.cTranStat" + 
                " FROM" +
                    " Job_Order_Master a" +
                    ", Job_Order_Parts b" + 
                        " LEFT JOIN Inventory c" + 
                            " ON b.sStockIDx = c.sStockIDx" + 
                " WHERE a.sTransNox = b.sTransNox" + 
                    " AND a.cTranStat NOT IN ('3')" + 
                    " AND DATE_FORMAT(a.dTransact, '%Y%m') = " + SQLUtil.toSQL(fsPeriod);
    }
}