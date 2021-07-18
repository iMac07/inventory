package org.xersys.imbentaryo.bo;

import org.xersys.imbentaryo.search.SearchEngine;
import org.xersys.kumander.iface.XNautilus;

public class Inventory {
    XNautilus p_oNautilus;
    
    SearchEngine p_oSearch;
    
    public Inventory(XNautilus foNautilus){
        p_oNautilus = foNautilus;
        
        p_oSearch = new SearchEngine(p_oNautilus);
    }
    
    public SearchEngine Search(){
        return p_oSearch;
    }
}
