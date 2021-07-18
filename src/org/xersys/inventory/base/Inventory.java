package org.xersys.inventory.base;

import org.xersys.inventory.search.InvSearchEngine;
import org.xersys.commander.iface.XNautilus;

public class Inventory {
    XNautilus p_oNautilus;
    
    InvSearchEngine p_oSearch;
    
    public Inventory(XNautilus foNautilus){
        p_oNautilus = foNautilus;
        
        p_oSearch = new InvSearchEngine(p_oNautilus);
    }
    
    public InvSearchEngine Search(){
        return p_oSearch;
    }
}
