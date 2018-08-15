package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.TreeMap;

class Node {
    String node_id = "";
    String hashedId;
    Node succ;
    Node pred;
    public Node(String node_id,String hashedId){
        this.node_id = node_id;
        this.hashedId = hashedId;
    }
    public Node lookUp(String id) {

        if(pred==null && succ ==null){
            Log.v("lookUp","Pred and succ are null");
            return this;
        }
        try {
            System.out.println(pred.hashedId);
            System.out.println(id);
            System.out.println(hashedId);
            TreeMap<String,Node> tvMap = SimpleDynamoProvider.getTreeMap();
            if (id.compareTo(tvMap.get(SimpleDynamoProvider.getTreeMap().lastKey()).hashedId)>0 && id.compareTo(tvMap.get(SimpleDynamoProvider.getTreeMap().firstKey()).hashedId)>=0)
                return tvMap.get(SimpleDynamoProvider.getTreeMap().firstKey());
            else if (id.compareTo(tvMap.get(SimpleDynamoProvider.getTreeMap().firstKey()).hashedId)<=0)
                return tvMap.get(SimpleDynamoProvider.getTreeMap().firstKey());

            else if (id.compareTo(pred.hashedId)>0 && id.compareTo(hashedId)<=0)
                return this;
            else
                return succ.lookUp(id);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            Log.e("lookup error","lookup error");
        }
        return null;
    }
}