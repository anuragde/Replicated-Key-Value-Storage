package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Application;
import android.content.Context;
import android.util.Log;

import java.io.File;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.prefs.AbstractPreferences;


public class SimpleDynamoHelper {
    static void printNodeList(ArrayList<Node> tA) {

        for (Node n : tA) {

            System.out.println(n.node_id +" "+ n.pred.node_id  +" "+n.succ.node_id);
        }
        try{
            System.out.println("-----------------------------------");
            System.out.println(SimpleDynamoProvider.getMyNode().node_id+SimpleDynamoProvider.getMyNode().succ.node_id+SimpleDynamoProvider.getMyNode().succ.succ.node_id);
            System.out.println("-----------------------------------");
        }
        catch (Exception ec){

        }


        return;
    }
    static void addNewNodeTreeMap(String newNodeID, String newNodeHashedId) {
        Node myNode = SimpleDynamoProvider.getMyNode();
        Node newNode1 = new Node(newNodeID, newNodeHashedId);
        TreeMap<String,Node> tvMap = SimpleDynamoProvider.getTreeMap();
        tvMap.put(newNodeHashedId, newNode1);
        SimpleDynamoProvider.setTreeMap(tvMap);
        String[] mapKeys = new String[tvMap.size()];
        int pos = 0;
        int keyPos = 0;
        for(String key : tvMap.keySet()){
            mapKeys[pos]=key;

            if(key.equals(newNodeHashedId)){
                keyPos = pos;
            }
            pos++;
        }
        if(myNode.node_id.equals(newNodeID)){
            if(keyPos == 0){
                myNode.pred = tvMap.get(tvMap.lastKey());
                newNode1.pred = tvMap.get(tvMap.lastKey());
                newNode1.pred.succ =newNode1;
                tvMap.put(tvMap.lastKey(),newNode1.pred);
            }
            else{
                myNode.pred = tvMap.get(mapKeys[keyPos-1]);
                newNode1.pred = tvMap.get(mapKeys[keyPos-1]);
                newNode1.pred.succ =newNode1;
                tvMap.put(mapKeys[keyPos-1],newNode1.pred);
            }
            if(keyPos == tvMap.size()-1){
                myNode.succ = tvMap.get(mapKeys[0]);
                newNode1.succ = tvMap.get(mapKeys[0]);
                newNode1.succ.pred =newNode1;
                tvMap.put(mapKeys[0],newNode1.succ);
            }
            else{
                myNode.succ = tvMap.get(mapKeys[keyPos+1]);
                newNode1.succ = tvMap.get(mapKeys[keyPos+1]);
                newNode1.succ.pred =newNode1;
                tvMap.put(mapKeys[keyPos+1],newNode1.succ);
            }


        }
        else{
            if(keyPos == 0){
                newNode1.pred = tvMap.get(tvMap.lastKey());
                newNode1.pred.succ =newNode1;
                if(newNode1.pred.node_id.equals(myNode.node_id)){
                    myNode.succ =newNode1;
                }
                tvMap.put(tvMap.lastKey(),newNode1.pred);

            }

            else{
                newNode1.pred = tvMap.get(mapKeys[keyPos-1]);
                newNode1.pred.succ =newNode1;
                if(newNode1.pred.node_id.equals(myNode.node_id)){
                    myNode.succ =newNode1;
                }
                tvMap.put(mapKeys[keyPos-1],newNode1.pred);

            }

            if(keyPos == tvMap.size()-1){
                newNode1.succ = tvMap.get(mapKeys[0]);
                newNode1.succ.pred =newNode1;
                if(newNode1.succ.node_id.equals(myNode.node_id)){
                    myNode.pred =newNode1;
                }
                tvMap.put(mapKeys[0],newNode1.succ);

            }

            else{
                newNode1.succ = tvMap.get(mapKeys[keyPos+1]);
                newNode1.succ.pred =newNode1;
                if(newNode1.succ.node_id.equals(myNode.node_id)){
                    myNode.pred =newNode1;
                }
                tvMap.put(mapKeys[keyPos+1],newNode1.succ);

            }

        }
        SimpleDynamoProvider.setMyNode(myNode);
        ArrayList<Node> temp = SimpleDynamoProvider.getNodeList();
        temp.add(newNode1);
        SimpleDynamoProvider.setNodeList(temp);
        tvMap.put(newNodeHashedId, newNode1);
        SimpleDynamoProvider.setTreeMap(tvMap);
        return;
    }

    static boolean getConnected(String id){
        switch(Integer.parseInt(id)) {
            case 11108:
                return SimpleDynamoProvider.connected[0] == true;
            case 11112:
                return SimpleDynamoProvider.connected[1] == true;
            case 11116:
                return SimpleDynamoProvider.connected[2] == true;
            case 11120:
                return SimpleDynamoProvider.connected[3] == true;
            case 11124:
                return SimpleDynamoProvider.connected[4] == true;
        }
        return false;
    }
    static void printConnected(){
        Log.v("ServerTask",Boolean.toString(SimpleDynamoProvider.connected[0]));
        Log.v("ServerTask",Boolean.toString(SimpleDynamoProvider.connected[1]));
        Log.v("ServerTask",Boolean.toString(SimpleDynamoProvider.connected[2]));
        Log.v("ServerTask",Boolean.toString(SimpleDynamoProvider.connected[3]));
        Log.v("ServerTask",Boolean.toString(SimpleDynamoProvider.connected[4]));

    }
    static void updateConnected(String id){
        int val = 0;
        if(id.contains("11108"))
            val = 11108;
        else if(id.contains("11112"))
            val = 11112;
        else if(id.contains("11116"))
            val = 11116;
        else if(id.contains("11120"))
            val = 11120;
        else if(id.contains("11124"))
            val = 11124;
        switch(val) {
            case 11108:
                SimpleDynamoProvider.connected[0] = true;
                break;
            case 11112:
                SimpleDynamoProvider.connected[1] = true;
                break;
            case 11116:
                SimpleDynamoProvider.connected[2] = true;
                break;
            case 11120:
                SimpleDynamoProvider.connected[3] = true;
                break;
            case 11124:
                SimpleDynamoProvider.connected[4] = true;
                break;
        }
        return;
    }

    static void updateConnectedFalse(String id){
        int val = 0;
        if(id.contains("11108"))
            val = 11108;
        else if(id.contains("11112"))
            val = 11112;
        else if(id.contains("11116"))
            val = 11116;
        else if(id.contains("11120"))
            val = 11120;
        else if(id.contains("11124"))
            val = 11124;
        switch(val) {
            case 11108:
                SimpleDynamoProvider.connected[0] = false;
                break;
            case 11112:
                SimpleDynamoProvider.connected[1] = false;
                break;
            case 11116:
                SimpleDynamoProvider.connected[2] = false;
                break;
            case 11120:
                SimpleDynamoProvider.connected[3] = false;
                break;
            case 11124:
                SimpleDynamoProvider.connected[4] = false;
                break;
        }
        return;
    }
}
