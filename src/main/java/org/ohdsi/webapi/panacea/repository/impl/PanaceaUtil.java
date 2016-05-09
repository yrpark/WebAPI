/**
 * The contents of this file are subject to the Regenstrief Public License
 * Version 1.0 (the "License"); you may not use this file except in compliance with the License.
 * Please contact Regenstrief Institute if you would like to obtain a copy of the license.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) Regenstrief Institute.  All Rights Reserved.
 */
package org.ohdsi.webapi.panacea.repository.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A util class mainly for JSON manipulation after calculation (introduced for unique pathway)
 */
public class PanaceaUtil {
    
    private static final Log log = LogFactory.getLog(PanaceaUtil.class);
    
    /**
     * Call mergeNode() for merging for unique pathway
     */
    public static JSONObject mergeFromRootNode(final String summaryJson) {
        try {
            final JSONObject rootNode = new JSONObject(summaryJson);
            
            if (rootNode.has("children")) {
                final JSONArray childJsonArray = rootNode.getJSONArray("children");
                
                final JSONArray newChildArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    //final JSONObject merged = mergeObj((JSONObject) childJsonArray.get(i));
                    //                    JSONObject merged = mergeNode((JSONObject) childJsonArray.get(i));
                    //                    merged = mergeSameUniqueDesedentNode(merged);
                    
                    //                    JSONObject merged = mergeSameDesedentNode((JSONObject) childJsonArray.get(i));
                    if (((JSONObject) childJsonArray.get(i)).has("uniqueConceptsArray")) {
                        final JSONObject merged = PanaceaUtil.mergeNode((JSONObject) childJsonArray.get(i));
                        
                        try {
                            final Map<Integer, String> oneConcept = getAddedOneConceptId(merged);
                            
                            final Map.Entry<Integer, String> entry = oneConcept.entrySet().iterator().next();
                            
                            merged.put("simpleUniqueConceptId", entry.getKey().intValue());
                            merged.put("simpleUniqueConceptName", entry.getValue());
                            merged.put("simpleUniqueConceptPercentage",  merged.get("percentage"));

                            
                        } catch (final JSONException e) {
                            // TODO Auto-generated catch block
                            log.error("Error generated", e);
                            e.printStackTrace();
                        }
                        
                        newChildArray.put(merged);
                    }
                }
                
                rootNode.remove("children");
                if (newChildArray.length() > 0) {
                    rootNode.putOpt("children", newChildArray);
                }
            }
            
            return rootNode;
        } catch (final Exception e) {
            e.printStackTrace();
        }
        
        return null;
    }
    
    public static Set<Integer> getUniqueConceptIds(final JSONObject jsonObj) {
        if (jsonObj != null) {
            final Set<Integer> conceptIds = new HashSet<Integer>();
            
            JSONArray uniqueConceptArray;
            try {
                uniqueConceptArray = jsonObj.getJSONArray("uniqueConceptsArray");
                
                if (uniqueConceptArray != null) {
                    for (int i = 0; i < uniqueConceptArray.length(); i++) {
                        if (uniqueConceptArray.get(i) != null) {
                            final Integer conceptId = new Integer(
                                    ((JSONObject) uniqueConceptArray.get(i)).getInt("innerConceptId"));
                            
                            conceptIds.add(conceptId);
                        }
                    }
                }
                
                return conceptIds;
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    public static Map<Integer, String> getUniqueConceptIdsMap(final JSONObject jsonObj) {
        if (jsonObj != null) {
            final Map<Integer, String> conceptIds = new HashMap<Integer, String>();
            
            JSONArray uniqueConceptArray;
            try {
                uniqueConceptArray = jsonObj.getJSONArray("uniqueConceptsArray");
                
                if (uniqueConceptArray != null) {
                    for (int i = 0; i < uniqueConceptArray.length(); i++) {
                        if (uniqueConceptArray.get(i) != null) {
                            final Integer conceptId = new Integer(
                                    ((JSONObject) uniqueConceptArray.get(i)).getInt("innerConceptId"));
                            final String conceptName = ((JSONObject) uniqueConceptArray.get(i))
                                    .getString("innerConceptName");
                            
                            conceptIds.put(conceptId, conceptName);
                        }
                    }
                }
                
                return conceptIds;
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    public static Map<Integer, String> getAddedOneConceptId(final JSONObject parent, final JSONObject child) {
        if ((parent != null) && (child != null)) {
            final Map<Integer, String> parentUniqueIds = getUniqueConceptIdsMap(parent);
            final Map<Integer, String> childUniqueIds = getUniqueConceptIdsMap(child);
            
            if ((parentUniqueIds != null) && (childUniqueIds != null)) {
                
                for (final Map.Entry<Integer, String> entry : parentUniqueIds.entrySet()) {
                    childUniqueIds.remove(entry.getKey());
                }
                
                final List<Integer> sortChildIdsList = new ArrayList<Integer>(childUniqueIds.keySet());
                Collections.sort(sortChildIdsList);
                
                if ((sortChildIdsList != null) && (sortChildIdsList.size() > 0)) {
                    final Map<Integer, String> returnIdMap = new HashMap<Integer, String>();
                    returnIdMap.put(sortChildIdsList.get(0), childUniqueIds.get(sortChildIdsList.get(0)));
                    
                    return returnIdMap;
                }
            }
        }
        return null;
    }
    
    public static Map<Integer, String> getAddedOneConceptId(final JSONObject parent) {
        if (parent != null) {
            final Map<Integer, String> parentUniqueIds = getUniqueConceptIdsMap(parent);
            final List<Integer> sortChildIdsList = new ArrayList<Integer>(parentUniqueIds.keySet());
            Collections.sort(sortChildIdsList);
            
            if ((sortChildIdsList != null) && (sortChildIdsList.size() > 0)) {
                final Map<Integer, String> returnIdMap = new HashMap<Integer, String>();
                returnIdMap.put(sortChildIdsList.get(0), parentUniqueIds.get(sortChildIdsList.get(0)));
                
                return returnIdMap;
            }
        }
        
        return null;
    }
    
    public static JSONObject mergeRemoveAction(final JSONObject parent, final JSONObject child) {
        if (getUniqueConceptIds(parent).equals(getUniqueConceptIds(child))) {
            return null;
        } else {
            //This is for adding Jon's one "simple unique array" - one drug only path
            final Map<Integer, String> addedOneSimpleId = getAddedOneConceptId(parent, child);
            try {
                final Map.Entry<Integer, String> entry = addedOneSimpleId.entrySet().iterator().next();
                
                child.put("simpleUniqueConceptId", entry.getKey().intValue());
                child.put("simpleUniqueConceptName", entry.getValue());
                
                double percentage = ((double)child.getInt("patientCount")) / ((double)parent.getInt("patientCount")) * ((double)100);
                
                double rounded = (double) Math.round(percentage * 100) / 100;
                
                child.put("simpleUniqueConceptPercentage",  rounded);
                
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
            
            return child;
        }
    }
    
    /**
     * For merging adjacent descendant units according to the same unique treatments.
     * 
     * @param node JSONObject
     * @return JSONObject
     */
    public static JSONObject mergeNode(final JSONObject node) {
        
        try {
            if (node.has("children") && (node.getJSONArray("children") != null)
                    && (node.getJSONArray("children").length() > 0)) {
                
                //non leaf node
                
                final JSONArray childJsonArray = node.getJSONArray("children");
                
                final JSONArray remainedChildJsonArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    //JSONObject grandChild = mergeNode(childJsonArray.getJSONObject(i));
                    mergeNode(childJsonArray.getJSONObject(i));
                    
                    final JSONObject newChild = mergeRemoveAction(node, childJsonArray.getJSONObject(i));
                    
                    if (newChild == null) {
                        //TODO -- calculate other parameters
                    } else {
                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    }
                }
                
                node.remove("children");
                if (remainedChildJsonArray.length() > 0) {
                    node.putOpt("children", remainedChildJsonArray);
                }
                
                return node;
                
            } else {
                //leaf
                return node;
            }
            
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return node;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    public static String getJSONObjStringAttr(final JSONObject jsonObj, final String attrName) {
        if (jsonObj != null) {
            try {
                return jsonObj.getString(attrName);
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    public static int getJSONObjIntegerAttr(final JSONObject jsonObj, final String attrName) {
        if (jsonObj != null) {
            try {
                return jsonObj.getInt(attrName);
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        return -1;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    //Could consider to overload mergeNode() method for different merging criteria as parameter (unique concepts/current unit's combo) 
    private JSONObject mergeSameDesedentNode(final JSONObject node) {
        
        try {
            if (node.has("children") && (node.getJSONArray("children") != null)
                    && (node.getJSONArray("children").length() > 0)) {
                
                //non leaf node
                
                final JSONArray childJsonArray = node.getJSONArray("children");
                
                final JSONArray remainedChildJsonArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    mergeSameDesedentNode(childJsonArray.getJSONObject(i));
                    //mergeSameUniqueDesedentNode(childJsonArray.getJSONObject(i));
                    
                    final JSONObject newChild = mergeRemoveAction(node, childJsonArray.getJSONObject(i));
                    if (newChild == null) {
                        //TODO -- calculate other parameters
                    } else {
                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    }
                    //                    if (getJSONObjIntegerAttr(node, "comboId") == getJSONObjIntegerAttr(childJsonArray.getJSONObject(i),
                    //                        "comboId")) {
                    //                        //TODO -- calculate other parameters
                    //                    } else {
                    //                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    //                    }
                }
                
                node.remove("children");
                if (remainedChildJsonArray.length() > 0) {
                    node.putOpt("children", remainedChildJsonArray);
                }
                
                return node;
                
            } else {
                //leaf
                return node;
            }
            
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return node;
    }
}
