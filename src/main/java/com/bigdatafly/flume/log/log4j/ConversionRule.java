package com.bigdatafly.flume.log.log4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author summer
 *
 */

public class ConversionRule {

	private boolean followedByQuotedString;  
    private int beginIndex;  
    private int length;  
    private int minWidth = -1;  
    private int maxWidth = -1;  
    private String placeholderName;  
    private String modifier;  
    private Map<String, Object> properties = new HashMap<String, Object>();  
  
    public boolean isFollowedByQuotedString() {  
        return followedByQuotedString;  
    }  
  
    public void setFollowedByQuotedString(boolean followedByQuotedString) {  
        this.followedByQuotedString = followedByQuotedString;  
    }  
  
    public int getBeginIndex() {  
        return beginIndex;  
    }  
  
    public void setBeginIndex(int beginIndex) {  
        this.beginIndex = beginIndex;  
    }  
  
    public int getLength() {  
        return length;  
    }  
  
    public void setLength(int length) {  
        this.length = length;  
    }  
  
    public int getMinWidth() {  
        return minWidth;  
    }  
  
    public void setMinWidth(int minWidth) {  
        this.minWidth = minWidth;  
    }  
  
    public int getMaxWidth() {  
        return maxWidth;  
    }  
  
    public void setMaxWidth(int maxWidth) {  
        this.maxWidth = maxWidth;  
    }  
  
    public String getPlaceholderName() {  
        return placeholderName;  
    }  
  
    public void setPlaceholderName(String placeholderName) {  
        this.placeholderName = placeholderName;  
    }  
  
    public String getModifier() {  
        return modifier;  
    }  
  
    public void setModifier(String modifier) {  
        this.modifier = modifier;  
    }  
  
    public void putProperty(String key, Object value) {  
        properties.put(key, value);  
    }  
  
    @SuppressWarnings("unchecked")  
    public <T> T getProperty(String key, Class<T> clazz) {  
        return (T) properties.get(key);  
    }  
  
    @Override  
    public String toString() {  
        return "ConversionRule [modifier=" + modifier + ", placeholderName="   + placeholderName + "]";    
    }  
}
