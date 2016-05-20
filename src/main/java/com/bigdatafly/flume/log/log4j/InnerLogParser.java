/**
 * 
 */
package com.bigdatafly.flume.log.log4j;

import java.text.DateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;







/**
 * @author summer
 *
 */
public class InnerLogParser {

    private static Logger logger = Logger.getLogger(InnerLogParser.class);  
    
	public static final String FIELD_LOGGER = "logger";  
    public static final String FIELD_TIMESTAMP = "timestamp";  
    public static final String FIELD_LEVEL = "level";  
    public static final String FIELD_THREAD = "thread";  
    public static final String FIELD_MESSAGE = "message";  
    public static final String FIELD_CURRENT_LINE = "currLine";  
    public static final String FIELD_IS_HIT = "isHit";  
    public static final String FIELD_NDC = "ndc";  
    public static final String FIELD_THROWABLE = "throwable";  
    public static final String FIELD_LOC_FILENAME = "locFilename";  
    public static final String FIELD_LOC_CLASS = "locClass";  
    public static final String FIELD_LOC_METHOD = "locMethod";  
    public static final String FIELD_LOC_LINE = "locLine";  
    

    private ConversionRuleParser conversionRuleParser;  
    private String conversionPattern;
    List<ConversionRule> extractRules;
    
    public InnerLogParser(String conversionPattern) {  
    	
    	this.conversionPattern = conversionPattern;
        this.conversionRuleParser = new ConversionRuleParser();
        
        try {
			this.extractRules = conversionRuleParser.extractRules(conversionPattern);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }  
  
  
    public Map<String,Object> fetchARecord(StringBuffer iter) throws Exception {  
    	
    	Map<String,Object> currentEntry = null;  
        
        String line = iter.toString();  
        
        if (StringUtils.isBlank(line)) {  
            return null;  
        }  
        Matcher m = conversionRuleParser.getInternalPattern(conversionPattern).matcher(line);
        if (m.find()) {
        	currentEntry = new HashMap<String,Object>();  
        	
             for (int i = 0; i < m.groupCount(); i++) {  
                        try {  
                            this.extractField(currentEntry, extractRules.get(i), m.group(i + 1));  
                            
                            System.out.println(m.group(i + 1));
                        } catch (Exception e) {  
                            // Mark for interruption   
                            logger.warn(e);  
                        }  
              }  
                    
             return currentEntry;  
         } 
         return null;

    }  
  
    private void extractField(Map<String,Object> entry, ConversionRule rule, String val) throws Exception {  
        if (rule.getPlaceholderName().equals("d")) {  
            DateFormat df = rule.getProperty(ConversionRuleParser.PROP_DATEFORMAT, DateFormat.class);  
            entry.put(FIELD_TIMESTAMP, df.parse(val.trim()));  
        } else if (rule.getPlaceholderName().equals("p")) {  
            Level lvl = Level.toLevel(val.trim());  
            entry.put(FIELD_LEVEL, lvl);  
        } else if (rule.getPlaceholderName().equals("c")) {  
            entry.put(FIELD_LOGGER, val.trim());  
        } else if (rule.getPlaceholderName().equals("t")) {  
            entry.put(FIELD_THREAD, val.trim());  
        } else if (rule.getPlaceholderName().equals("m")) {  
            entry.put(FIELD_MESSAGE, val.trim());  
        } else if (rule.getPlaceholderName().equals("F")) {  
            entry.put(FIELD_LOC_FILENAME, val.trim());  
        } else if (rule.getPlaceholderName().equals("C")) {  
            entry.put(FIELD_LOC_CLASS, val.trim());  
        } else if (rule.getPlaceholderName().equals("M")) {  
            entry.put(FIELD_LOC_METHOD, val.trim());  
        } else if (rule.getPlaceholderName().equals("L")) {  
            entry.put(FIELD_LOC_LINE, val.trim());  
        } else if (rule.getPlaceholderName().equals("x")) {  
            entry.put(FIELD_NDC, val.trim());  
        } else {  
            throw new Exception("异常消息暂未设置");  
        }  
    }
}
