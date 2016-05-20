package com.bigdatafly.flume.log.log4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author summer
 *
 */

public class ConversionRuleParser {

	private static final Pattern EXTRACTION_PATTERN = Pattern.compile("%(-?(\\d+))?(\\.(\\d+))?([a-zA-Z])(\\{([^\\}]+)\\})?");   
    public static final String PROP_DATEFORMAT = "dateFormat";  
  
    protected Pattern getInternalPattern(String externalPattern) throws Exception {  
        List<ConversionRule> rules = extractRules(externalPattern);  
        return Pattern.compile(toRegexPattern(prepare(externalPattern), rules));  
    }  
      
    protected List<ConversionRule> extractRules(String externalPattern) throws Exception {  
        externalPattern = prepare(externalPattern);  
        Matcher m = EXTRACTION_PATTERN.matcher(externalPattern);  
        List<ConversionRule> ret = new ArrayList<ConversionRule>();  
        while (m.find()) {  
            String minWidthModifier = m.group(2);  
            String maxWidthModifier = m.group(4);  
            String conversionName = m.group(5);  
            String conversionModifier = m.group(7);  
            int minWidth = -1;  
            if ((minWidthModifier != null) && (minWidthModifier.length() > 0)) {  
                minWidth = Integer.parseInt(minWidthModifier);  
            }  
            int maxWidth = -1;  
            if ((maxWidthModifier != null) && (maxWidthModifier.length() > 0)) {  
                maxWidth = Integer.parseInt(maxWidthModifier);  
            }  
            ConversionRule rule = new ConversionRule();  
            rule.setBeginIndex(m.start());  
            rule.setLength(m.end() - m.start());  
            rule.setMaxWidth(maxWidth);  
            rule.setMinWidth(minWidth);  
            rule.setPlaceholderName(conversionName);  
            rule.setModifier(conversionModifier);  
            rewrite(rule);  
            ret.add(rule);  
        }  
        return ret;  
    }  
  
    public String prepare(String externalPattern) throws Exception {  
        if (!externalPattern.endsWith("%n")) {   
            return externalPattern;  
        }  
        // Pattern without %n  
        externalPattern = externalPattern.substring(0, externalPattern.length() - 2);  
        if (externalPattern.contains("%n")) {   
            throw new Exception("ConversionPattern不合法!");  
        }  
        return externalPattern;  
    }  
      
    private void rewrite(ConversionRule rule) throws Exception {  
        if (rule.getPlaceholderName().equals("d")) {  
            applyDefaults(rule);  
            if (rule.getModifier().equals("ABSOLUTE")) {  
                rule.setModifier("HH:mm:ss,SSS");  
            } else if (rule.getModifier().equals("DATE")) {  
                rule.setModifier("dd MMM yyyy HH:mm:ss,SSS");  
            } else if (rule.getModifier().equals("ISO8601")) {  
                rule.setModifier("yyyy-MM-dd HH:mm:ss,SSS");  
            }  
            try {  
                // Cache date format  
                rule.putProperty(PROP_DATEFORMAT, new SimpleDateFormat(rule.getModifier()));  
            } catch (IllegalArgumentException e) {  
                throw new Exception(e);  
            }  
        }  
    }  
  
    private void applyDefaults(ConversionRule rule) throws Exception {  
        if (rule.getModifier() == null) {  
            // ISO8601 is the default  
            rule.setModifier("ISO8601");  
        }  
    }  
  
    private String getRegexPatternForRule(ConversionRule rule) throws Exception {  
        if (rule.getPlaceholderName().equals("d")) {  
            // Pattern is dynamic  
            return "(" + RegexUtils.getRegexForSimpleDateFormat(rule.getModifier()) + ")";  
        } else if (rule.getPlaceholderName().equals("p")) {  
            String lnHint = RegexUtils.getLengthHint(rule);  
            if (lnHint.length() > 0) {  
                return "([ A-Z]" + lnHint + ")";  
            }  
            // Default: Length is limited by the levels available  
            return "([A-Z]{4,5})";  
        } else if (rule.getPlaceholderName().equals("c")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("t")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("m")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("F")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("C")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("M")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("L")) {  
            return "([0-9]*" + RegexUtils.getLengthHint(rule) + ")";  
        } else if (rule.getPlaceholderName().equals("x")) {  
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";  
        }    
        throw new Exception("无法找到对应的表达式描述!");  
    }  
  
    protected String toRegexPattern(String externalPattern, List<ConversionRule> rules) throws Exception {  
        // Determine whether rules are followed by quoted string, allowing use of special Regex lazy modifiers  
        int idx = 0;  
        ConversionRule prevRule = null;  
        for (ConversionRule rule : rules) {  
            if ((rule.getBeginIndex() > idx) && (prevRule != null)) {  
                // Previous rule is followed by a quoted string, allowing special regex flags  
                prevRule.setFollowedByQuotedString(true);  
            }  
            idx = rule.getBeginIndex();  
            idx += rule.getLength();  
            prevRule = rule;  
        }  
        if ((externalPattern.length() > idx) && (prevRule != null)) {  
            // Previous rule is followed by a quoted string, allowing special regex flags  
            prevRule.setFollowedByQuotedString(true);  
        }  
        // Build the internal Regex pattern  
        StringBuilder sb = new StringBuilder();  
        idx = 0;  
        for (ConversionRule rule : rules) {  
            if (rule.getBeginIndex() > idx) {  
                // Escape chars with special meaning  
                sb.append(Pattern.quote(externalPattern.substring(idx, rule.getBeginIndex())));  
            }  
            idx = rule.getBeginIndex();  
            String regex = this.getRegexPatternForRule(rule);  
            sb.append(regex);  
            idx += rule.getLength();  
        }  
        if (externalPattern.length() > idx) {  
            // Append suffix  
            sb.append(Pattern.quote(externalPattern.substring(idx)));  
        }  
        return sb.toString();  
    } 
}
