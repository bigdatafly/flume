/**
 * 
 */
package com.bigdatafly.flume.log.log4j;

import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author summer
 *
 */

public class RegexUtils {  
	  
    private static transient Logger logger = LoggerFactory.getLogger(RegexUtils.class);  
  
    /** 
     * Returns the Regex lazy suffix for the given rule. 
     * @param rule the conversion rule 
     * @return the Regex lazy suffix 
     */  
    public static String getLazySuffix(ConversionRule rule) {  
        if (rule.isFollowedByQuotedString()) {  
            return "?";  
        } else {  
            return "";  
        }  
    }  
  
    /** 
     * Returns the Regex length hint for the given rule. 
     * @param rule the conversion rule 
     * @return the Regex length hint 
     */  
    public static String getLengthHint(ConversionRule rule) {  
        if ((rule.getMaxWidth() > 0) && (rule.getMaxWidth() == rule.getMinWidth())) {  
            // Exact length specified  
            return "{" + rule.getMaxWidth() + "}";  
        } else if (rule.getMaxWidth() > 0) {  
            // Both min and max are specified  
            return "{" + Math.max(0, rule.getMinWidth()) + "," + rule.getMaxWidth() + "}"; //$NON-NLS-3$  
        } else if (rule.getMinWidth() > 0) {  
            // Only min is specified  
            return "{" + rule.getMinWidth() + ",}";  
        }  
        return "";  
    }  
  
    /** 
     * Converts a given <code>java.lang.SimpleDateFormat</code> pattern into  
     * a regular expression 
     * @param format the pattern 
     * @return the translated pattern 
     * @throws Exception if an error occurred 
     */  
    public static String getRegexForSimpleDateFormat(String format) throws Exception {  
        RegexUtils utils = new RegexUtils();  
        return utils.doGetRegexForSimpleDateFormat(format);  
    }  
  
    private String doGetRegexForSimpleDateFormat(String format) throws Exception {  
        try {  
            new SimpleDateFormat(format);  
        } catch (Exception e) {  
            // Pattern is invalid  
            throw new Exception(e);  
        }  
        // Initialize  
        ReplacementContext ctx = new ReplacementContext();  
        ctx.setBits(new BitSet(format.length()));  
        ctx.setBuffer(new StringBuffer(format));  
        // Unquote  
        unquote(ctx);  
        // G - Era designator  
        replace(ctx, "G+", "[ADBC]{2}");  
        // y - Year  
        replace(ctx, "[y]{3,}", "\\d{4}");  
        replace(ctx, "[y]{2}", "\\d{2}");  
        replace(ctx, "y", "\\d{4}");  
        // M - Month in year  
        replace(ctx, "[M]{3,}", "[a-zA-Z]*");  
        replace(ctx, "[M]{2}", "\\d{2}");  
        replace(ctx, "M", "\\d{1,2}");  
        // w - Week in year  
        replace(ctx, "w+", "\\d{1,2}");  
        // W - Week in month  
        replace(ctx, "W+", "\\d");  
        // D - Day in year  
        replace(ctx, "D+", "\\d{1,3}");  
        // d - Day in month  
        replace(ctx, "d+", "\\d{1,2}");  
        // F - Day of week in month  
        replace(ctx, "F+", "\\d");  
        // E - Day in week  
        replace(ctx, "E+", "[a-zA-Z]*");  
        // a - Am/pm marker  
        replace(ctx, "a+", "[AMPM]{2}");  
        // H - Hour in day (0-23)  
        replace(ctx, "H+", "\\d{1,2}");  
        // k - Hour in day (1-24)  
        replace(ctx, "k+", "\\d{1,2}");  
        // K - Hour in am/pm (0-11)  
        replace(ctx, "K+", "\\d{1,2}");  
        // h - Hour in am/pm (1-12)  
        replace(ctx, "h+", "\\d{1,2}");  
        // m - Minute in hour  
        replace(ctx, "m+", "\\d{1,2}");  
        // s - Second in minute  
        replace(ctx, "s+", "\\d{1,2}");  
        // S - Millisecond  
        replace(ctx, "S+", "\\d{1,3}");  
        // z - Time zone  
        replace(ctx, "z+", "[a-zA-Z-+:0-9]*");  
        // Z - Time zone  
        replace(ctx, "Z+", "[-+]\\d{4}");  
        return ctx.getBuffer().toString();  
    }  
  
    private void unquote(ReplacementContext ctx) {  
        Pattern p = Pattern.compile("'[^']+'");  
        Matcher m = p.matcher(ctx.getBuffer().toString());  
        while (m.find()) {  
            logger.trace(ctx.toString());  
            // Match is valid  
            int offset = -2;  
            // Copy all bits after the match  
            for (int i = m.end(); i < ctx.getBuffer().length(); i++) {  
                ctx.getBits().set(i + offset, ctx.getBits().get(i));  
            }  
            for (int i = m.start(); i < m.end() + offset; i++) {  
                ctx.getBits().set(i);  
            }  
            ctx.getBuffer().replace(m.start(), m.start() + 1, "");  
            ctx.getBuffer().replace(m.end() - 2, m.end() - 1, "");  
            logger.trace(ctx.toString());  
        }  
        p = Pattern.compile("''");  
        m = p.matcher(ctx.getBuffer().toString());  
        while (m.find()) {  
            logger.trace(ctx.toString());  
            // Match is valid  
            int offset = -1;  
            // Copy all bits after the match  
            for (int i = m.end(); i < ctx.getBuffer().length(); i++) {  
                ctx.getBits().set(i + offset, ctx.getBits().get(i));  
            }  
            for (int i = m.start(); i < m.end() + offset; i++) {  
                ctx.getBits().set(i);  
            }  
            ctx.getBuffer().replace(m.start(), m.start() + 1, "");  
            logger.trace(ctx.toString());  
        }  
    }  
  
    private void replace(ReplacementContext ctx, String regex, String replacement) {  
        Pattern p = Pattern.compile(regex);  
        Matcher m = p.matcher(ctx.getBuffer().toString());  
        while (m.find()) {  
            logger.trace(regex);  
            logger.trace(ctx.toString());  
            int idx = ctx.getBits().nextSetBit(m.start());  
            if ((idx == -1) || (idx > m.end() - 1)) {  
                // Match is valid  
                int len = m.end() - m.start();  
                int offset = replacement.length() - len;  
                if (offset > 0) {  
                    // Copy all bits after the match, in reverse order  
                    for (int i = ctx.getBuffer().length() - 1; i > m.end(); i--) {  
                        ctx.getBits().set(i + offset, ctx.getBits().get(i));  
                    }  
                } else if (offset < 0) {  
                    // Copy all bits after the match  
                    for (int i = m.end(); i < ctx.getBuffer().length(); i++) {  
                        ctx.getBits().set(i + offset, ctx.getBits().get(i));  
                    }  
                }  
                for (int i = m.start(); i < m.end() + offset; i++) {  
                    ctx.getBits().set(i);  
                }  
                ctx.getBuffer().replace(m.start(), m.end(), replacement);  
                logger.trace(ctx.toString());  
            }  
        }  
    }  
  
    private class ReplacementContext {  
  
        private BitSet bits;  
        private StringBuffer buffer;  
  
        /** 
         * @return the bits 
         */  
        public BitSet getBits() {  
            return bits;  
        }  
  
        /** 
         * @param bits the bits to set 
         */  
        public void setBits(BitSet bits) {  
            this.bits = bits;  
        }  
  
        /** 
         * @return the buffer 
         */  
        public StringBuffer getBuffer() {  
            return buffer;  
        }  
  
        /** 
         * @param buffer the buffer to set 
         */  
        public void setBuffer(StringBuffer buffer) {  
            this.buffer = buffer;  
        }  
  
        /* 
         * (non-Javadoc) 
         * @see java.lang.Object#toString() 
         */  
        @Override  
        public String toString() {  
            StringBuffer sb = new StringBuffer();  
            sb.append("ReplacementContext [bits=");  
            for (int i = 0; i < buffer.length(); i++) {  
                sb.append(bits.get(i) ? '1' : '0');  
            }  
            sb.append(", buffer=");  
            sb.append(buffer);  
            sb.append(']');  
            return sb.toString();  
        }  
    }  
}  