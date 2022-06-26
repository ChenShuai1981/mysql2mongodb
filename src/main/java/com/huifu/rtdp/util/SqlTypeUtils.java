package com.huifu.rtdp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author shuai
 * @date 2021-11-11 14:15
 **/
public class SqlTypeUtils {

    private static final Logger logger = LoggerFactory.getLogger(SqlTypeUtils.class);

    private static final DateTimeFormatter dateDtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter timeDtf = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter tsDtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Map<String, Object> transformToSpecificType(Map<String, Integer> sqlTypeMap, Map<String, Object> stringMap) {
        Map<String, Object> dataMap = new HashMap<>(stringMap);
        Iterator<Map.Entry<String, Object>> it = dataMap.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Object> itEntry = it.next();
            String fieldName = itEntry.getKey();
            Object fieldValue = itEntry.getValue();
            Integer sqlType = sqlTypeMap.get(fieldName);
            itEntry.setValue(castType(sqlType, fieldValue));
        }
        return dataMap;
    }

    public static Object castType(Integer sqlType, Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        String stringFieldValue = String.valueOf(fieldValue);
        Object result = null;
        switch (sqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
                result = Integer.valueOf(stringFieldValue);
                break;
            case Types.BIGINT:
                result = Long.valueOf(stringFieldValue);
                break;
            case Types.FLOAT:
                result = Float.valueOf(stringFieldValue);
                break;
            case Types.BIT:
            case Types.TINYINT:
            case Types.BOOLEAN:
                result = Boolean.valueOf(stringFieldValue);
                break;
            case Types.REAL:
            case Types.DOUBLE:
                result = Double.valueOf(stringFieldValue);
                break;
            case Types.DECIMAL:
                result = new BigDecimal(stringFieldValue);
                break;
            case Types.LONGNVARCHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
            case Types.CLOB:
            case Types.NCLOB:
                result = stringFieldValue;
                break;
            case Types.LONGVARBINARY:
            case Types.BINARY:
            case Types.BLOB:
                result = stringFieldValue.getBytes();
                break;
            case Types.DATE:
                try {
                    result = parseToDate(stringFieldValue, dateDtf);
                } catch (NumberFormatException e) {
                    logger.error("Parse date type string failed: " + stringFieldValue);
                    result = stringFieldValue;
                }
                break;
            case Types.TIME:
                try {
                    result = parseToDate(stringFieldValue, timeDtf);
                } catch (NumberFormatException e) {
                    logger.error("Parse time type string failed: " + stringFieldValue);
                    result = stringFieldValue;
                }
                break;
            case Types.TIMESTAMP:
                try {
                    stringFieldValue = stringFieldValue.substring(0, 19);
                    result = parseToDate(stringFieldValue, tsDtf);
                } catch (NumberFormatException e) {
                    logger.error("Parse timestamp type string failed: " + stringFieldValue);
                    result = stringFieldValue;
                }
                break;
            default:
                break;
        }
        return result;
    }

    public static Date parseToDate(String dtStr, DateTimeFormatter dtf) {
        LocalDateTime ldt = LocalDateTime.parse(dtStr, dtf);
        return java.util.Date.from(ldt.atZone(ZoneId.of("Asia/Shanghai")).toInstant());
    }

    public static void main(String[] args) {
        String str = "2019-08-14 13:33:12.567000";
        str = str.substring(0, 19);
        System.out.println(parseToDate(str, tsDtf));
    }

}
