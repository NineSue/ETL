package core.flowdata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Row extends ArrayList<Object> {
    public Object get(String field, RowSetTable table) {
        // 传入一个字段,返回该字段的下标
        int index = table.getFieldIndex(field);
        if (index == - 1) {
            System.err.println("字段不存在！");
            return null;
        }
        return this.get(index);
    }
    
    public void set(String field, Object newValue, RowSetTable table) {
        // 返回一个表中一个字段的值的set
        int index = table.getFieldIndex(field);
        if (index == - 1) {
            System.err.println("字段不存在！");
            return;
        }
        this.set(index, newValue);
    }
    public String toInsertSQL(String[] field, String tableName) {
        // 返回一行数据的sql形式,insert into语句
        if (field.length != this.size()) {
            System.err.println("row转insert into语句错误!原因: 字段长度与数据长度不符!");
            return null;
        }
        StringBuffer sql = new StringBuffer("insert into ");
        sql.append(tableName).append("(");
        sql.append(String.join(", ", field)).append(") values (");
        List<String> values = this.stream()
                .map(e -> "'" + e.toString() + "'")
                                   .collect(Collectors.toList());
        sql.append(String.join(", ", values)).append(");");
        return sql.toString();
    }
}
