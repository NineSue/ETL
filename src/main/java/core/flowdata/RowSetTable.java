package core.flowdata;

import ch.qos.logback.classic.spi.EventArgUtil;
import core.intf.RowLive;
import core.intf.RowSelect;
import core.intf.RowUpdate;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class RowSetTable {
    @Getter
    @Setter
    private String tbName;
    @Getter
    @Setter
    private List<Row> rowList = new ArrayList<>();
    private final List<String> field = new LinkedList<>();
    
    public RowSetTable(List<String> field) {
        this.field.addAll(field);
    }
    
    public RowSetTable(List<String> field, List<Row> rowList) {
        this.field.addAll(field);
        this.rowList = rowList;
    }
    
    public boolean haveField(String field) {
        return this.field.contains(field);
    }
    
    public int getFieldIndex(String field) {
        if (! haveField(field)) {
            System.err.println("字段不存在！，返回-1下标！");
            return - 1;
        }
        return this.field.indexOf(field);
    }
    
    public Set<Object> fieldValueSet(String field) {
        if (! haveField(field)) {
            System.err.println("字段不存在！，返回null！");
            return null;
        }
        HashSet<Object> set = new HashSet<>();
        int index = getFieldIndex(field);
        for (Row row : rowList) {
            set.add(row.get(index));
        }
        return set;
    }
    
    public void addRow(Row row) {
        if (row.size() == field.size()) {
            rowList.add(row);
        } else {
            System.err.println("数据行长度不对！应为" + field.size() + "个数据");
        }
    }
    
    public void addRow(List<Row> table) {
        for (Row row : table) {
            this.addRow(row);
        }
    }
    
    public void addRow(Row[] table) {
        for (Row row : table) {
            this.addRow(row);
        }
    }
    
    public RowSetTable update(RowUpdate update) {
        for (int i = 0; i < this.rowList.size(); i++) {
            Row row = this.rowList.get(i);
            update.update(row);
        }
        return new RowSetTable(field, rowList);
    }
    
    public RowSetTable where(RowLive compare) {
        RowSetTable result = new RowSetTable(field);
        for (Row row : rowList) {
            if (compare.reserve(row)) {
                result.addRow(row);
            }
        }
        return result;
    }
    
    public RowSetTable select(RowSelect selectField) {
        String[] newField = selectField.select(this.field);
        RowSetTable result = new RowSetTable(Arrays.asList(newField));
        Set<Integer> liveIndex = new HashSet<>();
        for (String field : newField) {
            if (this.field.contains(field)) {
                liveIndex.add(this.field.indexOf(field));
            }
        }
        for (Row oldRow : this.rowList) {
            Row newRow = new Row();
            for (int i = 0; i < oldRow.size(); i++) {
                if (liveIndex.contains(i)) {
                    newRow.add(oldRow.get(i));
                }
            }
            result.addRow(newRow);
        }
        return result;
    }
    
    public RowSetTable orderBy(Comparator<Row> compare) { // 传true代表升序
        RowSetTable result = new RowSetTable(this.field);
        result.addRow(this.rowList);
        result.getRowList()
              .sort(compare);
        return result;
    }
    
    public GroupByTable groupBy(RowSelect selectField) {
        String[] groupBy = selectField.select(this.field);
        for (String field : groupBy) {
            if (! this.field.contains(field)) {
                System.err.println("groupBy错误！字段不存在！");
                return null;
            }
        }
        return new GroupByTable(groupBy, this);
    }
    
    @Override
    public String toString() {
        StringBuffer builder = new StringBuffer();
        builder.append(String.join(", ", field))
               .append("\n");
        for (Row row : rowList) {
            Object[] array = row.toArray();
            for (int i = 0; i < array.length; i++) {
                builder.append(array[i]);
                if (i + 1 == array.length) {
                    builder.append("\n");
                    break;
                }
                builder.append(", ");
            }
        }
        return builder.toString();
    }
    
    public String[] getInsertSQL(String tableName) {
        List<String> sql = new ArrayList<>();
        for (Row row : this.rowList) {
            sql.add(row.toInsertSQL(this.field.toArray(new String[0]), tableName));
        }
        return sql.toArray(new String[0]);
    }
    
    public List<String> getField() {
        return field;
    }
}
