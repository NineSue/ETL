package org.gugu.etl;

import core.flowdata.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;

public class RowSetTableTest {
    @Test
    public void test_1() {
        RowSetTable table = new RowSetTable(Arrays.asList("id", "age", "sex"));
        Row row1 = new Row();
        row1.addAll(Arrays.asList("0001", "18", "man"));
        Row row2 = new Row();
        row2.addAll(Arrays.asList("0002", "20", "woman"));
        Row row3 = new Row();
        row3.addAll(Arrays.asList("0003", "30", "man"));
        Row row4 = new Row();
        row4.addAll(Arrays.asList("0004", "27", "woman"));
        Row row5 = new Row();
        row5.addAll(Arrays.asList("0005", "20", "woman"));
        table.addRow(new Row[]{row1, row2, row3, row4, row5});
        RowSetTable lambaTb = table;
        String[] infos = lambaTb.getInsertSQL("info");
        System.out.println(String.join("\n", infos));
        table = table.where(row -> row.get("id", lambaTb).equals("0001"))
                     .update(row -> row.set("age","30",lambaTb));
        System.out.println(table);
    }
    
    @Test
    public void test_2() {
        RowSetTable table = new RowSetTable(Arrays.asList("id", "age", "sex", "address"));
        Row row1 = new Row();
        row1.addAll(Arrays.asList("0001", "18", "man", "北"));
        Row row2 = new Row();
        row2.addAll(Arrays.asList("0002", "20", "woman", "南"));
        Row row3 = new Row();
        row3.addAll(Arrays.asList("0003", "30", "man", "北"));
        Row row4 = new Row();
        row4.addAll(Arrays.asList("0004", "20", "woman", "南"));
        Row row5 = new Row();
        row5.addAll(Arrays.asList("0005", "20", "woman", "北"));
        table.addRow(new Row[]{row1, row2, row3, row4, row5});
        GroupByTable groupByTable = table.groupBy(row -> new String[]{"sex", "address", "age"});
        System.out.println(groupByTable);
    }
}
