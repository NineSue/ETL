package core.flowdata;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class GroupByTable {
    @Getter
    private List<RowSetTable> tables = new ArrayList<>();
    @Getter
    private String[] groupByFields;
    
    public GroupByTable(String[] fields, RowSetTable table) {
        this.groupByFields = fields;
        for (Object whereValue : table.fieldValueSet(fields[0])) {
            RowSetTable tmp = table.where(row -> row.get(fields[0], table)
                                                    .equals(whereValue));
            this.tables.add(tmp);
        }
        if (fields.length == 1) {
            return;
        }
        for (int i = 1; i < fields.length; i++) {
            String field = fields[i];
            List<RowSetTable> tmp = new ArrayList<>();
            for (RowSetTable tb : this.tables) {
                for (Object whereValue : tb.fieldValueSet(fields[i])) {
                    RowSetTable whered = tb.where(row -> row.get(field, tb)
                                                            .equals(whereValue));
                    tmp.add(whered);
                }
            }
            this.tables = tmp;
        }
    }
}
