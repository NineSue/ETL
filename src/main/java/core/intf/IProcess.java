package core.intf;

import core.flowdata.RowSetTable;

public interface IProcess {
    RowSetTable deal(Object config);
}