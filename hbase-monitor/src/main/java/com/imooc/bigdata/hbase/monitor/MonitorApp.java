package com.imooc.bigdata.hbase.monitor;

import com.imooc.bigdata.hbase.monitor.entity.HbaseSummary;

import java.io.IOException;

public class MonitorApp {
    public static void main(String[] args) throws IOException {
        StatefulHttpClient client = new StatefulHttpClient(null);
        HadoopUtil.getHdfsSummary(client).printInfo();
        HBaseUtil.getHbaseSummary(client).printInfo();
    }
}