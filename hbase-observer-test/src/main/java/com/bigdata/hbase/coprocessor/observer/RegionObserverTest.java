package com.imooc.bigdata.hbase.coprocessor.observer;

import java.awt.image.ImagingOpException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionObserverTest extends BaseRegionObserver {
    private byte[] columnFamily = Bytes.toBytes(s:"cf")
    private byte[] countCol = Bytes.toBytes(s:"countCol")
    private byte[] unDeleteCol = Bytes.toBytes(s:"unDeleteCol")
    private RegionCoprocessorEnvironment environment;

    //regionServer 打开region前执行
    @Override
    public void start(CoprocessorEnvironment e) {
        environment = (RegionCoprocessorEnvironment) e;
    }

    //regionserver关闭region前调用
    @Override
    public void stop(CoprocessorEnvironment e) {

    }
    /*
    1. cf:count进行累加操作,每次插入的时候都要与之前的值相加
    * */

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {
        if (put.has(columnFamily)) {
            //获得old countcol value
            Result rs = e.getEnvironment().getRegion.get(new Get(put.getRow));
            int oldNum = 0;
            for (Cell cell : rs.rawCell()) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    oldNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            //获得new countcol value
            List<Cell> cells = put.get(columnFamily, countCol);
            int newNum = 0;
            for (Cell cell : Cells) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    newNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            put.addColumn(columnFamily, countCol, Bytes.toBytes(String.valueOf((newNum + oldNum))));
        }
    }
    /*
     *  2. 不能直接删除unDeleteCol, 删除countCol的时候, 将unDeleteCol一同删除*/
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException{
        //判断是否操作cf列族
        List<Cell> cells = delete.getFamilyCellMap().get(columnFamily);
        if(cells == null || cells.size() == 0){return;}

        boolean deleteFlag = false;
        for(Cell cell : cells){
            byte[] qualifier = CellUtil.cloneQualifer(cell);

            if(Arrays.equals(qualifier, unDeleteCol)){
                throw new IOException("Cannot delete unDel Column");
            }
            if(Arrays.equals(qualifier, countCol)){
                deleteFlag = true;
            }
        }

        if(deleteFlag){
            delete.addColumn(columnFamily, unDeleteCol);
        }
    }
