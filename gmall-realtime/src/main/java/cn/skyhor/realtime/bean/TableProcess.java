package cn.skyhor.realtime.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * (TableProcess)实体类
 *
 * @author wbw
 * @since 2023-02-23 16:46:52
 */
public class TableProcess implements Serializable {
    private static final long serialVersionUID = 334148383619826631L;
    /**
     * 来源表
     */
    private String sourceTable;
    /**
     * 操作类型  insert,update,delete
     */
    private String operateType;
    /**
     * 输出类型  hbase kafka
     */
    private String sinkType;
    /**
     * 输出表 (主题 )
     */
    private String sinkTable;
    /**
     * 输出字段
     */
    private String sinkColumns;
    /**
     * 主键字段
     */
    private String sinkPk;
    /**
     * 建表扩展
     */
    private String sinkExtend;


    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public void setSinkPk(String sinkPk) {
        this.sinkPk = sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }

}

