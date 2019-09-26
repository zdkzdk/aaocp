package cn.dc.webui.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ClickTrendOption implements Serializable {
    //line的name属性的集合,用在顶部的快速显隐列表
    private List<String> legend_data = new ArrayList<>();
    //xAxis的data属性，跟line对象的data属性的值一一对应
    private List<Integer> data = new ArrayList<>();
    //Line对象，封装了一条线的数据，orm  ad_click_trend表的一行记录，但line对象中
    private List<Line> lineList = new ArrayList<>();

    public List<String> getLegend_data() {
        return legend_data;
    }

    public void setLegend_data(List<String> legend_data) {
        this.legend_data = legend_data;
    }

    public List<Integer> getData() {
        return data;
    }

    public void setData(List<Integer> data) {
        this.data = data;
    }

    public List<Line> getLineList() {
        return lineList;
    }

    public void setLineList(List<Line> lineList) {
        this.lineList = lineList;
    }
}

