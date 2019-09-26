package cn.dc.webui.bean;


import javax.swing.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Line implements Serializable {

    private String name;
    private List<Integer> data;

    /*固定值的几个属性*/
    private String type = "line";
    private Map<String, List<Map<String, String>>> markPoint;
    private Map<String, List<Map<String, String>>> markLine;

    public Line(){
        Map<String, List<Map<String, String>>> map = new HashMap<>();
        List<Map<String,String>> list1 = new ArrayList();
        Map<String, String> map1 = new HashMap<>();
        map1.put("type", "average");
        map1.put("name", "平均值");
        list1.add(map1);
        map.put("data", list1);

        this.markLine = map;
        Map<String, List<Map<String, String>>> map0 = new HashMap<>();
        List<Map<String,String>> list = new ArrayList();
        Map<String, String> map11 = new HashMap<>();
        map11.put("type", "max");
        map11.put("name", "最大值");
        Map<String, String> map22 = new HashMap<>();
        map22.put("type", "min");
        map22.put("name", "最小值");
        list.add(map11);
        list.add(map22);

        map0.put("data", list);
        this.markPoint = map0;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getData() {
        return data;
    }

    public void setData(List<Integer> data) {
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, List<Map<String, String>>> getMarkPoint() {
        return markPoint;
    }

    public void setMarkPoint(Map<String, List<Map<String, String>>> markPoint) {
        this.markPoint = markPoint;
    }

    public Map<String, List<Map<String, String>>> getMarkLine() {
        return markLine;
    }

    public void setMarkLine(Map<String, List<Map<String, String>>> markLine) {
        this.markLine = markLine;
    }
}
