package cn.dc.webui.bean;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "ad_click_trend")
@IdClass(ClickTrendBeanId.class)
public class ClickTrendBean {
    @Id
    private String date;
    @Id
    private String hour;
    @Id
    private String minute;
    @Id
    private int adid;
    @Id
    @Column(name="clickcount")
    private int clickcount;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public int getAdid() {
        return adid;
    }

    public void setAdid(int adid) {
        this.adid = adid;
    }

    public int getClickcount() {
        return clickcount;
    }

    public void setClickcount(int clickcount) {
        this.clickcount = clickcount;
    }
}
class ClickTrendBeanId implements Serializable {

    private String date;

    private String hour;

    private String minute;

    private int adid;

    private int clickcount;
}
