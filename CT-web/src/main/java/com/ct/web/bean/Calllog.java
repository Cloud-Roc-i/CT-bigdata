package com.ct.web.bean;

public class Calllog {
    private Integer id;
    private Integer telid;
    private Integer dateid;
    private Integer sumcall;
    private Integer sumduration;

    public Integer getDateid() {
        return dateid;
    }

    public Integer getId() {
        return id;
    }

    public Integer getSumcall() {
        return sumcall;
    }

    public Integer getSumduration() {
        return sumduration;
    }

    public Integer getTelid() {
        return telid;
    }

    public void setDateid(Integer dateid) {
        this.dateid = dateid;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setSumcall(Integer sumcall) {
        this.sumcall = sumcall;
    }

    public void setSumduration(Integer sumduration) {
        this.sumduration = sumduration;
    }

    public void setTelid(Integer telid) {
        this.telid = telid;
    }
}
