package com.example;

public class UnderlyingComponent {
    private String underlying;
    private String share;
    private String shareType;
    private Double weight;

    public UnderlyingComponent(){

    }

    public UnderlyingComponent(String underlying, String share, String shareType, Double weight) {
        this.underlying = underlying;
        this.share = share;
        this.shareType = shareType;
        this.weight = weight;
    }

    public String getUnderlying() {
        return underlying;
    }

    public void setUnderlying(String underlying) {
        this.underlying = underlying;
    }

    public String getShare() {
        return share;
    }

    public void setShare(String share) {
        this.share = share;
    }

    public String getShareType() {
        return shareType;
    }

    public void setShareType(String shareType) {
        this.shareType = shareType;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

}