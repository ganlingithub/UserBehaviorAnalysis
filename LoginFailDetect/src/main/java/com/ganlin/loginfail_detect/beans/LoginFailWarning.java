package com.ganlin.loginfail_detect.beans;

public class LoginFailWarning {
    private Long userId;
    private Long fisrstFailTime;
    private Long lastFailTime;
    private String warningMsg;

    @Override
    public String toString() {
        return "LoginFailWarning{" +
                "userId=" + userId +
                ", fisrstFailTime=" + fisrstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getFisrstFailTime() {
        return fisrstFailTime;
    }

    public void setFisrstFailTime(Long fisrstFailTime) {
        this.fisrstFailTime = fisrstFailTime;
    }

    public Long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(Long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    public LoginFailWarning(Long userId, Long fisrstFailTime, Long lastFailTime, String warningMsg) {
        this.userId = userId;
        this.fisrstFailTime = fisrstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }

    public LoginFailWarning() {
    }
}
