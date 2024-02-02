package com.centit.framework.jdbc.test.po;

import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Date;

/**
 * 职业生涯表
 */
@Entity
@Table(name = "T_CAREER")
public class Career implements Serializable {
    /**
     * 主键，在创建时生成随机UUID
     */
    @Column(name = "CAREER_ID")
    @ValueGenerator( strategy= GeneratorType.UUID)
    @Id
    String careerId;
    /**
     * 外建
     */
    @Column(name = "WORKER_ID")
    private String workerId;

    @Column(name = "CORPORATE_NAME")
    private String corporateName;
    @Column(name = "BEGIN_DATE")
    private Date beginDate;
    @Column(name = "END_DATE")
    private Date endDate;

    public String getCareerId() {
        return careerId;
    }

    public void setCareerId(String careerId) {
        this.careerId = careerId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getCorporateName() {
        return corporateName;
    }

    public void setCorporateName(String corporateName) {
        this.corporateName = corporateName;
    }

    public Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
}
