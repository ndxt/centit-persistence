package com.centit.framework.jdbc.test.po;

import com.centit.framework.core.po.EntityWithDeleteTag;
import com.centit.framework.core.po.EntityWithVersionTag;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import jakarta.persistence.*;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * 员工信息表
 */
@Entity
@Table(name = "T_OFFICE_WORKER")
public class OfficeWorker implements EntityWithDeleteTag,EntityWithVersionTag,Serializable {
    /**
     * 主键，在创建时根据序列S_WORKER_ID自动生成
     */
    @Column(name = "WORKER_ID")
    @ValueGenerator(strategy= GeneratorType.UUID, value = "S_WORKER_ID")
    private String workerId;
    @Column(name = "WORKER_NAME")
    private String workerName;
    @Column(name = "WORKER_SEX")
    private String workerSex;

    @Column(name = "WORKER_AGE")
    private int workerAge;

    @Column(name = "WORKER_BIRTHDAY")
    private Date workerBirthday;

    @Basic(fetch = FetchType.LAZY)
    @Column(name = "HEAD_IMAGE")
    private byte[] headImage;

    @Column(name = "IS_DELETE")
    @ValueGenerator( strategy= GeneratorType.CONSTANT, value = "F" )
    private String isDelete;
    /**
     * 更新版本号，初始版本为 000001
     */
    @Column(name = "VERSION_NO")
    @ValueGenerator( strategy= GeneratorType.CONSTANT, value = "000001" )
    private String versionNo;
    /**
     * 创建时间，新建时自动赋值，更加函数获取当前时间
     */
    @Column(name = "CREATE_DATE")
    @ValueGenerator( strategy= GeneratorType.FUNCTION, value = "today()" )
    private Date createDate;
    /**
     * 最后更新时间，每次数据有更改是都会赋值
     */
    @Column(name = "LAST_UPDATE_DATE")
    @ValueGenerator( strategy= GeneratorType.FUNCTION, value = "today()",
            condition = GeneratorCondition.ALWAYS, occasion = GeneratorTime.NEW_UPDATE )
    private Date lastUpdateTime;

    /**
     * 这个 targetEntity 必须指定，因为java反射获取不到泛型的实体类型
     * name 为主表的字段名
     * referencedColumnName 为子表的字段名
     * 多字段引用使用 JoinColumns 注解
     */
    @OneToMany
    @Basic(fetch = FetchType.LAZY)
    @JoinColumn(name="WORKER_ID", referencedColumnName="WORKER_ID")
    private List<Career> workerCareers;
    /**
     * 判断是否为已删除
     * @return 是否为已删除
     */
    @Override
    public boolean isDeleted() {
        return "T".equals(isDelete);
    }

    /**
     * 设置删除标志
     *
     * @param isDeleted 删除标志
     */
    @Override
    public void setDeleted(boolean isDeleted) {
        isDelete = isDeleted?"T":"F";
    }

    /**
     * 计算下一个版本号
     * 版本号可以为任何类型，但是必须支持sql语句中的 =
     * @return 下一个版本号
     */
    @Override
    public Object calcNextVersion() {
        return StringBaseOpt.nextCode(versionNo);
    }

    /**
     * 返回记录版本的属性，这个属性必须和数据库表中的某个字段对应，
     * 也就是说这个属性必须有 @Column
     *
     * @return 版本字段属性名
     */
    @Override
    public String obtainVersionProperty() {
        return "versionNo";
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public String getWorkerSex() {
        return workerSex;
    }

    public void setWorkerSex(String workerSex) {
        this.workerSex = workerSex;
    }

    public Date getWorkerBirthday() {
        return workerBirthday;
    }

    public void setWorkerBirthday(Date workerBirthday) {
        this.workerBirthday = workerBirthday;
    }

    public String getIsDelete() {
        return isDelete;
    }

    public void setIsDelete(String isDelete) {
        this.isDelete = isDelete;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public List<Career> getWorkerCareers() {
        return workerCareers;
    }

    public void setWorkerCareers(List<Career> workerCareers) {
        this.workerCareers = workerCareers;
    }

    public byte[] getHeadImage() {
        return headImage;
    }

    public void setHeadImage(byte[] headImage) {
        this.headImage = headImage;
    }

    public String getVersionNo() {
        return versionNo;
    }

    public void setVersionNo(String versionNo) {
        this.versionNo = versionNo;
    }
}
